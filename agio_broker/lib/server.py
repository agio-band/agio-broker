import asyncio
import json
import logging
import re
import threading
import urllib.parse
import uuid
from http import HTTPStatus
from queue import Queue

from agio_broker.lib import models
from agio_broker.lib.exceptions import ActionInProgressError

logger = logging.getLogger(__name__)


class BrokerServer:
    REQUEST_TIMEOUT = 30
    def __init__(self, queue: Queue, response_map: dict, host='127.0.0.1', port=8080):
        self.host = host
        self.port = port
        self.queue = queue
        self.response_map = response_map
        self.loop = None
        self.server = None
        self.thread = None
        self._should_stop = threading.Event()

    async def _start_server(self):
        self.server = await asyncio.start_server(self.handle_request, self.host, self.port)
        logger.info(f"Broker server running at http://{self.host}:{self.port}")
        await self.server.serve_forever()

    def _run_loop(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_until_complete(self._start_server())
        except asyncio.CancelledError:
            pass
        finally:
            self.loop.run_until_complete(self._cleanup())
            self.loop.stop()
            self.loop.close()

    async def _cleanup(self):
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()

    def start(self):
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()

    def stop(self):
        """Synchronous stop: closes the server and waits for thread to finish."""
        if self.loop is not None and self.server is not None:
            async def shutdown():
                self.server.close()
                await self.server.wait_closed()
                # self.loop.stop()
            # Schedule the shutdown coroutine in the loop
            asyncio.run_coroutine_threadsafe(shutdown(), self.loop)
            self.thread.join()
            logger.debug("Broker server stopped.")

    async def handle_request(self, reader, writer):
        try:
            await self.handle_client(reader, writer)
        except ActionInProgressError:
            warning_response = models.ActionResponseModel(
                status=models.Status.PROCESSING,
                message='Action was in progress.',
            )
            writer.write(
                self._with_head(
                    data=warning_response.model_dump(mode='json'),
                    code=500
                ).encode('utf-8', errors='ignore')
            )
        except Exception as e:
            logger.exception('Request failed')
            error_response = models.ActionResponseModel(
                status=models.Status.ERROR,
                message=str(e),
            )
            writer.write(
                self._with_head(
                    data=error_response.model_dump(mode='json'),
                    code=500
                ).encode('utf-8', errors='ignore')
            )
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    async def handle_client(self, reader, writer):
        data = await reader.read(65536)
        request = data.decode('utf-8', errors='ignore')
        try:
            header_section, body = request.split('\r\n\r\n', 1)
        except ValueError:
            logger.warning(f"Bad request: {request}")
            return

        request_lines = header_section.split('\r\n')
        request_line = request_lines[0]
        method, path, _ = request_line.split(' ', 2)
        headers = self._parse_headers(request_lines[1:])

        if method == "OPTIONS":
            response = (
                "HTTP/1.1 204 No Content\r\n"
                "Access-Control-Allow-Origin: *\r\n"
                "Access-Control-Allow-Methods: GET, POST, OPTIONS\r\n"
                "Access-Control-Allow-Headers: Content-Type\r\n"
                "Content-Length: 0\r\n"
                "Connection: close\r\n"
                "\r\n"
            )
            writer.write(response.encode("utf-8"))
            return

        parsed_body, files = await self._decode_body(headers, body)
        request = models.ActionRequestModel(**parsed_body)
        query = self._parse_query_string(path)
        request_id = uuid.uuid4().hex
        payload = {
            "id": request_id,
            "method": method,
            "path": path,
            "query": query,
            "data": request.model_dump(mode="json"),
            "files": files,
        }
        result = await self.process_request(payload)
        if result is not None:
            result = models.ActionResponseModel(
                status=models.Status.OK,
                data=result
            ).model_dump(mode="json")
        response = self._with_head(result)
        writer.write(response.encode('utf-8', errors='ignore'))

    async def process_request(self, payload: dict) -> dict|None:
        future = self.loop.create_future()
        self.response_map[payload['id']] = future
        self.queue.put(payload)
        timeout_sec = payload.get('action_task_timeout', self.REQUEST_TIMEOUT)
        try:
            result = await asyncio.wait_for(future, timeout=timeout_sec)
        except asyncio.TimeoutError as e:
            raise Exception("Action Task timed out") from e
        finally:
            self.response_map.pop(payload['id'], None)
        return result

    def _parse_query_string(self, path):
        parsed = urllib.parse.urlparse(path)
        return dict(urllib.parse.parse_qsl(parsed.query))

    def _parse_headers(self, header_lines):
        headers = {}
        for line in header_lines:
            if ': ' in line:
                key, value = line.split(': ', 1)
                headers[key.lower()] = value
        return headers

    def _parse_multipart(self, body, boundary):
        files = {}
        parts = body.split(f'--{boundary}')
        for part in parts:
            if not part or part == "--\r\n":
                continue
            headers_body = part.lstrip().split('\r\n\r\n', 1)
            if len(headers_body) != 2:
                continue
            headers_raw, content = headers_body
            content = content.rstrip('\r\n')
            headers = self._parse_headers(headers_raw.split('\r\n'))
            disposition = headers.get('content-disposition', '')
            name_match = re.search(r'name="(.+?)"', disposition)
            filename_match = re.search(r'filename="(.+?)"', disposition)
            if name_match:
                name = name_match.group(1)
                if filename_match:
                    files[name] = {
                        'filename': filename_match.group(1),
                        'content': content
                    }
                else:
                    files[name] = content
        return files

    async def _decode_body(self, headers, body):
        content_type = headers.get('content-type', '')
        if 'application/json' in content_type:
            try:
                return json.loads(body), {}
            except json.JSONDecodeError:
                return {}, {}
        elif 'application/x-www-form-urlencoded' in content_type:
            return dict(urllib.parse.parse_qsl(body)), {}
        elif 'multipart/form-data' in content_type:
            boundary_match = re.search(r'boundary=(.+)', content_type)
            if boundary_match:
                boundary = boundary_match.group(1)
                return {}, self._parse_multipart(body, boundary)
        return {}, {}

    def _with_head(self, data: dict, code: int = 200, extra_headers: dict = None) -> str:
        if isinstance(data, dict):
            data_bytes = json.dumps(data, ensure_ascii=False)
        else:
            if not data:
                data_bytes = "{}"
            else:
                data_bytes = data
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Content-Length": str(len(data_bytes)),
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type",
            "Connection": "close",
        }
        if extra_headers:
            headers.update(extra_headers)
        header_str = "\r\n".join(f"{k}: {v}" for k, v in headers.items())

        return f"HTTP/1.1 {code} {HTTPStatus(code).phrase}\r\n{header_str}\r\n\r\n{data_bytes}"
