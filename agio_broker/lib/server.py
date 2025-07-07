import asyncio
import logging
import urllib.parse
import json
import re
import threading
import time
from queue import Queue

logger = logging.getLogger(__name__)


class BrokerServer:
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
        self.server = await asyncio.start_server(self.handle_client, self.host, self.port)
        logger.info(f"Server running at http://{self.host}:{self.port}")
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

    async def handle_client(self, reader, writer):
        data = await reader.read(65536)
        request = data.decode('utf-8', errors='ignore')

        try:
            header_section, body = request.split('\r\n\r\n', 1)
        except ValueError:
            writer.close()
            return

        request_lines = header_section.split('\r\n')
        request_line = request_lines[0]
        method, path, _ = request_line.split(' ', 2)
        headers = self._parse_headers(request_lines[1:])
        query = self._parse_query_string(path)
        parsed_body, files = await self._decode_body(headers, body)

        request_id = str(time.time())
        payload = {
            "id": request_id,
            "method": method,
            "path": path,
            "query": query,
            "body": parsed_body,
            "files": files,
        }
        print(payload)
        # TODO
        # future = self.loop.create_future()
        # self.response_map[request_id] = future
        # self.queue.put(payload)
        #
        # try:
        #     result = await asyncio.wait_for(future, timeout=5)
        # except asyncio.TimeoutError:
        #     result = {"error": "Request timed out"}
        # except Exception as e:
        #     result = {"error": str(e)}
        # finally:
        #     self.response_map.pop(request_id, None)
        result = {'status': 'ok'}

        response_body = json.dumps(result, ensure_ascii=False, indent=2)
        response = (
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: application/json; charset=utf-8\r\n"
            f"Content-Length: {len(response_body.encode())}\r\n"
            "Connection: close\r\n"
            "\r\n"
            f"{response_body}"
        )

        writer.write(response.encode())
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    # async def _start_server(self):
    #     self.loop = asyncio.get_running_loop()
    #     self.server = await asyncio.start_server(self.handle_client, self.host, self.port)
    #     print(f"Server running at http://{self.host}:{self.port}")
    #     async with self.server:
    #         await self.server.serve_forever()
    #
    # def start(self):
    #     asyncio.run(self._start_server())
    #     # self.task = asyncio.create_task(self._start_server())
    #     # return self.task
    #
    # async def stop(self):
    #     if self.task:
    #         self.task.cancel()
    #         try:
    #             await self.task
    #         except asyncio.CancelledError:
    #             print("Server task cancelled.")
    #     if self.server:
    #         self.server.close()
    #         await self.server.wait_closed()
    #     self.running = False
    #     print("Server stopped.")
