from pathlib import Path

from agio.tools.modules import import_modules_from_dir

routes_dir = Path(__file__).parent / 'routes'
import_modules_from_dir(routes_dir, f'{__name__}.routes')
