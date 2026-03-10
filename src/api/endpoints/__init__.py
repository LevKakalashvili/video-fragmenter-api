import importlib
from pathlib import Path

from fastapi import APIRouter

api_router = APIRouter()

_current_dir = Path(__file__).resolve().parent
_module_names = sorted(
    module_path.stem for module_path in _current_dir.glob("*_router.py") if module_path.name != "__init__.py"
)

for module_name in _module_names:
    module = importlib.import_module(f".{module_name}", package=__name__)
    router = getattr(module, "router", None)
    if router is not None:
        api_router.include_router(router)
