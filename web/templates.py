"""Shared packaged Web templates; no router or runtime-store dependency."""
import json
from importlib.resources import files

from fastapi.templating import Jinja2Templates


templates = Jinja2Templates(directory=str(files("web").joinpath("templates")))


def _tojson_cn(value):
    return json.dumps(value, ensure_ascii=False)


templates.env.filters["tojson_cn"] = _tojson_cn
