"""Shared immutable constants and in-process state for the works feature."""

from collections import OrderedDict
from threading import RLock


_OPENALEX_GLOBAL_ANALYTICS_CACHE = OrderedDict()
_OPENALEX_GLOBAL_ANALYTICS_CACHE_LOCK = RLock()
_OPENALEX_INSTITUTION_ANALYTICS_CACHE = OrderedDict()
_OPENALEX_INSTITUTION_ANALYTICS_CACHE_LOCK = RLock()

_OPENALEX_ANALYTICS_CACHE_MAX_SIZE = 40
_OPENALEX_ANALYTICS_CACHE_TTL = 86400
_OPENALEX_PERSISTENT_CACHE_VERSION = 8
_OPENALEX_PERSISTENT_CACHE_MAX_FILES = 120

# Source: https://www.igi-global.com/newsroom/archive/guide-understanding-colors-open-access/4925/
_OPENALEX_OA_COLORS = {
    # IGI Global names Platinum OA as sponsored or Diamond OA.
    "diamond": "#595959",
    "green": "#00b050",
    "blue": "#2f5496",
    "yellow": "#eab200",
    "hybrid": "#ed7d31",
    "gold": "#bf8f00",
    "bronze": "#806000",
    "white": "#a0a0a0",
    "black": "#000000",
    # OpenAlex closed is not an IGI color category; use IGI's neutral gray.
    "closed": "#a0a0a0",
}
_OPENALEX_PRIORITY_OA_COLORS = {
    status: _OPENALEX_OA_COLORS[status]
    for status in ("diamond", "green")
}
