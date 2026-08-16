"""Compatibility facade for the modular works, cache, and OpenAlex feature.

The public Flask blueprint and historical helper imports remain available from
this module, while implementation is grouped into focused modules.
"""

from .works_blueprint import bp_works
from .works_state import (
    _OPENALEX_ANALYTICS_CACHE_MAX_SIZE,
    _OPENALEX_ANALYTICS_CACHE_TTL,
    _OPENALEX_GLOBAL_ANALYTICS_CACHE,
    _OPENALEX_GLOBAL_ANALYTICS_CACHE_LOCK,
    _OPENALEX_INSTITUTION_ANALYTICS_CACHE,
    _OPENALEX_INSTITUTION_ANALYTICS_CACHE_LOCK,
    _OPENALEX_OA_COLORS,
    _OPENALEX_PERSISTENT_CACHE_MAX_FILES,
    _OPENALEX_PERSISTENT_CACHE_VERSION,
    _OPENALEX_PRIORITY_OA_COLORS,
)
from . import works_shared as _shared
from . import works_openalex_data as _openalex_data
from . import works_institution_analytics as _institution_analytics
from . import works_global_analytics as _global_analytics
from . import works_analytics_cache as _analytics_cache
from . import works_sync as _sync
from . import works_views as _views
from . import works_exports as _exports


_IMPLEMENTATION_MODULES = (
    _shared,
    _openalex_data,
    _institution_analytics,
    _global_analytics,
    _analytics_cache,
    _sync,
    _views,
    _exports,
)

# Preserve imports used by existing extensions and tests during the transition.
for _module in _IMPLEMENTATION_MODULES:
    for _name, _value in vars(_module).items():
        if not _name.startswith("__") and _name not in globals():
            globals()[_name] = _value

del _module, _name, _value
