"""Built-in plugin modules.

Plugin registration is explicit via `toolkit.core.registry.register_builtin_plugins()`.

Builtin stable sources exposed by the default runtime:

- `local_file`
- `http_file`
- `ckan`
- `sdmx`
"""

from toolkit.plugins.ckan import CkanSource
from toolkit.plugins.http_file import HttpFileSource
from toolkit.plugins.local_file import LocalFileSource
from toolkit.plugins.sdmx import SdmxSource

__all__ = [
    "LocalFileSource",
    "HttpFileSource",
    "CkanSource",
    "SdmxSource",
]
