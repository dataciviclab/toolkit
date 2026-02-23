"""Built-in source plugins.

Plugins are registered into `toolkit.core.registry.registry`.
"""

_REGISTERED = False


def register_plugins() -> None:
    global _REGISTERED
    if _REGISTERED:
        return

    # Import side-effects register factories
    from .http_file import HttpFileSource  # noqa: F401
    from .local_file import LocalFileSource  # noqa: F401

    # Optional plugins (can be implemented later)
    try:
        from .api_json_paged import ApiJsonPagedSource  # noqa: F401
    except Exception:
        pass

    try:
        from .html_table import HtmlTableSource  # noqa: F401
    except Exception:
        pass

    _REGISTERED = True
