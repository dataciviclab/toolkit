class ToolkitError(Exception):
    """Base exception for the toolkit."""


class DownloadError(ToolkitError):
    """Raised when a source download/fetch fails."""


# ValidationError è stato rimosso — il sistema di validazione usa
# ``ValidationResult`` da ``toolkit.core.validation``.
