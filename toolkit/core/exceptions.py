class ToolkitError(Exception):
    """Base exception for the toolkit."""


class DownloadError(ToolkitError):
    """Raised when a source download/fetch fails."""


class ValidationError(ToolkitError):
    """Raised when validation fails (schema, keys, required columns)."""
