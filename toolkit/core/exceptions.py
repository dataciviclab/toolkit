class ToolkitError(Exception):
    """Base exception for the toolkit."""


class DownloadError(ToolkitError):
    """Raised when a source download/fetch fails."""
