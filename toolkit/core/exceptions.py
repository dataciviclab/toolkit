class ToolkitError(Exception):
    """Base exception for the toolkit."""


class DownloadError(ToolkitError):
    """Raised when a source download/fetch fails."""


class ValidationError(ToolkitError):
    """Raised when validation fails (schema, keys, required columns).

    Deprecated: il sistema di validazione usa :class:`ValidationResult`
    da ``toolkit.core.validation``, non eccezioni. ``ValidationError``
    non è importata in produzione e potrebbe essere rimossa in una versione
    futura.
    """
