class ToolkitError(Exception):
    """Base exception for the toolkit."""


class DownloadError(ToolkitError):
    """Raised when a source download/fetch fails."""


import warnings as _warnings


class ValidationError(ToolkitError):
    """Raised when validation fails (schema, keys, required columns).

    Deprecato: il sistema di validazione usa :class:`ValidationResult`
    da ``toolkit.core.validation``. Questa eccezione è mantenuta come
    shim backward compat — non viene sollevata dal codice interno.
    Sarà rimossa in una versione futura.
    """

    def __init_subclass__(cls, **kwargs):
        _warnings.warn(
            f"{cls.__name__} eredita da ValidationError che è deprecato. "
            f"Usa ValidationResult da toolkit.core.validation.",
            DeprecationWarning,
            stacklevel=2,
        )
