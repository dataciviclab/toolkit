class ToolkitError(Exception):
    """Base exception for the toolkit."""


class DownloadError(ToolkitError):
    """Raised when a source download/fetch fails."""


class EmptyResultError(DownloadError):
    """La query è andata a buon fine ma non ha restituito righe.

    Distinta da ``DownloadError`` perché per un fetch incrementale "nessun
    dato nuovo" è uno stato legittimo (no-op), non un errore. Il plugin la
    solleva; il chiamante incrementale la tratta come delta vuoto.
    """
