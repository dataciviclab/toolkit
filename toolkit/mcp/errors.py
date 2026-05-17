"""Error types for the MCP toolkit client.

ToolkitClientError è un ponte verso lab_connectors.mcp.
Tutte le eccezioni interne diventano McpError con codice UNEXPECTED
e vengono gestite da ``guard_timed()`` in server.py.
"""

from __future__ import annotations

from lab_connectors.mcp.errors import McpError, ErrorCode


class ToolkitClientError(McpError):
    """Raised when the toolkit CLI fails or returns an unexpected result.

    Ponte verso lab_connectors.mcp: eredita McpError, quindi
    ``guard()`` lo cattura come McpError e produce
    ``{"error": code.value, "message": "..."}``.

    Args:
        message: human-readable description of the error.
        code: tassonomic error code. Defaults to UNEXPECTED (conservative).
              Call sites should use the most specific code available.
    """

    def __init__(
        self, message: str, code: ErrorCode = ErrorCode.UNEXPECTED
    ) -> None:
        super().__init__(code, message)
