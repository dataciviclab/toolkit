"""Domain layer — orchestrazione e logica di dominio del toolkit.

Separa la logica di dominio dallo strato di presentazione (CLI, MCP).
I moduli qui NON dipendono da ``toolkit.cli`` ne' da ``toolkit.mcp``.
Possono dipendere solo da ``toolkit.core``, ``toolkit.domain`` e ``lab_connectors``.
"""
