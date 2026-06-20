def should_write(layer: str, artifact_name: str, cfg: object = None) -> bool:
    """Decide whether a given artifact should be produced.

    All artifacts are always written — no special cases.
    """
    return True
