import io
import zipfile
from dataclasses import dataclass
from typing import Callable

@dataclass
class ExtractorSpec:
    type: str
    args: dict

def _safe_name(name: str) -> str:
    # evita path traversal e sottocartelle dentro gli zip
    name = name.replace("\\", "/").split("/")[-1]
    return name or "file.bin"

def extract_identity(payload: bytes, args: dict | None = None) -> dict[str, bytes]:
    return {"file.bin": payload}


def _open_zip(payload: bytes) -> zipfile.ZipFile:
    try:
        return zipfile.ZipFile(io.BytesIO(payload))
    except (zipfile.BadZipFile, zipfile.LargeZipFile) as e:
        raise ValueError("Invalid ZIP payload") from e


def extract_zip_all(payload: bytes, args: dict | None = None) -> dict[str, bytes]:
    args = args or {}
    only_ext = set(args.get("only_ext", []))  # es: [".csv"]
    out: dict[str, bytes] = {}
    z = _open_zip(payload)
    for n in z.namelist():
        if n.endswith("/"):
            continue
        nn = _safe_name(n)
        if only_ext and not any(nn.lower().endswith(e.lower()) for e in only_ext):
            continue
        out[nn] = z.read(n)
    return out

def extract_zip_first(payload: bytes, args: dict | None = None) -> dict[str, bytes]:
    z = _open_zip(payload)
    names = [n for n in z.namelist() if not n.endswith("/")]
    if not names:
        return {}
    n = names[0]
    return {_safe_name(n): z.read(n)}

def extract_zip_first_csv(payload: bytes, args: dict | None = None) -> dict[str, bytes]:
    z = _open_zip(payload)
    names = [n for n in z.namelist() if (not n.endswith("/")) and n.lower().endswith(".csv")]
    if not names:
        return {}
    n = names[0]
    return {_safe_name(n): z.read(n)}

_EXTRACTORS: dict[str, Callable[[bytes, dict | None], dict[str, bytes]]] = {
    "identity": extract_identity,
    "unzip_all": extract_zip_all,
    "unzip_first": extract_zip_first,
    "unzip_first_csv": extract_zip_first_csv,
}

def get_extractor(spec: dict | None):
    """
    spec:
      { type: "identity" | "unzip_all" | "unzip_first" | "unzip_first_csv", args: {...} }
    """
    if not spec:
        return _EXTRACTORS["identity"], {}
    etype = spec.get("type") or "identity"
    eargs = spec.get("args", {}) or {}
    if etype not in _EXTRACTORS:
        raise ValueError(f"Unknown extractor: {etype}")
    return _EXTRACTORS[etype], eargs
