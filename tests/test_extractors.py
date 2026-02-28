import io
import zipfile

import pytest

from toolkit.raw.extractors import extract_zip_all, extract_zip_first, extract_zip_first_csv


def _make_zip_bytes(files: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for name, content in files.items():
            z.writestr(name, content)
    return buf.getvalue()


def test_extract_zip_all_sanitizes_names_and_filters_ext():
    payload = _make_zip_bytes(
        {
            "../evil.csv": b"a,b\n1,2\n",
            "folder/good.csv": b"a,b\n3,4\n",
            "folder/ignore.txt": b"nope",
        }
    )

    out = extract_zip_all(payload, args={"only_ext": [".csv"]})
    # safe_name elimina path traversal e sottocartelle
    assert "evil.csv" in out
    assert "good.csv" in out
    assert "ignore.txt" not in out


def test_extract_zip_first_csv_returns_first_csv_only():
    payload = _make_zip_bytes(
        {
            "a.txt": b"hello",
            "b.csv": b"x,y\n1,2\n",
            "c.csv": b"x,y\n3,4\n",
        }
    )

    out = extract_zip_first_csv(payload)
    # prende il primo .csv (ordine namelist dello zip)
    assert list(out.keys()) == ["b.csv"]
    assert out["b.csv"].startswith(b"x,y")


@pytest.mark.parametrize(
    "fn",
    [extract_zip_all, extract_zip_first, extract_zip_first_csv],
)
def test_zip_extractors_raise_value_error_on_invalid_zip_payload(fn):
    with pytest.raises(ValueError, match="Invalid ZIP payload"):
        fn(b"not-a-zip")
