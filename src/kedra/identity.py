"""Stable decision identity and reversible, portable output filenames."""

import hashlib
import json
import re
from urllib.parse import quote, urlsplit, urlunsplit


def canonical_url(value: str) -> str:
    """Normalize host/scheme and remove fragments, preserving path case and query."""
    if not isinstance(value, str) or not value:
        raise ValueError("Expected a nonblank HTTP(S) URL")
    try:
        parts = urlsplit(value)
        if (
            parts.scheme not in ("http", "https")
            or not parts.hostname
            or parts.username is not None
            or parts.password is not None
            or any(c.isspace() or ord(c) < 32 for c in value)
        ):
            raise ValueError
        port = parts.port
    except ValueError:
        raise ValueError("Expected an HTTP(S) URL without credentials or whitespace") from None
    host = parts.hostname.lower()
    if ":" in host:
        host = f"[{host}]"
    if port is not None and (parts.scheme, port) not in (("http", 80), ("https", 443)):
        host = f"{host}:{port}"
    return urlunsplit((parts.scheme.lower(), host, parts.path or "/", parts.query, ""))


def stable_hash(value: dict[str, str | None] | list[str]) -> str:
    """Hash a deterministic JSON representation; this is not a downloaded-file hash."""
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def record_key(source: str, body_id: str, reference_number: str | None, source_url: str) -> str:
    """Identity excludes publication dates, titles, partitions and run provenance."""
    if any(not isinstance(value, str) or not value.strip() for value in (source, body_id)):
        raise ValueError("source and body_id must not be blank")
    if reference_number is not None and not isinstance(reference_number, str):
        raise ValueError("reference_number must be a string or None")
    url = canonical_url(source_url)
    reference = reference_number.strip() if reference_number else ""
    kind, value = ("reference", reference) if reference else ("url", url)
    return stable_hash([source.strip(), body_id.strip(), kind, value])


def identifier_filename(identifier: str, extension: str) -> str:
    """Keep identifiers reversible, including Windows-reserved names and literal %."""
    if not isinstance(identifier, str) or not identifier.strip():
        raise ValueError("identifier must not be blank")
    if extension not in ("html", "pdf", "doc", "docx"):
        raise ValueError("extension must be html, pdf, doc or docx")
    encoded = quote(identifier, safe="-_.")
    # quote leaves dots unescaped; trailing dots are unsafe on Windows.
    trimmed = encoded.rstrip(".")
    encoded = trimmed + "%2E" * (len(encoded) - len(trimmed))
    if re.fullmatch(r"CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9]", encoded.split(".")[0], re.I):
        encoded = f"%{ord(encoded[0]):02X}" + encoded[1:]
    filename = f"{encoded}.{extension}"
    if len(filename) > 255:
        raise ValueError("Encoded filename exceeds 255 bytes; do not truncate the identifier")
    return filename
