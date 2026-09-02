"""Serve a deterministic WRC-shaped source for manual end-to-end verification."""

import argparse
import io
import json
import threading
import zipfile
from collections import Counter
from contextlib import suppress
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlencode, urlsplit

HOST = "127.0.0.1"
DEFAULT_PORT = 18766
SOURCE_NAME = "kedra-manual-demo-v1"
PUBLISHED_DATE = "17/07/2025"
BODY_IDS = ("2", "1", "3", "15376")
FORMATS = ("html", "pdf", "doc", "docx")
PAGE_SIZE = 2
OLE_HEADER = bytes.fromhex("d0cf11e0a1b11ae1")


def emit(event: dict) -> None:
    print(json.dumps(event, ensure_ascii=False, sort_keys=True), flush=True)


def pdf_bytes(text: str) -> bytes:
    """Build a small valid one-page PDF without an additional dependency."""
    escaped = text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
    stream = f"BT /F1 12 Tf 72 720 Td ({escaped}) Tj ET".encode("ascii")
    objects = (
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        (
            b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
            b"/Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>"
        ),
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
        b"<< /Length "
        + str(len(stream)).encode("ascii")
        + b" >>\nstream\n"
        + stream
        + b"\nendstream",
    )
    output = bytearray(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
    offsets = [0]
    for number, value in enumerate(objects, start=1):
        offsets.append(len(output))
        output.extend(f"{number} 0 obj\n".encode("ascii"))
        output.extend(value)
        output.extend(b"\nendobj\n")
    xref = len(output)
    output.extend(f"xref\n0 {len(objects) + 1}\n".encode("ascii"))
    output.extend(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        output.extend(f"{offset:010d} 00000 n \n".encode("ascii"))
    output.extend(
        (f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n").encode(
            "ascii"
        )
    )
    return bytes(output)


def docx_bytes(identifier: str) -> bytes:
    """Build a minimal WordprocessingML package recognized as DOCX."""
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        files = {
            "[Content_Types].xml": (
                '<?xml version="1.0" encoding="UTF-8"?>'
                '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
                '<Default Extension="rels" '
                'ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
                '<Default Extension="xml" ContentType="application/xml"/>'
                '<Override PartName="/word/document.xml" '
                'ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>'
                "</Types>"
            ),
            "_rels/.rels": (
                '<?xml version="1.0" encoding="UTF-8"?>'
                '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
                '<Relationship Id="rId1" '
                'Type="http://schemas.openxmlformats.org/officeDocument/2006/'
                'relationships/officeDocument" '
                'Target="word/document.xml"/>'
                "</Relationships>"
            ),
            "word/document.xml": (
                '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
                f"<w:body><w:p><w:r><w:t>{identifier}</w:t></w:r></w:p><w:sectPr/></w:body>"
                "</w:document>"
            ),
        }
        for name, value in files.items():
            info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, value.encode("utf-8"))
    return output.getvalue()


@dataclass(frozen=True)
class DemoRecord:
    body_id: str
    index: int
    identifier: str
    kind: str

    @property
    def path(self) -> str:
        extension = "html" if self.kind == "wrapper" else self.kind
        return f"/documents/{self.body_id}/{self.index}.{extension}"

    @property
    def attachment_path(self) -> str:
        return f"/attachments/{self.body_id}-{self.index}.pdf"


class DemoSite:
    def __init__(self, port: int, records_per_body: int):
        self.endpoint = f"http://{HOST}:{port}"
        self.records_per_body = records_per_body
        self.status_counts: Counter[int] = Counter()
        self.request_count = 0
        self._lock = threading.Lock()

    def records(self, body_id: str) -> list[DemoRecord]:
        body_offset = BODY_IDS.index(body_id) * self.records_per_body
        records = []
        for index in range(self.records_per_body):
            kind = FORMATS[(body_offset + index) % len(FORMATS)]
            if body_id == BODY_IDS[-1] and index == self.records_per_body - 1:
                kind = "wrapper"
            identifier = f"DEMO-{body_id}-{index + 1:04d}"
            records.append(DemoRecord(body_id, index + 1, identifier, kind))
        return records

    def find_record(self, path: str) -> DemoRecord | None:
        for body_id in BODY_IDS:
            for record in self.records(body_id):
                if record.path == path:
                    return record
        return None

    def find_attachment(self, path: str) -> DemoRecord | None:
        for body_id in BODY_IDS:
            for record in self.records(body_id):
                if record.kind == "wrapper" and record.attachment_path == path:
                    return record
        return None

    def note(self, status: int) -> None:
        with self._lock:
            self.request_count += 1
            self.status_counts[status] += 1


def media_type(kind: str) -> str:
    return {
        "html": "text/html; charset=utf-8",
        "wrapper": "text/html; charset=utf-8",
        "pdf": "application/pdf",
        "doc": "application/msword",
        "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    }[kind]


def record_body(record: DemoRecord) -> bytes:
    if record.kind == "html":
        return (
            "<!doctype html><html><body><header>Demo site header</header>"
            f'<h1 class="page-title">{record.identifier}</h1>'
            '<div class="content"><nav>Remove this navigation</nav>'
            f"<p>Relevant legal content for {record.identifier}.</p>"
            "<table><tr><th>Field</th><th>Value</th></tr>"
            f"<tr><td>Body</td><td>{record.body_id}</td></tr></table>"
            "<button>Remove this button</button></div>"
            "<footer>Demo site footer</footer></body></html>"
        ).encode()
    if record.kind == "wrapper":
        return (
            "<!doctype html><html><body>"
            f'<h1 class="page-title">{record.identifier}</h1>'
            '<div class="content"></div><div class="related-file">'
            f'<a class="download" href="{record.attachment_path}">Decision PDF</a>'
            "</div></body></html>"
        ).encode()
    if record.kind == "pdf":
        return pdf_bytes(f"Controlled legal decision {record.identifier}")
    if record.kind == "doc":
        return OLE_HEADER + f" Controlled DOC bytes for {record.identifier}".encode("ascii")
    return docx_bytes(record.identifier)


def search_body(site: DemoSite, body_id: str, page_number: int) -> bytes:
    records = site.records(body_id)
    start = (page_number - 1) * PAGE_SIZE
    selected = records[start : start + PAGE_SIZE]
    cards = []
    for record in selected:
        cards.append(
            '<li class="each-item">'
            f'<h2 class="title">{record.identifier}</h2>'
            f'<p class="description">Controlled record for body {body_id}.</p>'
            f'<span class="date">{PUBLISHED_DATE}</span>'
            f'<span class="refNO">Ref no: {record.identifier}</span>'
            f'<div class="link"><a href="{record.path}">View Page</a></div>'
            "</li>"
        )
    next_link = ""
    if start + PAGE_SIZE < len(records):
        query = urlencode(
            {
                "decisions": "1",
                "from": PUBLISHED_DATE,
                "to": PUBLISHED_DATE,
                "body": body_id,
                "pageNumber": page_number + 1,
            }
        )
        next_link = f'<a class="next-page" href="/search/?{query}">Next</a>'
    first = start + 1 if selected else 0
    last = start + len(selected)
    return (
        "<!doctype html><html><body>"
        f'<p class="results-count">Shows {first} to {last} of {len(records)} results</p>'
        f"{''.join(cards)}{next_link}</body></html>"
    ).encode()


def handler_for(site: DemoSite):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, format, *args):
            return

        def respond(
            self,
            status: int,
            body: bytes = b"",
            *,
            content_type: str | None = None,
            etag: str | None = None,
            event: dict | None = None,
        ) -> None:
            with suppress(OSError):
                self.send_response(status)
                if content_type is not None:
                    self.send_header("Content-Type", content_type)
                if etag is not None:
                    self.send_header("ETag", etag)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                if body:
                    self.wfile.write(body)
            site.note(status)
            emit(
                {
                    "event": "demo_source_request",
                    "method": "GET",
                    "path": urlsplit(self.path).path,
                    "status": status,
                    "response_body_bytes": len(body),
                    **(event or {}),
                }
            )

        def do_GET(self):
            parsed = urlsplit(self.path)
            if parsed.path == "/robots.txt":
                self.respond(200, b"User-agent: *\nDisallow:\n", content_type="text/plain")
                return
            if parsed.path == "/search/":
                query = parse_qs(parsed.query)
                body_id = query.get("body", [""])[0]
                expected = {
                    "decisions": ["1"],
                    "from": [PUBLISHED_DATE],
                    "to": [PUBLISHED_DATE],
                    "body": [body_id],
                }
                unexpected = set(query) - {*expected, "pageNumber"}
                try:
                    page_number = int(query.get("pageNumber", ["1"])[0])
                except ValueError:
                    page_number = 0
                if (
                    body_id not in BODY_IDS
                    or any(query.get(name) != value for name, value in expected.items())
                    or unexpected
                    or page_number < 1
                ):
                    self.respond(400, event={"kind": "listing", "reason": "invalid_filters"})
                    return
                body = search_body(site, body_id, page_number)
                self.respond(
                    200,
                    body,
                    content_type="text/html; charset=utf-8",
                    event={"kind": "listing", "body_id": body_id, "page_number": page_number},
                )
                return
            record = site.find_record(parsed.path)
            attachment = site.find_attachment(parsed.path)
            if record is None and attachment is None:
                self.respond(404, event={"kind": "unknown"})
                return
            active = record or attachment
            assert active is not None
            kind = active.kind if attachment is None else "pdf"
            body = (
                record_body(active)
                if attachment is None
                else pdf_bytes(f"Attached legal decision {active.identifier}")
            )
            etag = f'"demo-v1-{active.body_id}-{active.index}-{kind}"'
            conditional = self.headers.get("If-None-Match")
            if conditional == etag:
                self.respond(
                    304,
                    etag=etag,
                    event={"kind": "asset", "identifier": active.identifier, "conditional": True},
                )
                return
            self.respond(
                200,
                body,
                content_type=media_type(kind),
                etag=etag,
                event={
                    "kind": "asset",
                    "identifier": active.identifier,
                    "conditional": conditional is not None,
                },
            )

    return Handler


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Serve deterministic decision listings and assets on loopback only."
    )
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--records-per-body", type=int, default=3)
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    if not 3 <= args.records_per_body <= 20:
        parser.error("--records-per-body must be between 3 and 20")
    site = DemoSite(args.port, args.records_per_body)
    server = ThreadingHTTPServer((HOST, args.port), handler_for(site))
    server.daemon_threads = True
    emit(
        {
            "event": "demo_source_started",
            "endpoint": site.endpoint,
            "source": SOURCE_NAME,
            "published_date": "2025-07-17",
            "body_ids": list(BODY_IDS),
            "records_per_body": args.records_per_body,
            "records": len(BODY_IDS) * args.records_per_body,
            "instruction": "Press Ctrl+C after both pipeline runs finish.",
        }
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    emit(
        {
            "event": "demo_source_stopped",
            "requests": site.request_count,
            "status_counts": dict(sorted(site.status_counts.items())),
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
