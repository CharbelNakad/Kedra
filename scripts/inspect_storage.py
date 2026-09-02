"""Read and hash-check persisted metadata and objects without changing either store."""

import argparse
import hashlib
import json
from collections import Counter
from contextlib import closing
from pathlib import Path, PurePosixPath

from botocore.exceptions import BotoCoreError, ClientError

from kedra.identity import identifier_filename
from kedra.orchestration import load_profile_settings
from kedra.storage import ObjectStore, StorageError, mongo_client, s3_client


def export_sample(path: Path, data: bytes) -> bool:
    """Create one local inspection copy, or verify an identical existing copy."""
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("xb") as stream:
            stream.write(data)
        return True
    except FileExistsError:
        if path.read_bytes() != data:
            raise ValueError(f"Existing inspection copy differs: {path}") from None
        return False


def inspect_layer(
    name: str,
    documents: list[dict],
    objects: ObjectStore,
    export_directory: Path | None,
    samples_per_format: int,
) -> tuple[dict, dict[str, bytes], list[str]]:
    formats: Counter[str] = Counter()
    samples: Counter[str] = Counter()
    bytes_by_version: dict[str, bytes] = {}
    exported: list[str] = []
    records = set()
    object_keys = set()
    total_bytes = 0
    ordered_documents = sorted(
        documents,
        key=lambda document: (
            document["document_format"],
            document.get("asset_role") == "wrapper",
            document["_id"],
        ),
    )
    for document in ordered_documents:
        version_id = document["_id"]
        file_hash = document["file_hash"]
        key = document["object_key"]
        data = objects.read(key, file_hash)
        if hashlib.sha256(data).hexdigest() != file_hash or len(data) != document["size_bytes"]:
            raise ValueError(f"{name} object integrity mismatch")
        document_format = document["document_format"]
        formats[document_format] += 1
        records.add(document["record_key"])
        object_keys.add(key)
        total_bytes += len(data)
        bytes_by_version[version_id] = data
        if export_directory is not None and samples[document_format] < samples_per_format:
            original_name = PurePosixPath(key).name
            export_path = (
                export_directory / name / document_format / f"{version_id[:12]}-{original_name}"
            )
            export_sample(export_path, data)
            exported.append(str(export_path.resolve()))
            samples[document_format] += 1
    return (
        {
            "metadata_versions": len(documents),
            "logical_records": len(records),
            "unique_object_keys": len(object_keys),
            "formats": dict(sorted(formats.items())),
            "verified_bytes": total_bytes,
            "verified_objects": len(documents),
        },
        bytes_by_version,
        exported,
    )


def listed_keys(client, bucket: str, prefix: str) -> set[str]:
    try:
        pages = client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
        return {item["Key"] for page in pages for item in page.get("Contents", [])}
    except (BotoCoreError, ClientError, KeyError, TypeError):
        raise StorageError("Object listing failed") from None


def inspect(args) -> dict:
    if not args.source.strip():
        raise ValueError("--source must be nonblank")
    settings = load_profile_settings(args.config, args.profile)
    storage = settings.storage
    with mongo_client(storage) as mongo, closing(s3_client(storage)) as s3:
        database = mongo[storage.mongo_database]
        landing_documents = list(
            database[storage.landing_collection].find({"source": args.source}).sort([("_id", 1)])
        )
        transformed_documents = list(
            database[storage.transformed_collection]
            .find({"source": args.source})
            .sort([("_id", 1)])
        )
        landing_objects = ObjectStore(s3, storage.landing_bucket, storage.object_prefix)
        transformed_objects = ObjectStore(s3, storage.transformed_bucket, storage.object_prefix)
        landing, landing_bytes, landing_exports = inspect_layer(
            "landing",
            landing_documents,
            landing_objects,
            args.export_directory,
            args.export_samples_per_format,
        )
        transformed, transformed_bytes, transformed_exports = inspect_layer(
            "transformed",
            transformed_documents,
            transformed_objects,
            args.export_directory,
            args.export_samples_per_format,
        )
        landing_listed = listed_keys(
            s3, storage.landing_bucket, f"{storage.object_prefix}/records/"
        )
        transformed_listed = listed_keys(
            s3, storage.transformed_bucket, f"{storage.object_prefix}/transformed/"
        )

    landing_referenced = {document["object_key"] for document in landing_documents}
    transformed_referenced = {document["object_key"] for document in transformed_documents}
    landing["listed_prefix_objects"] = len(landing_listed)
    landing["unreferenced_prefix_objects"] = len(landing_listed - landing_referenced)
    transformed["listed_prefix_objects"] = len(transformed_listed)
    transformed["unreferenced_prefix_objects"] = len(transformed_listed - transformed_referenced)

    landing_by_id = {document["_id"]: document for document in landing_documents}
    binary_exact_copies = 0
    html_hash_changes = 0
    resolved_links = 0
    for document in transformed_documents:
        landing_version_id = document["landing_version_id"]
        landing_document = landing_by_id.get(landing_version_id)
        if landing_document is None:
            raise ValueError("Transformed metadata points to a missing Landing version")
        if document["landing_file_hash"] != landing_document["file_hash"]:
            raise ValueError("Transformed metadata has the wrong Landing hash")
        expected_name = identifier_filename(document["identifier"], document["document_format"])
        if PurePosixPath(document["object_key"]).name != expected_name:
            raise ValueError("Transformed filename does not match identifier.ext")
        resolved_links += 1
        if document["document_format"] == "html":
            html_hash_changes += document["file_hash"] != document["landing_file_hash"]
        else:
            if transformed_bytes[document["_id"]] != landing_bytes[landing_version_id]:
                raise ValueError("A transformed binary is not an exact Landing copy")
            binary_exact_copies += 1

    return {
        "event": "storage_inspection_summary",
        "source": args.source,
        "landing": landing,
        "transformed": transformed,
        "cross_checks": {
            "resolved_transformed_to_landing_links": resolved_links,
            "binary_exact_copies": binary_exact_copies,
            "html_outputs_with_new_hash": html_hash_changes,
        },
        "exported_files": landing_exports + transformed_exports,
        "complete": bool(landing_documents and transformed_documents),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify persisted Mongo/S3 data read-only.")
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--profile", required=True, type=Path)
    parser.add_argument("--source", required=True)
    parser.add_argument("--export-directory", type=Path)
    parser.add_argument("--export-samples-per-format", type=int, default=0)
    args = parser.parse_args()
    if not 0 <= args.export_samples_per_format <= 10:
        parser.error("--export-samples-per-format must be between 0 and 10")
    try:
        summary = inspect(args)
    except (KeyError, OSError, StorageError, TypeError, ValueError) as error:
        print(
            json.dumps(
                {
                    "event": "storage_inspection_summary",
                    "complete": False,
                    "reason": "inspection_failed",
                    "error": str(error),
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 3
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 0 if summary["complete"] else 3


if __name__ == "__main__":
    raise SystemExit(main())
