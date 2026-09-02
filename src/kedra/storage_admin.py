"""Explicit local setup: generate credentials, then provision roles, buckets and indexes."""

import argparse
import json
import os
import secrets
import sys
from contextlib import closing
from dataclasses import asdict
from pathlib import Path
from urllib.parse import quote, urlsplit

from botocore.exceptions import BotoCoreError, ClientError
from pymongo.errors import PyMongoError

from kedra.config import StorageSettings, load_settings
from kedra.storage import StorageError, mongo_client, s3_client

LOCAL_DIRECTORY = Path(".local")
SECRET_FIELDS = {"mongo_uri", "s3_access_key_id", "s3_secret_access_key"}
PROVISIONED_FIELDS = (
    "mongo_database",
    "landing_collection",
    "transformed_collection",
    "state_collection",
    "s3_endpoint_url",
    "landing_bucket",
    "transformed_bucket",
)


def _public_storage(config: Path) -> dict:
    settings = load_settings(
        config,
        {
            "KEDRA_MONGO_URI": "mongodb://localhost:27017",
            "KEDRA_S3_ACCESS_KEY_ID": "configuration-preview",
            "KEDRA_S3_SECRET_ACCESS_KEY": "configuration-preview",
        },
    )
    return {
        key: value for key, value in asdict(settings.storage).items() if key not in SECRET_FIELDS
    }


def local_settings(config: Path, role: str, directory: Path = LOCAL_DIRECTORY) -> StorageSettings:
    """Read credentials only for explicit local administration and integration checks."""
    public = _public_storage(config)
    try:
        local = json.loads((directory / "credentials.json").read_text(encoding="utf-8"))
        if any(local["storage"][key] != public[key] for key in PROVISIONED_FIELDS):
            raise ValueError(
                "Storage config differs from local provisioning; do not reset data or secrets"
            )
        return StorageSettings(**public, **local["credentials"][role])
    except (KeyError, TypeError, json.JSONDecodeError):
        raise ValueError(
            "Local credential file is invalid; restore it without resetting data"
        ) from None


def _new_manifest(public: dict, mongo_port: int) -> dict:
    credentials = {}
    database = quote(public["mongo_database"], safe="")
    root_password = secrets.token_urlsafe(32)
    for role in ("admin", "ingest", "transform"):
        password = root_password if role == "admin" else secrets.token_urlsafe(32)
        auth_database = "admin" if role == "admin" else database
        credentials[role] = {
            "mongo_uri": (
                f"mongodb://kedra-{role}:{password}@127.0.0.1:{mongo_port}/"
                f"?authSource={auth_database}"
            ),
            "s3_access_key_id": secrets.token_hex(12),
            "s3_secret_access_key": secrets.token_urlsafe(32),
        }
    return {"storage": public, "mongo_port": mongo_port, "credentials": credentials}


def _load_manifest(path: Path) -> dict:
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(manifest, dict):
            raise TypeError
        return manifest
    except (OSError, TypeError, json.JSONDecodeError):
        raise ValueError(
            "Local credential file is invalid; restore it without resetting data"
        ) from None


def _derived_files(manifest: dict) -> dict[str, str]:
    public = manifest["storage"]
    credentials = manifest["credentials"]
    landing = public["landing_bucket"]
    transformed = public["transformed_bucket"]
    actions = {
        "admin": ["Admin", "Read", "Write", "List", "Tagging"],
        "ingest": [f"Read:{landing}", f"List:{landing}", f"Write:{landing}"],
        "transform": [
            f"Read:{landing}",
            f"List:{landing}",
            f"Read:{transformed}",
            f"List:{transformed}",
            f"Write:{transformed}",
        ],
    }
    identities = []
    for role in ("admin", "ingest", "transform"):
        profile = credentials[role]
        identities.append(
            {
                "name": f"kedra-{role}",
                "credentials": [
                    {
                        "accessKey": profile["s3_access_key_id"],
                        "secretKey": profile["s3_secret_access_key"],
                    }
                ],
                "actions": actions[role],
            }
        )
    endpoint = urlsplit(public["s3_endpoint_url"])
    root_password = urlsplit(credentials["admin"]["mongo_uri"]).password
    if not root_password:
        raise ValueError("Local administrator credential is invalid")
    files = {
        "mongo-root-password": root_password,
        "s3.json": json.dumps({"identities": identities}, indent=2),
        "compose.env": (
            f"KEDRA_MONGO_PORT={manifest['mongo_port']}\nKEDRA_S3_PORT={endpoint.port or 80}\n"
        ),
    }
    for role in ("ingest", "transform"):
        profile = credentials[role]
        files[f"{role}.env"] = (
            f"KEDRA_MONGO_URI={profile['mongo_uri']}\n"
            f"KEDRA_S3_ACCESS_KEY_ID={profile['s3_access_key_id']}\n"
            f"KEDRA_S3_SECRET_ACCESS_KEY={profile['s3_secret_access_key']}\n"
        )
    return files


def _write_or_validate(path: Path, content: str) -> None:
    if path.exists():
        if not path.is_file() or path.read_text(encoding="utf-8") != content:
            raise ValueError(f"Existing local provisioning file {path.name} differs; restore it")
        return
    pending = path.with_name(path.name + ".pending")
    if pending.exists():
        if pending.is_file() and pending.read_text(encoding="utf-8") == content:
            pending.rename(path)
            return
        if not pending.is_file():
            raise ValueError(f"Local provisioning path {pending.name} must be a file")
        # This is a derived temporary file; the credential manifest remains authoritative.
        pending.unlink()
    descriptor = os.open(pending, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as file:
        file.write(content)
        file.flush()
        os.fsync(file.fileno())
    pending.rename(path)


def prepare(config: Path, mongo_port: int, directory: Path = LOCAL_DIRECTORY) -> None:
    public = _public_storage(config)
    endpoint = urlsplit(public["s3_endpoint_url"])
    if endpoint.scheme != "http" or endpoint.hostname not in ("localhost", "127.0.0.1"):
        raise ValueError("Local Compose setup requires a loopback HTTP S3 endpoint")
    if endpoint.path not in ("", "/") or endpoint.query or endpoint.fragment:
        raise ValueError("Local S3 endpoint must not contain a path, query or fragment")
    if not 1 <= mongo_port <= 65535:
        raise ValueError("Mongo port must be between 1 and 65535")
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    manifest_path = directory / "credentials.json"
    pending_path = directory / "credentials.json.pending"
    if manifest_path.exists():
        manifest = _load_manifest(manifest_path)
    elif pending_path.exists():
        manifest = _load_manifest(pending_path)
        pending_path.rename(manifest_path)
    elif any(directory.iterdir()):
        raise ValueError(
            "Local credential manifest is missing; restore it without rotating secrets"
        )
    else:
        manifest = _new_manifest(public, mongo_port)
        _write_or_validate(manifest_path, json.dumps(manifest, indent=2))

    try:
        if any(manifest["storage"][key] != public[key] for key in PROVISIONED_FIELDS):
            raise ValueError(
                "Storage config differs from local provisioning; do not reset data or secrets"
            )
        if manifest["mongo_port"] != mongo_port:
            raise ValueError("Mongo port differs from the existing local provisioning")
        for role in ("admin", "ingest", "transform"):
            StorageSettings(**public, **manifest["credentials"][role])
        derived = _derived_files(manifest)
    except (KeyError, TypeError):
        raise ValueError(
            "Local credential file is invalid; restore it without resetting data"
        ) from None
    for name, content in derived.items():
        _write_or_validate(directory / name, content)


def landing_policy(bucket: str) -> dict:
    """Deny object edits/deletes; the private gateway also requires conditional uploads."""
    resource = f"arn:aws:s3:::{bucket}/*"
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "DenyObjectMutation",
                "Effect": "Deny",
                "Principal": "*",
                "Action": [
                    "s3:DeleteObject",
                    "s3:DeleteObjectVersion",
                    "s3:PutObjectAcl",
                    "s3:PutObjectTagging",
                    "s3:DeleteObjectTagging",
                    "s3:PutObjectRetention",
                    "s3:PutObjectLegalHold",
                ],
                "Resource": resource,
            },
        ],
    }


def _permission_set(privileges: list[dict]) -> set[str]:
    # Mongo returns actions in canonical order, which need not match the creation command.
    return {
        json.dumps(
            {"resource": item["resource"], "actions": sorted(item["actions"])}, sort_keys=True
        )
        for item in privileges
    }


def landing_validator() -> dict:
    """Require the canonical BSON shape for every new Landing metadata version."""
    required = [
        "_id",
        "schema_version",
        "record_key",
        "asset_id",
        "source",
        "body_id",
        "title",
        "identifier",
        "reference_number",
        "description",
        "published_date",
        "source_date_raw",
        "date_semantics",
        "source_url",
        "partition_date",
        "partition_size",
        "metadata_hash",
        "object_bucket",
        "object_key",
        "file_hash",
        "size_bytes",
        "document_format",
    ]
    sha256 = "^[0-9a-f]{64}$"
    return {
        "$jsonSchema": {
            "bsonType": "object",
            "required": required,
            "properties": {
                "_id": {"bsonType": "string", "pattern": sha256},
                "schema_version": {"enum": [1]},
                "record_key": {"bsonType": "string", "pattern": sha256},
                "asset_id": {"bsonType": "string", "minLength": 1},
                "source": {"bsonType": "string", "minLength": 1},
                "body_id": {"bsonType": "string", "minLength": 1},
                "title": {"bsonType": "string", "minLength": 1},
                "identifier": {"bsonType": "string", "minLength": 1},
                "reference_number": {"bsonType": ["string", "null"]},
                "description": {"bsonType": ["string", "null"]},
                "published_date": {"bsonType": "date"},
                "source_date_raw": {"bsonType": "string", "minLength": 1},
                "date_semantics": {"enum": ["decision_or_determination_date"]},
                "source_url": {"bsonType": "string", "minLength": 1},
                "partition_date": {"bsonType": "date"},
                "partition_size": {"enum": ["month", "day"]},
                "metadata_hash": {"bsonType": "string", "pattern": sha256},
                "object_bucket": {"bsonType": "string", "minLength": 1},
                "object_key": {"bsonType": "string", "minLength": 1},
                "file_hash": {"bsonType": "string", "pattern": sha256},
                "size_bytes": {"bsonType": ["int", "long"], "minimum": 0},
                "document_format": {"enum": ["html", "pdf", "doc", "docx"]},
                "asset_role": {"enum": ["primary", "wrapper", "attachment", "continuation"]},
                "asset_source_url": {"bsonType": "string", "minLength": 1},
                "asset_final_url": {"bsonType": "string", "minLength": 1},
                "media_type": {"bsonType": "string", "minLength": 1},
            },
        }
    }


def transformed_validator() -> dict:
    """Require complete output provenance for every new transformed metadata version."""
    required = [
        "_id",
        "schema_version",
        "landing_version_id",
        "transform_version",
        "record_key",
        "asset_id",
        "asset_role",
        "source",
        "body_id",
        "title",
        "identifier",
        "reference_number",
        "description",
        "published_date",
        "source_date_raw",
        "date_semantics",
        "source_url",
        "partition_date",
        "partition_size",
        "asset_source_url",
        "asset_final_url",
        "media_type",
        "landing_object_bucket",
        "landing_object_key",
        "landing_file_hash",
        "object_bucket",
        "object_key",
        "file_hash",
        "size_bytes",
        "document_format",
        "content_transformed",
    ]
    sha256 = "^[0-9a-f]{64}$"
    return {
        "$jsonSchema": {
            "bsonType": "object",
            "required": required,
            "properties": {
                "_id": {"bsonType": "string", "pattern": sha256},
                "schema_version": {"enum": [1]},
                "landing_version_id": {"bsonType": "string", "pattern": sha256},
                "transform_version": {"bsonType": "string", "minLength": 1},
                "record_key": {"bsonType": "string", "pattern": sha256},
                "asset_id": {"bsonType": "string", "minLength": 1},
                "asset_role": {"enum": ["primary", "wrapper", "attachment", "continuation"]},
                "source": {"bsonType": "string", "minLength": 1},
                "body_id": {"bsonType": "string", "minLength": 1},
                "title": {"bsonType": "string", "minLength": 1},
                "identifier": {"bsonType": "string", "minLength": 1},
                "reference_number": {"bsonType": ["string", "null"]},
                "description": {"bsonType": ["string", "null"]},
                "published_date": {"bsonType": "date"},
                "source_date_raw": {"bsonType": "string", "minLength": 1},
                "date_semantics": {"enum": ["decision_or_determination_date"]},
                "source_url": {"bsonType": "string", "minLength": 1},
                "partition_date": {"bsonType": "date"},
                "partition_size": {"enum": ["month", "day"]},
                "asset_source_url": {"bsonType": "string", "minLength": 1},
                "asset_final_url": {"bsonType": "string", "minLength": 1},
                "media_type": {"bsonType": "string", "minLength": 1},
                "landing_object_bucket": {"bsonType": "string", "minLength": 1},
                "landing_object_key": {"bsonType": "string", "minLength": 1},
                "landing_file_hash": {"bsonType": "string", "pattern": sha256},
                "object_bucket": {"bsonType": "string", "minLength": 1},
                "object_key": {"bsonType": "string", "minLength": 1},
                "file_hash": {"bsonType": "string", "pattern": sha256},
                "size_bytes": {"bsonType": ["int", "long"], "minimum": 1},
                "document_format": {"enum": ["html", "pdf", "doc", "docx"]},
                "content_transformed": {"bsonType": "bool"},
            },
        }
    }


def bootstrap(config: Path, directory: Path = LOCAL_DIRECTORY) -> None:
    admin = local_settings(config, "admin", directory)
    with mongo_client(admin) as client:
        client.admin.command("ping")
        db = client[admin.mongo_database]
        existing = db.list_collection_names()
        for name in (
            admin.landing_collection,
            admin.transformed_collection,
            admin.state_collection,
        ):
            if name not in existing:
                validator = None
                if name == admin.landing_collection:
                    validator = landing_validator()
                elif name == admin.transformed_collection:
                    validator = transformed_validator()
                options = (
                    {
                        "validator": validator,
                        "validationLevel": "strict",
                        "validationAction": "error",
                    }
                    if validator is not None
                    else {}
                )
                db.create_collection(name, **options)
        # collMod applies validation to future writes without rewriting preserved records.
        db.command(
            "collMod",
            admin.landing_collection,
            validator=landing_validator(),
            validationLevel="strict",
            validationAction="error",
        )
        db.command(
            "collMod",
            admin.transformed_collection,
            validator=transformed_validator(),
            validationLevel="strict",
            validationAction="error",
        )
        for name in (admin.landing_collection, admin.transformed_collection):
            db[name].create_index(
                [("published_date", 1), ("_id", 1)], name="published_date_version"
            )
        logical_fields = [
            ("record_key", 1),
            ("asset_id", 1),
            ("metadata_hash", 1),
            ("file_hash", 1),
            ("document_format", 1),
        ]
        db[admin.landing_collection].create_index(
            logical_fields,
            name="logical_landing_version",
            unique=True,
            partialFilterExpression={field: {"$type": "string"} for field, _ in logical_fields},
        )
        transformed_logical_fields = [("landing_version_id", 1), ("transform_version", 1)]
        db[admin.transformed_collection].create_index(
            transformed_logical_fields,
            name="logical_transformed_version",
            unique=True,
            partialFilterExpression={
                field: {"$type": "string"} for field, _ in transformed_logical_fields
            },
        )
        privileges = {
            "ingest": [
                {
                    "resource": {"db": db.name, "collection": admin.landing_collection},
                    "actions": ["find", "insert"],
                },
                {
                    "resource": {"db": db.name, "collection": admin.state_collection},
                    "actions": ["find", "insert", "update", "remove"],
                },
            ],
            "transform": [
                {
                    "resource": {"db": db.name, "collection": admin.landing_collection},
                    "actions": ["find"],
                },
                {
                    "resource": {"db": db.name, "collection": admin.transformed_collection},
                    "actions": ["find", "insert"],
                },
            ],
        }
        for role, permissions in privileges.items():
            name = f"kedra-{role}"
            current = db.command("rolesInfo", name, showPrivileges=True)["roles"]
            if not current:
                db.command("createRole", name, privileges=permissions, roles=[])
            elif (
                _permission_set(current[0]["privileges"]) != _permission_set(permissions)
                or current[0]["roles"]
            ):
                raise StorageError(
                    "Existing Mongo role differs from the required restricted permissions"
                )
            current_users = db.command("usersInfo", name)["users"]
            roles = [{"role": name, "db": db.name}]
            if not current_users:
                password = urlsplit(local_settings(config, role, directory).mongo_uri).password
                db.command("createUser", name, pwd=password, roles=roles)
            elif current_users[0]["roles"] != roles:
                raise StorageError("Existing Mongo user has unexpected roles")

    with closing(s3_client(admin)) as client:
        for bucket in (admin.landing_bucket, admin.transformed_bucket):
            try:
                client.head_bucket(Bucket=bucket)
            except ClientError as error:
                if error.response["Error"]["Code"] not in ("404", "NoSuchBucket"):
                    raise
                client.create_bucket(Bucket=bucket)
        policy = landing_policy(admin.landing_bucket)
        try:
            current_policy = json.loads(
                client.get_bucket_policy(Bucket=admin.landing_bucket)["Policy"]
            )
        except ClientError as error:
            if error.response["Error"]["Code"] != "NoSuchBucketPolicy":
                raise
            client.put_bucket_policy(Bucket=admin.landing_bucket, Policy=json.dumps(policy))
        else:
            if current_policy != policy:
                raise StorageError(
                    "Existing Landing bucket policy differs; inspect it without resetting data"
                )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "bootstrap"))
    parser.add_argument("--config", type=Path, default=Path("config.example.toml"))
    parser.add_argument("--mongo-port", type=int, default=27017)
    args = parser.parse_args(argv)
    try:
        if args.action == "prepare":
            prepare(args.config, args.mongo_port)
        else:
            bootstrap(args.config)
    except (PyMongoError, BotoCoreError, ClientError):
        print("Storage setup failed: check local service health and credentials", file=sys.stderr)
        return 2
    except (ValueError, OSError, StorageError) as error:
        print(f"Storage setup failed: {error}", file=sys.stderr)
        return 2
    print(json.dumps({"status": "ready", "action": args.action}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
