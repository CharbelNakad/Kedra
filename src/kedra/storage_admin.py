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


def prepare(config: Path, mongo_port: int, directory: Path = LOCAL_DIRECTORY) -> None:
    public = _public_storage(config)
    endpoint = urlsplit(public["s3_endpoint_url"])
    if endpoint.scheme != "http" or endpoint.hostname not in ("localhost", "127.0.0.1"):
        raise ValueError("Local Compose setup requires a loopback HTTP S3 endpoint")
    if endpoint.path not in ("", "/") or endpoint.query or endpoint.fragment:
        raise ValueError("Local S3 endpoint must not contain a path, query or fragment")
    if not 1 <= mongo_port <= 65535:
        raise ValueError("Mongo port must be between 1 and 65535")
    if directory.exists():
        local_settings(config, "admin", directory)
        saved = json.loads((directory / "credentials.json").read_text(encoding="utf-8"))
        if saved["mongo_port"] != mongo_port:
            raise ValueError("Mongo port differs from the existing local provisioning")
        for name in (
            "mongo-root-password",
            "s3.json",
            "compose.env",
            "ingest.env",
            "transform.env",
        ):
            if not (directory / name).is_file():
                raise ValueError(
                    "Local provisioning is incomplete; recover files without rotating secrets"
                )
        return

    credentials = {}
    identities = []
    database = quote(public["mongo_database"], safe="")
    root_password = secrets.token_urlsafe(32)
    landing, transformed = public["landing_bucket"], public["transformed_bucket"]
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
    for role in ("admin", "ingest", "transform"):
        password = root_password if role == "admin" else secrets.token_urlsafe(32)
        auth_database = "admin" if role == "admin" else database
        access_key, secret_key = secrets.token_hex(12), secrets.token_urlsafe(32)
        credentials[role] = {
            "mongo_uri": f"mongodb://kedra-{role}:{password}@127.0.0.1:{mongo_port}/?authSource={auth_database}",
            "s3_access_key_id": access_key,
            "s3_secret_access_key": secret_key,
        }
        identities.append(
            {
                "name": f"kedra-{role}",
                "credentials": [{"accessKey": access_key, "secretKey": secret_key}],
                "actions": actions[role],
            }
        )

    # Refuse to replace any existing credentials. Losing them does not justify deleting volumes.
    directory.mkdir(mode=0o700, parents=True, exist_ok=False)
    files = {
        "credentials.json": json.dumps(
            {"storage": public, "mongo_port": mongo_port, "credentials": credentials}, indent=2
        ),
        "mongo-root-password": root_password,
        "s3.json": json.dumps({"identities": identities}, indent=2),
        "compose.env": f"KEDRA_MONGO_PORT={mongo_port}\nKEDRA_S3_PORT={endpoint.port or 80}\n",
    }
    for role in ("ingest", "transform"):
        profile = credentials[role]
        files[f"{role}.env"] = (
            f"KEDRA_MONGO_URI={profile['mongo_uri']}\n"
            f"KEDRA_S3_ACCESS_KEY_ID={profile['s3_access_key_id']}\n"
            f"KEDRA_S3_SECRET_ACCESS_KEY={profile['s3_secret_access_key']}\n"
        )
    for name, content in files.items():
        descriptor = os.open(directory / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as file:
            file.write(content)


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
                db.create_collection(name)
        for name in (admin.landing_collection, admin.transformed_collection):
            db[name].create_index(
                [("published_date", 1), ("_id", 1)], name="published_date_version"
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
