from pathlib import Path

import pytest

from kedra.config import load_settings

EXAMPLE = Path(__file__).resolve().parents[1] / "config.example.toml"


def test_example_config_and_secrets_load_without_connections(example_env):
    settings = load_settings(EXAMPLE, example_env)
    assert settings.source.body_ids == ("2", "1", "3", "15376")
    assert settings.scraping.partition_size == "month"
    assert settings.storage.landing_bucket != settings.storage.transformed_bucket
    for secret in example_env.values():
        assert secret not in repr(settings)


@pytest.mark.parametrize(
    "env_name", ["KEDRA_MONGO_URI", "KEDRA_S3_ACCESS_KEY_ID", "KEDRA_S3_SECRET_ACCESS_KEY"]
)
@pytest.mark.parametrize("value", [None, "", "   "])
def test_missing_or_blank_required_secrets_are_rejected(example_env, env_name, value):
    if value is None:
        example_env.pop(env_name)
    else:
        example_env[env_name] = value
    with pytest.raises(ValueError, match=env_name):
        load_settings(EXAMPLE, example_env)


@pytest.mark.parametrize(
    "old,new",
    [
        ('partition_size = "month"', 'partition_size = "week"'),
        ("download_delay_seconds = 2.0", "download_delay_seconds = 0"),
        ("timeout_seconds = 25.0", "timeout_seconds = nan"),
        ("timeout_seconds = 25.0", "timeout_seconds = inf"),
        ("concurrency_per_domain = 1", "concurrency_per_domain = true"),
        ("retry_times = 3", "retry_times = -1"),
        ("max_response_bytes = 20971520", "max_response_bytes = 0"),
        ('body_ids = ["2", "1", "3", "15376"]', "body_ids = []"),
        ('body_ids = ["2", "1", "3", "15376"]', 'body_ids = ["2", "2"]'),
        ('body_ids = ["2", "1", "3", "15376"]', "body_ids = [2]"),
        ('transformed_bucket = "kedra-transformed"', 'transformed_bucket = "kedra-landing"'),
        ('transformed_bucket = "kedra-transformed"', 'transformed_bucket = "INVALID_BUCKET"'),
        ('state_collection = "crawl_state"', 'state_collection = "landing_metadata"'),
        ('object_prefix = "workplace-relations"', 'object_prefix = "../landing"'),
        ('object_prefix = "workplace-relations"', 'object_prefix = "/absolute"'),
        ('name = "workplace-relations"', 'unknown = "workplace-relations"'),
    ],
)
def test_invalid_config_is_rejected(tmp_path, example_env, old, new):
    path = tmp_path / "config.toml"
    path.write_text(EXAMPLE.read_text().replace(old, new), encoding="utf-8")
    with pytest.raises(ValueError):
        load_settings(path, example_env)


def test_nonsecret_settings_can_be_changed_without_code_edits(tmp_path, example_env):
    path = tmp_path / "config.toml"
    text = EXAMPLE.read_text().replace('partition_size = "month"', 'partition_size = "day"')
    text = text.replace('body_ids = ["2", "1", "3", "15376"]', 'body_ids = ["3"]')
    text = text.replace('mongo_database = "kedra"', 'mongo_database = "kedra_test"')
    text = text.replace("http://localhost:8333", "http://localhost:9333")
    path.write_text(text, encoding="utf-8")
    settings = load_settings(path, example_env)
    assert settings.scraping.partition_size == "day"
    assert settings.source.body_ids == ("3",)
    assert settings.storage.mongo_database == "kedra_test"
    assert settings.storage.s3_endpoint_url == "http://localhost:9333"


def test_invalid_mongo_uri_does_not_disclose_its_value(example_env):
    example_env["KEDRA_MONGO_URI"] = "not-a-uri-with-sensitive-value"
    with pytest.raises(ValueError, match="MongoDB connection URI") as error:
        load_settings(EXAMPLE, example_env)
    assert "sensitive-value" not in str(error.value)


@pytest.mark.parametrize(
    "text",
    [
        '[source\nsecret = "sensitive-value"',
        'source = "sensitive-value"',
        EXAMPLE.read_text() + '\n[unexpected]\nvalue = "sensitive-value"',
        EXAMPLE.read_text() + '\nmongo_uri = "sensitive-value"',
    ],
)
def test_malformed_unknown_or_secret_config_does_not_echo_values(tmp_path, example_env, text):
    path = tmp_path / "config.toml"
    path.write_text(text, encoding="utf-8")
    with pytest.raises(ValueError) as error:
        load_settings(path, example_env)
    assert "sensitive-value" not in str(error.value)
