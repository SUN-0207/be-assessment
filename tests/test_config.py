import importlib

import dotenv

import ledger.config as config_module


def _disable_dotenv(monkeypatch):
    # Isolate these unit tests from any ambient .env on the developer's machine
    # (config.py calls load_dotenv() at import). We are testing load_config's
    # handling of the process environment, not whatever .env happens to exist.
    monkeypatch.setattr(dotenv, "load_dotenv", lambda *a, **k: False)


def test_defaults(monkeypatch):
    _disable_dotenv(monkeypatch)
    for var in ("DATABASE_URL", "BATCH_SIZE", "POOL_SIZE"):
        monkeypatch.delenv(var, raising=False)
    importlib.reload(config_module)
    cfg = config_module.load_config()
    assert cfg.database_url == "postgresql://postgres:postgres@localhost:5432/ledger"
    assert cfg.batch_size == 5000
    assert cfg.pool_size == 16


def test_overrides(monkeypatch):
    _disable_dotenv(monkeypatch)
    monkeypatch.setenv("DATABASE_URL", "postgresql://x@y/z")
    monkeypatch.setenv("BATCH_SIZE", "10")
    monkeypatch.setenv("POOL_SIZE", "4")
    importlib.reload(config_module)
    cfg = config_module.load_config()
    assert cfg.database_url == "postgresql://x@y/z"
    assert cfg.batch_size == 10
    assert cfg.pool_size == 4
