import os
from dataclasses import dataclass

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:  # python-dotenv is optional
    pass

DEFAULT_DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/ledger"


@dataclass(frozen=True)
class Config:
    database_url: str
    batch_size: int
    pool_size: int


def load_config() -> Config:
    return Config(
        database_url=os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL),
        batch_size=int(os.environ.get("BATCH_SIZE", "5000")),
        pool_size=int(os.environ.get("POOL_SIZE", "16")),
    )
