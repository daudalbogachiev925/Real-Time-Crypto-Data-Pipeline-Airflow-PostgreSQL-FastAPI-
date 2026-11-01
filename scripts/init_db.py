"""Create database schema."""
from __future__ import annotations

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.db.models import Base
from src.db.session import sync_engine
from src.utils.logging import configure_logging
from src.config import get_settings

configure_logging(get_settings().log_level)
logger = logging.getLogger("init_db")


def main() -> int:
    logger.info("Creating schema at %s", sync_engine.url)
    Base.metadata.create_all(sync_engine)
    logger.info("Schema created")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
