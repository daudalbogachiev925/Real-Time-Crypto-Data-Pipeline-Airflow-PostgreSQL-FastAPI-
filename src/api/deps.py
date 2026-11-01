"""FastAPI dependencies."""
from __future__ import annotations

from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import AsyncSession

from src.db.session import get_async_session


async def db_session() -> AsyncIterator[AsyncSession]:
    async with get_async_session() as session:
        yield session
