from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Generator

from dotenv import load_dotenv
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker


for candidate in (Path(__file__).resolve().parents[2] / '.env', Path(__file__).resolve().parents[3] / '.env'):
    if candidate.exists():
        load_dotenv(candidate, override=False)


def _build_database_url() -> str:
    direct_url = os.getenv('BACKEND_DATABASE_URL') or os.getenv('DATABASE_URL')
    if direct_url:
        if direct_url.startswith('postgresql+psycopg2://'):
            return direct_url
        if direct_url.startswith('postgresql://'):
            return direct_url.replace('postgresql://', 'postgresql+psycopg2://', 1)
        return direct_url

    required = {
        'DB_HOST': os.getenv('DB_HOST'),
        'DB_NAME': os.getenv('DB_NAME'),
        'BACKEND_USER_USERNAME': os.getenv('BACKEND_USER_USERNAME'),
        'BACKEND_USER_PASSWORD': os.getenv('BACKEND_USER_PASSWORD'),
    }
    missing = sorted(name for name, value in required.items() if not value)
    if missing:
        raise RuntimeError(
            'Database is not configured. Set BACKEND_DATABASE_URL or '
            'DB_HOST, DB_NAME, BACKEND_USER_USERNAME, and BACKEND_USER_PASSWORD.'
        )

    return (
        f"postgresql+psycopg2://{required['BACKEND_USER_USERNAME']}:{required['BACKEND_USER_PASSWORD']}"
        f"@{required['DB_HOST']}:{os.getenv('DB_PORT', '5432')}/{required['DB_NAME']}"
    )


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    return create_engine(_build_database_url(), pool_pre_ping=True, future=True)


@lru_cache(maxsize=1)
def _session_factory() -> sessionmaker[Session]:
    return sessionmaker(bind=get_engine(), autoflush=False, autocommit=False, future=True)


def get_db_session() -> Generator[Session, None, None]:
    try:
        session = _session_factory()()
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    try:
        yield session
    finally:
        session.close()
