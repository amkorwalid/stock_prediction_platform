from datetime import date
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.database import get_db_session
from app.schemas import OHLCVBar, PaginatedTickers, TechnicalFeatures, Ticker
from app.services import StockService

router = APIRouter(prefix='/stocks', tags=['stocks'])
DBSession = Annotated[Session, Depends(get_db_session)]


@router.get('', response_model=PaginatedTickers)
def list_stocks(
    db: DBSession,
    sector: str | None = None,
    exchange: str | None = None,
    is_active: bool = True,
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> PaginatedTickers:
    return StockService.list_tickers(db, sector, exchange, is_active, limit, offset)


@router.get('/{symbol}', response_model=Ticker)
def get_stock(symbol: str, db: DBSession) -> Ticker:
    return StockService.get_ticker(db, symbol)


@router.get('/{symbol}/prices', response_model=list[OHLCVBar])
def list_prices(
    symbol: str,
    db: DBSession,
    date_from: date = Query(alias='from'),
    date_to: date | None = Query(default=None, alias='to'),
) -> list[OHLCVBar]:
    return StockService.list_prices(db, symbol, date_from, date_to)


@router.get('/{symbol}/technical', response_model=list[TechnicalFeatures])
def list_technical(
    symbol: str,
    db: DBSession,
    date_from: date | None = Query(default=None, alias='from'),
    date_to: date | None = Query(default=None, alias='to'),
) -> list[TechnicalFeatures]:
    return StockService.list_technicals(db, symbol, date_from, date_to)
