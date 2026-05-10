from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.schemas.stocks import OHLCVBar, PaginatedTickers, TechnicalFeatures, Ticker
from app.services import stock_service

router = APIRouter(prefix="/stocks", tags=["stocks"])


@router.get("", response_model=PaginatedTickers)
async def list_tickers(
    sector: str | None = Query(None),
    exchange: str | None = Query(None),
    is_active: bool = Query(True),
    limit: int = Query(50, le=500),
    offset: int = Query(0),
    db: AsyncSession = Depends(get_db),
):
    total, items = await stock_service.list_tickers(db, sector, exchange, is_active, limit, offset)
    return PaginatedTickers(total=total, limit=limit, offset=offset, items=items)


@router.get("/{symbol}", response_model=Ticker)
async def get_ticker(symbol: str, db: AsyncSession = Depends(get_db)):
    ticker = await stock_service.get_ticker(db, symbol)
    if not ticker:
        raise HTTPException(status_code=404, detail="Ticker not found")
    return ticker


@router.get("/{symbol}/prices", response_model=list[OHLCVBar])
async def get_prices(
    symbol: str,
    from_: date = Query(..., alias="from"),
    to: date | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    bars = await stock_service.get_prices(db, symbol, from_, to)
    if bars is None:
        raise HTTPException(status_code=404, detail="Ticker not found")
    return bars


@router.get("/{symbol}/technical", response_model=list[TechnicalFeatures])
async def get_technical(
    symbol: str,
    from_: date | None = Query(None, alias="from"),
    to: date | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    features = await stock_service.get_technical(db, symbol, from_, to)
    if features is None:
        raise HTTPException(status_code=404, detail="Ticker not found")
    return features
