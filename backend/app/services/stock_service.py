from datetime import date

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.stocks import DimDate, DimTicker, FactDailyPrices
from app.models.features import FeatTechnical
from app.schemas.stocks import OHLCVBar, TechnicalFeatures, Ticker


async def list_tickers(
    db: AsyncSession,
    sector: str | None,
    exchange: str | None,
    is_active: bool,
    limit: int,
    offset: int,
) -> tuple[int, list[Ticker]]:
    stmt = select(DimTicker).where(DimTicker.is_active == is_active)
    if sector:
        stmt = stmt.where(DimTicker.sector == sector)
    if exchange:
        stmt = stmt.where(DimTicker.exchange == exchange)

    count_stmt = select(func.count()).select_from(stmt.subquery())
    total = (await db.execute(count_stmt)).scalar_one()

    stmt = stmt.offset(offset).limit(limit).order_by(DimTicker.symbol)
    rows = (await db.execute(stmt)).scalars().all()
    return total, [Ticker.model_validate(r) for r in rows]


async def get_ticker(db: AsyncSession, symbol: str) -> Ticker | None:
    stmt = select(DimTicker).where(DimTicker.symbol == symbol.upper())
    row = (await db.execute(stmt)).scalar_one_or_none()
    return Ticker.model_validate(row) if row else None


async def get_prices(
    db: AsyncSession,
    symbol: str,
    from_date: date,
    to_date: date | None,
) -> list[OHLCVBar] | None:
    ticker_stmt = select(DimTicker).where(DimTicker.symbol == symbol.upper())
    ticker = (await db.execute(ticker_stmt)).scalar_one_or_none()
    if not ticker:
        return None

    stmt = (
        select(DimDate.trade_date, FactDailyPrices)
        .join(DimDate, FactDailyPrices.date_id == DimDate.date_id)
        .where(FactDailyPrices.ticker_id == ticker.ticker_id)
        .where(DimDate.trade_date >= from_date)
    )
    if to_date:
        stmt = stmt.where(DimDate.trade_date <= to_date)
    stmt = stmt.order_by(DimDate.trade_date)

    rows = (await db.execute(stmt)).all()
    return [
        OHLCVBar(
            trade_date=r.trade_date,
            open=float(r.FactDailyPrices.open),
            high=float(r.FactDailyPrices.high),
            low=float(r.FactDailyPrices.low),
            close=float(r.FactDailyPrices.close),
            adj_close=float(r.FactDailyPrices.adj_close),
            volume=r.FactDailyPrices.volume,
            vwap=float(r.FactDailyPrices.vwap) if r.FactDailyPrices.vwap else None,
        )
        for r in rows
    ]


async def get_technical(
    db: AsyncSession,
    symbol: str,
    from_date: date | None,
    to_date: date | None,
) -> list[TechnicalFeatures] | None:
    ticker_stmt = select(DimTicker).where(DimTicker.symbol == symbol.upper())
    ticker = (await db.execute(ticker_stmt)).scalar_one_or_none()
    if not ticker:
        return None

    stmt = (
        select(DimDate.trade_date, FeatTechnical)
        .join(DimDate, FeatTechnical.date_id == DimDate.date_id)
        .where(FeatTechnical.ticker_id == ticker.ticker_id)
    )
    if from_date:
        stmt = stmt.where(DimDate.trade_date >= from_date)
    if to_date:
        stmt = stmt.where(DimDate.trade_date <= to_date)
    stmt = stmt.order_by(DimDate.trade_date)

    rows = (await db.execute(stmt)).all()
    return [
        TechnicalFeatures(
            trade_date=r.trade_date,
            sma_5=_f(r.FeatTechnical.sma_5),
            sma_10=_f(r.FeatTechnical.sma_10),
            sma_20=_f(r.FeatTechnical.sma_20),
            sma_50=_f(r.FeatTechnical.sma_50),
            ema_12=_f(r.FeatTechnical.ema_12),
            ema_26=_f(r.FeatTechnical.ema_26),
            rsi_14=_f(r.FeatTechnical.rsi_14),
            macd=_f(r.FeatTechnical.macd),
            macd_signal=_f(r.FeatTechnical.macd_signal),
            macd_hist=_f(r.FeatTechnical.macd_hist),
            boll_upper=_f(r.FeatTechnical.boll_upper),
            boll_lower=_f(r.FeatTechnical.boll_lower),
            boll_mid=_f(r.FeatTechnical.boll_mid),
            atr_14=_f(r.FeatTechnical.atr_14),
            obv=r.FeatTechnical.obv,
            vwap=_f(r.FeatTechnical.vwap),
        )
        for r in rows
    ]


def _f(v) -> float | None:
    return float(v) if v is not None else None
