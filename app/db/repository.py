from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import func

from app.db.models import TradeResult


class TradeResultRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_record_count(self) -> int:
        result = await self.session.execute(select(func.count(TradeResult.id)))
        count = result.scalar_one()
        return count

    async def get_max_volume_trade(self) -> TradeResult:
        result = await self.session.execute(
            select(TradeResult).order_by(TradeResult.volume.desc()).limit(1)
        )
        max_volume_trade = result.scalar_one()
        return max_volume_trade

    async def get_unique_exchange_product_ids(self) -> list[str]:
        result = await self.session.execute(
            select(TradeResult.exchange_product_id).distinct()
        )
        unique_ids = [row[0] for row in result.fetchall()]
        return unique_ids
