import asyncio

from sqlalchemy.ext.asyncio import AsyncEngine

from app.parser.scrapping import scrape_reports
from app.db.database import Base, async_session_maker, engine
from app.db.repository import TradeResultRepository
from app.db.save_data import save_data_to_db
from app.db.models import TradeResult


async def create_tables(engine_db: AsyncEngine):
    async with engine_db.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def main():
    print("Starting main process...")
    report_data = await scrape_reports()
    await save_data_to_db(report_data)
    print("Main process finished.")

    async with async_session_maker() as session:
        repository = TradeResultRepository(session)

        record_count = await repository.get_record_count()
        print(f"Total records: {record_count}")

        max_volume_trade = await repository.get_max_volume_trade()
        print(f"Max volume trade: {max_volume_trade}")

        unique_exchange_product_ids = await repository.get_unique_exchange_product_ids()
        print(f"Unique exchange product IDs: {unique_exchange_product_ids}")


if __name__ == "__main__":

    async def run():

        await create_tables(engine)
        await main()

    asyncio.run(run())
