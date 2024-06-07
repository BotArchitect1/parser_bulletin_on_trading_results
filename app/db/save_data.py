from app.db.database import async_session_maker
from app.db.models import TradeResult


async def save_data_to_db(data):
    print("Saving data to database...")
    async with async_session_maker() as session:
        async with session.begin():
            try:
                for item in data:
                    trade_result = TradeResult(
                        exchange_product_id=item["exchange_product_id"],
                        exchange_product_name=item["exchange_product_name"],
                        delivery_basis_name=item["delivery_basis_name"],
                        volume=item["volume"],
                        total=item["total"],
                        count=item["count"],
                        date=item["date"],
                    )
                    session.add(trade_result)
                await session.commit()
            except Exception as e:
                await session.rollback()
                raise e
