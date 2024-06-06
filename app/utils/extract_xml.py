from datetime import datetime
from typing import List, Dict
import io

import openpyxl

from app.db.models import TradeResult
from app.db.database import async_session_maker


async def extract_report_data(content: bytes, date: datetime.date) -> List[Dict]:
    print(f"Extracting report data for date: {date}")

    if not content.startswith(b"\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1"):
        print(f"Skipping file for date {date}: File is not in exel format")
        return []

    workbook = openpyxl.load_workbook(io.BytesIO(content), data_only=True)
    worksheet = workbook.active

    data = []
    for row in worksheet.iter_rows(min_row=7, values_only=True):
        if row[-1] > 0:  # Если количество договоров больше 0
            data.append({
                'exchange_product_id': row[0],
                'exchange_product_name': row[1],
                'delivery_basis_name': row[2],
                'volume': row[3],
                'total': row[4],
                'count': row[-1],
                'date': date
            })

    return data


async def save_data_to_db(data):
    print("Saving data to database...")
    async with async_session_maker() as session:
        async with session.begin():
            try:
                for item in data:
                    trade_result = TradeResult(
                        exchange_product_id=item['exchange_product_id'],
                        exchange_product_name=item['exchange_product_name'],
                        delivery_basis_name=item['delivery_basis_name'],
                        volume=item['volume'],
                        total=item['total'],
                        count=item['count'],
                        date=item['date']
                    )
                    session.add(trade_result)
                await session.commit()
            except Exception as e:
                await session.rollback()
                raise e