from datetime import datetime
from typing import List, Dict
import io
import pandas as pd
import openpyxl
from openpyxl.reader.excel import load_workbook

from app.db.models import TradeResult
from app.db.database import async_session_maker

import xlrd


def extract_report_data(file_path):
    workbook = xlrd.open_workbook(file_path)
    sheet = workbook.sheet_by_index(0)

    data = []
    found_table = False
    headers = {}

    for row_idx in range(sheet.nrows):
        row = sheet.row_values(row_idx)
        if ''.join(row).strip() == 'Единица измерения: Метрическая тонна':
            found_table = True
        elif found_table:
            if any(row):
                if not headers:
                    headers = {header.lower().replace('\n', ' '): idx for idx, header in enumerate(row)}
                else:
                    exchange_product_id = row[headers['код инструмента']]
                    exchange_product_name = row[headers['наименование инструмента']]
                    delivery_basis_name = row[headers['базис поставки']]
                    volume = row[headers['объем договоров в единицах измерения']]
                    total = row[headers['обьем договоров, руб.']]
                    count = row[headers['количество договоров, шт.']]

                    if count > '0' and exchange_product_id not in ('Итого:', 'Итого по секции:'):
                        data.append({
                            'exchange_product_id': exchange_product_id,
                            'exchange_product_name': exchange_product_name,
                            'delivery_basis_name': delivery_basis_name,
                            'volume': volume,
                            'total': total,
                            'count': count
                        })
    print(data)
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
