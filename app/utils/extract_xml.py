import xlrd


def extract_report_data(file_path):
    workbook = xlrd.open_workbook(file_path)
    sheet = workbook.sheet_by_index(0)

    data = []
    found_table = False
    headers = {}

    for row_idx in range(sheet.nrows):
        row = sheet.row_values(row_idx)
        if "".join(row).strip() == "Единица измерения: Метрическая тонна":
            found_table = True
        elif found_table:
            if any(row):
                if not headers:
                    headers = {
                        header.lower().replace("\n", " "): idx
                        for idx, header in enumerate(row)
                    }
                else:
                    exchange_product_id = row[headers["код инструмента"]]
                    exchange_product_name = row[headers["наименование инструмента"]]
                    delivery_basis_name = row[headers["базис поставки"]]
                    volume_value = row[headers["объем договоров в единицах измерения"]]
                    volume = (
                        float(volume_value)
                        if volume_value != "-" and volume_value != ""
                        else 0.0
                    )
                    total_value = row[headers["обьем договоров, руб."]]
                    total = (
                        float(total_value)
                        if total_value != "-" and total_value != ""
                        else 0.0
                    )
                    count_value = row[headers["количество договоров, шт."]]
                    count = (
                        int(count_value)
                        if count_value != "" and count_value != "-"
                        else 0
                    )

                    if count > 0 and exchange_product_id not in (
                        "Итого:",
                        "Итого по секции:",
                    ):
                        data.append(
                            {
                                "exchange_product_id": exchange_product_id,
                                "exchange_product_name": exchange_product_name,
                                "delivery_basis_name": delivery_basis_name,
                                "volume": volume,
                                "total": total,
                                "count": count,
                            }
                        )
    return data
