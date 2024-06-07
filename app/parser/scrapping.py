import aiohttp
import asyncio
from datetime import datetime

from app.parser.download_files import download_file
from app.parser.get_links import fetch_page, get_report_links
from app.utils.extract_xml import extract_report_data

BASE_URL = "https://spimex.com/markets/oil_products/trades/results/"


async def scrape_reports(start_page=1, end_page=365):
    print("Starting report scraping...")

    tasks = []
    all_data = []
    link_date_map = {}

    async with aiohttp.ClientSession() as session:
        for page in range(start_page, end_page + 1):
            page_url = f"{BASE_URL}?page=page-{page}"
            page_html = await fetch_page(session, page_url)
            report_links_and_dates = await get_report_links(page_html)
            for link, date in report_links_and_dates:
                if date.year >= 2023:
                    link_date_map[link] = date
                    task = asyncio.create_task(download_file(session, link, date))
                    tasks.append(task)

        await asyncio.gather(*tasks)
        print("Extracting report data...")
        for link, date in link_date_map.items():
            file_path = f"downloads/{date}.xls"
            report_data = extract_report_data(file_path)
            for item in report_data:
                item["date"] = date
                item["created_on"] = datetime.now()
                item["updated_on"] = datetime.now()
            all_data.extend(report_data)

    return all_data
