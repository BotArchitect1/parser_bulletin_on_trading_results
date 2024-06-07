import os

import aiohttp
import asyncio
from bs4 import BeautifulSoup
from datetime import datetime

from app.utils.extract_xml import extract_report_data, save_data_to_db

BASE_URL = 'https://spimex.com/markets/oil_products/trades/results/'


async def fetch_page(session, url):
    print(f"Fetching page: {url}")
    async with session.get(url) as response:
        return await response.text()


async def get_report_links(page_html):
    print("Extracting report links and dates...")
    soup = BeautifulSoup(page_html, 'html.parser')
    report_links = []
    dates = []
    for item in soup.select('.accordeon-inner__wrap-item'):
        link = item.select_one('.accordeon-inner__item-title.link.xls')
        if link:
            report_links.append("https://spimex.com" + link.get('href'))
        date_elem = item.select_one('.accordeon-inner__item-inner__title span')
        if date_elem:
            date_str = date_elem.text.strip()
            dates.append(datetime.strptime(date_str, '%d.%m.%Y').date())
    return list(zip(report_links, dates))


async def download_file(session, url, date, folder='downloads'):
    os.makedirs(folder, exist_ok=True)
    filename = f"{folder}/{date}.xls"
    print(f"Downloading file: {filename} from {url}")
    async with session.get(url) as response:
        if response.status == 200:
            content = await response.read()
            with open(filename, 'wb') as f:
                f.write(content)
            print(f"Successfully downloaded {filename}")
        else:
            print(f"Failed to download {url}: HTTP {response.status}")


async def scrape_reports(start_page=1, end_page=2):
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
                item['date'] = date
                item['created_on'] = datetime.now()
                item['updated_on'] = datetime.now()
            all_data.extend(report_data)

    return all_data
