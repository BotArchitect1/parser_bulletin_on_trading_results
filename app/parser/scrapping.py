import aiohttp
import asyncio
from bs4 import BeautifulSoup
from datetime import datetime

from app.utils.extract_xml import extract_report_data

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
            report_links.append(BASE_URL + link.get('href'))
        date_elem = item.select_one('.accordeon-inner__item-inner__title span')
        if date_elem:
            date_str = date_elem.text.strip()
            dates.append(datetime.strptime(date_str, '%d.%m.%Y').date())
    return list(zip(report_links, dates))


async def download_report(session, url, date):
    print(f"Downloading report for date: {date}")
    async with session.get(url) as response:
        content = await response.read()
        return content, date


async def scrape_reports(start_page=1, end_page=5):
    print("Starting report scraping...")
    tasks = []
    async with aiohttp.ClientSession() as session:
        for page in range(start_page, end_page + 1):
            page_url = f"{BASE_URL}?page=page-{page}"
            page_html = await fetch_page(session, page_url)
            report_links_and_dates = await get_report_links(page_html)
            for link, date in report_links_and_dates:
                if date.year >= 2023:
                    task = asyncio.create_task(download_report(session, link, date))
                    tasks.append(task)
        report_data = []
        print("Extracting report data...")
        for task in asyncio.as_completed(tasks):
            content, date = await task
            report_data.extend(await extract_report_data(content, date))
    print("Report scraping finished.")
    return report_data

