import os


async def download_file(session, url, date, folder="downloads"):
    os.makedirs(folder, exist_ok=True)
    filename = f"{folder}/{date}.xls"
    print(f"Downloading file: {filename} from {url}")
    async with session.get(url) as response:
        if response.status == 200:
            content = await response.read()
            with open(filename, "wb") as f:
                f.write(content)
            print(f"Successfully downloaded {filename}")
        else:
            print(f"Failed to download {url}: HTTP {response.status}")
