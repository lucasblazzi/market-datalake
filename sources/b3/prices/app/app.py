import os
import json
import asyncio
from io import BytesIO
import pandas as pd

from cryptography.fernet import Fernet

from utils.crawler import Crawler
from utils.builder import Builder
from aws.s3 import S3


daily_field_length = [2, 8, 2, 12, 3, 12, 10, 3, 4, 13, 13, 13, 13, 13, 13, 13, 5, 18, 18, 13, 1, 8, 7, 13, 12, 3]

# https://www.b3.com.br/data/files/C8/F3/08/B4/297BE410F816C9E492D828A8/SeriesHistoricas_Layout.pdf
# https://www.b3.com.br/data/files/4F/91/A8/CD/2A280710E7BCA507DC0D8AA8/TradeIntradayFile.pdf

daily_data = b"gAAAAABhvn3zDitlQLagTDCxbMMFHStNqnxCnAq_CS37Yqnm1KADWdvb_BM3iLaD12mykvVHrwR9PqECM8JZDCMioC-UXWFu" \
             b"htXFtUXa41m6-PuyRGDl8jHk7EE3i6VSOJ7Sn4ewZBjdz9VpltLvrA1kfg0kR9I9yg=="


BUCKET_NAME = os.environ.get("BUCKET", "ira-market-raw-data")


def decrypt_url(url):
    f = Fernet(b'zBD0XJb_uD0Qn3tDjtt-Zk1MGSh2ZRlg6nTMXkcdH9U=')
    return f.decrypt(url).decode()


async def save_data(df):
    tasks = list()
    async with S3() as s3:
        buffer = BytesIO()
        df = df.sort_values("date")
        df.to_csv(buffer, index=False)
        tasks.append(s3.insert_file(buffer.getvalue(), f"s3://{BUCKET_NAME}/b3/prices/{year}.csv"))
        await asyncio.gather(*tasks)
    return register


async def loader(event):
    raw_results = list()
    for year in range(event["start"], event["end"] + 1):
        print(f"LOADING {year}")
        url = f"{decrypt_url(daily_data)}{year}.ZIP"
        crawler = Crawler(url=url, txt_field_length=daily_field_length)
        raw_results.extend(crawler.zip_crawl())
        results = Builder(dfs=raw_results, year=year).architect
        await save_data(results)
        print(f"[SUCCESSFULL] {year}")


def lambda_handler(event, context):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(loader(event))
    loop.close()


lambda_handler({"start": 2021, "end": 2022}, "")
