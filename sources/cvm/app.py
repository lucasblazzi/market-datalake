import os
import json
import asyncio
from io import BytesIO
import re

from crawler import Crawler
from builder import Builder
from aws.s3 import S3


proxy_source = "https://www.sslproxies.org/"
base_path = os.path.dirname(os.path.abspath(__file__))
BUCKET_NAME = os.environ.get("BUCKET", "ira-raw-data-market")



class Loader:
    def __init__(self, schema, args):
        self.schema = json.load(open(os.path.join(base_path, "plans/catalog.json"), encoding="utf8")).get(schema)

    @staticmethod
    def quarterly_results_path_map(name):
        itr = re.sub("[^A-Z]", "", name)
        return f"{itr}/{name}"

    @staticmethod
    def simple_path_map(name):
        return name

    @staticmethod
    def year_path_map(name):
        return name

    def prepare_result(self, result):
        res = list()
        for df in result:
            path = f"{self.schema['path']}/{getattr(self, self.schema['map_path'])(df.index.name)}"
            s3_path = f"s3://{BUCKET_NAME}/cvm/{path}.csv"
            buffer = BytesIO()
            df.to_csv(buffer, index=False)
            res.append((buffer, s3_path))
        return res

    @staticmethod
    def build_chunks(slots):
        chunk_size = 5
        return [slots[i:i + chunk_size] for i in range(0, len(slots), chunk_size)]

    @staticmethod
    async def save_data(chunks):
        for chunk in chunks:
            async with S3() as s3:
                tasks = list()
                for buffer, s3_path in chunk:
                    print(f"[RUNNING] TASK - {s3_path}")
                    tasks.append(s3.insert_file(buffer.getvalue(), s3_path))
                await asyncio.gather(*tasks)

    async def load(self, args):
        function = f"{self.schema['process']}_loader"
        print(f"[STARTING] {function}")
        result = await getattr(self, function)(args)
        print(f"[SUCCESS] RESULTS PROCESSED")
        print(f"[RUNNING] PREPARING RESULTS")
        slots = self.prepare_result(result)
        print(f"[RUNNING] SAVING RESULTS")
        chunks = self.build_chunks(slots)
        await self.save_data(chunks)
        print(f"[SUCCESS] ALL FILES SAVED")

    async def full_loader(self, args):
        data = Crawler(proxy_source=proxy_source, url=self.schema["url"]).zip_crawl()
        return data

    async def simple_loader(self, args):
        data = Crawler(proxy_source=proxy_source, url=self.schema["url"]).crawl()
        # results = Builder(data).architect
        return data

    async def yearly_loader(self, args):
        url = f"{self.schema['url']}{args['year']}.zip"
        data = Crawler(proxy_source=proxy_source, url=url).crawl()
        # results = Builder(data).architect
        return data

    async def monthly_loader(self, year, month):
        url = f"{self.schema['url']}{year}{month}.csv"
        data = Crawler(proxy_source=proxy_source, url=url)
        # results = Builder(data).architect.crawl()
        return data


async def handler(event):
    results = await Loader(event["schema"], {}).load(event["args"])


def lambda_handler(event, context):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(handler(event))
    loop.close()


event = {
    "schema": "traded-companies-quarterly-results",
    "args": {
        "year": None
    }
}

# for year in range(2012, 2022):
#     event["year"] = year
lambda_handler(event, "")
