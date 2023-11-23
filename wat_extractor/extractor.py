import os
import json
import logging
import asyncio
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import ray
from funcy import chunks
from warcio.archiveiterator import ArchiveIterator

from wat_extractor.filters import is_in_alt, is_img_src, is_valid_url
from wat_extractor.schema import ImageAltData
from wat_extractor.utils import S3ObjectLoader

BUCKET_NAME = 'commoncrawl'
REGION_NAME = 'us-east-1'


class WatExtractProcess:
    def __init__(self, wat_paths_path, s3_bucket_name=BUCKET_NAME, s3_region_name=REGION_NAME):
        self.wat_paths_path = wat_paths_path
        self.s3_loader = S3ObjectLoader(s3_bucket_name, s3_region_name)

    @property
    def filters(self):
        return [
            is_in_alt,
            is_img_src,
            is_valid_url
        ]

    def filter_link(self, link):
        for _filter in self.filters:
            if not _filter(link):
                return True

        return False

    def extract_link_from_wat(self, wat_file) -> dict:
        for i, wat_record in enumerate(ArchiveIterator(wat_file)):
            if i == 0:
                continue

            str_wat_content = wat_record.content_stream().read().decode('utf-8')
            wat_content = json.loads(str_wat_content)

            if response_metadata := wat_content['Envelope']['Payload-Metadata'].get('HTTP-Response-Metadata'):
                if html_metadata := response_metadata.get('HTML-Metadata'):
                    if links := html_metadata.get('Links'):
                        for link in links:
                            yield link

    @ray.remote
    def filter_batch(self, links):
        filtered_datas = []
        for link in links:
            if not self.filter_link(link):
                filtered_datas.append({'image_url': link['url'], 'alt_text': link['alt']})

        return filtered_datas

    @ray.remote
    def extract_and_save_data(self, wat_path, parquet_path, chunk_size=10000):
        Path(parquet_path).parent.mkdir(parents=True, exist_ok=True)

        logging.info(f'Extracting data from {wat_path} and saving to {parquet_path}')
        schema = ImageAltData.arrow_schema()

        with pq.ParquetWriter(parquet_path, schema=schema) as writer:
            wat_file = self.s3_loader.load(object_name=wat_path)

            obj_refs = []
            for link_chunk in chunks(chunk_size, self.extract_link_from_wat(wat_file)):
                obj_refs.append(self.filter_batch.remote(self, link_chunk))

            while obj_refs:
                ready_obj_refs, obj_refs = ray.wait(obj_refs, num_returns=1)

                filtered_data_chunk = ray.get(ready_obj_refs[0])
                table = pa.Table.from_pylist(filtered_data_chunk, schema=schema)

                writer.write_table(table)

    def iter_wat_paths(self, num_wat_paths):
        with open(self.wat_paths_path, 'r') as f:
            for line in f.readlines()[:num_wat_paths]:
                yield line.strip()

    def execute(self, save_root, num_wat_paths=8):
        obj_refs = []
        for wat_path in self.iter_wat_paths(num_wat_paths):
            parquet_filename = wat_path.replace(os.sep, '_').replace('.warc.wat.gz', '.parquet')
            parquet_path = save_root / parquet_filename

            obj_refs.append(
                self.extract_and_save_data.remote(
                    self,
                    wat_path=wat_path,
                    parquet_path=parquet_path,
                )
            )

        ray.get(obj_refs)


def download_wat_paths_file(wat_paths_path):
    download_url = 'https://data.commoncrawl.org/crawl-data/CC-MAIN-2023-14/wat.paths.gz'
    save_path = Path(f'{wat_paths_path}.gz')

    if not save_path.exists():
        os.system(f'wget {download_url} -O {save_path}')
        os.system(f'gunzip {save_path}')
