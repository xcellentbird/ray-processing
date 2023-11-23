from pathlib import Path

import ray

from wat_extractor.extractor import WatExtractProcess, download_wat_paths_file
from attr_tagging.tagger import AttrTagProcess

DATA_ROOT = Path('data')
WAT_PATHS_PATH = Path(DATA_ROOT, 'wat.paths')  # '/mnt/az-files/wat.paths'

if __name__ == '__main__':
    if not ray.is_initialized():
        ray.init()

    download_wat_paths_file(WAT_PATHS_PATH)

    extracted_path = Path(DATA_ROOT, 'extracted.parquet')  # Path('/mnt', 'az-files', 'data', 'extracted')
    WatExtractProcess(WAT_PATHS_PATH).execute(extracted_path)

    tagged_path = Path(DATA_ROOT, 'tagged.parquet')  # Path('/mnt', 'az-files', 'data', 'tagged')
    AttrTagProcess(extracted_path).execute(tagged_path)
