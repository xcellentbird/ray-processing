from pathlib import Path

import ray

from attr_tagging.tagger import AttrTagProcess
from wat_extractor.extractor import WatExtractProcess, download_wat_paths_file

DATA_ROOT = Path('/mnt', 'az-files', 'data')
WAT_PATHS_PATH = Path(DATA_ROOT, 'wat.paths')

if __name__ == '__main__':
    if not ray.is_initialized():
        ray.init()

    download_wat_paths_file(WAT_PATHS_PATH)

    extracted_path = Path(DATA_ROOT, 'extracted.parquet')
    WatExtractProcess(WAT_PATHS_PATH).execute(extracted_path, num_wat_paths=256)

    tagged_path = Path(DATA_ROOT, 'tagged.parquet')
    AttrTagProcess(extracted_path).execute(tagged_path)
