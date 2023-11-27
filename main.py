from pathlib import Path

import ray

from attr_tagging.tagger import AttrTagProcess
from wat_extractor.extractor import WatExtractProcess, download_wat_paths_file
from config import IN_KUBERNETES


DATA_ROOT = Path('/mnt', 'az-files', 'data') if IN_KUBERNETES else Path('data')
WAT_PATHS_PATH = Path(DATA_ROOT, 'wat.paths')

if __name__ == '__main__':
    if not ray.is_initialized():
        ray.init()

    DATA_ROOT.mkdir(parents=True, exist_ok=True)

    download_wat_paths_file(WAT_PATHS_PATH)

    extracted_path = Path(DATA_ROOT, 'extracted.parquet')
    WatExtractProcess(WAT_PATHS_PATH).execute(extracted_path, num_wat_paths=256)

    tagged_path = Path(DATA_ROOT, 'tagged.parquet')
    AttrTagProcess().execute(extracted_path, tagged_path)
