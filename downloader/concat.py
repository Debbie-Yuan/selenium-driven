import logging
import os
import pathlib
import pickle
import shutil
import sys
from typing import List, Tuple

import tqdm

from .static import DEFAULT_PARTS_LIST_FILE_NAME, DEFAULT_DISTRIBUTED_DOWNLOADED_TARFILE
from .rangespec import UNIT


logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


def precheck_missing_block(
        files: List[pathlib.Path]
) -> Tuple[int, List[pathlib.Path]]:
    # TODO Have to remember the full size of the original file.
    # Currently consider the last block as healthy.
    missing_size = 0
    missing_blocks = []
    for file in files[0: -1]:
        size = os.path.getsize(file)
        if size != UNIT:
            missing_blocks.append(file)
            missing_size += UNIT
    return missing_size, missing_blocks


def missing_handler(total: List[pathlib.Path], missing: List[pathlib.Path]):
    # Create a folder
    _f = total[0].parent
    _f_name = total[0].name.rsplit('@', maxsplit=1)[0]
    temp_dir_path = _f / "_temp_concat"
    logging.debug(f"Temp folder created @ {temp_dir_path}")
    try:
        os.mkdir(temp_dir_path)
    except Exception as be:
        logging.exception("Can't create a temp folder called _temp_concat.", exc_info=be)
        exit(1)

    # Processing
    qs = {str(f.name) for f in missing}
    missing_bytes_range = []
    for file in total:
        if file.name in qs:
            missing_part = file.name.split("@bytes=")[-1]
            missing_bytes_range.append(missing_part)
            logging.info(f"Found a missing part, ranging with {missing_part}.")
            os.remove(file)
            logging.info(f"Missing part {missing_part} deleted.")
        else:
            shutil.move(file, temp_dir_path)
            logging.info(f"Healthy part {file.name} moved to the temp dir.")

    # Save the targets as a pickle file.
    with open(temp_dir_path / (_f_name + DEFAULT_PARTS_LIST_FILE_NAME), "wb") as mpf:
        pickle.dump(missing_bytes_range, mpf)

    fn = _f_name + DEFAULT_DISTRIBUTED_DOWNLOADED_TARFILE
    tarball_cmd = f"tar -zcvf {fn} {str(temp_dir_path.absolute())}"
    logging.info(f"[MissingHandler] Archiving fragments using cmd: {tarball_cmd!r}")
    os.system(tarball_cmd)


def concat(path):
    if not os.path.isdir(path):
        raise FileNotFoundError(path)
    p = pathlib.Path(path)
    files = [file for file in p.glob("*.*") if "@bytes" in file.name]
    ms, mbs = precheck_missing_block(files)
    if ms > 0:
        logging.warning(f"Currently we found {ms} bytes of missing, collecting peaceful ones.")
        missing_handler(files, mbs)
        logging.info(f"Successfully created dparts info, exiting.")
        exit(0)

    # @bytes=119537665-125829120
    files.sort(key=lambda x: int(str(x.name).rsplit("-", maxsplit=1)[-1]))
    real_name = files[0].name.rsplit("@", maxsplit=1)[0]
    final_path = p / real_name
    with open(final_path, "wb") as fp:
        for file in tqdm.tqdm(files):
            with open(file, "rb") as _tf:
                fp.write(_tf.read())


if __name__ == '__main__':
    concat(sys.argv[-1])
