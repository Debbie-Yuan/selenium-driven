import logging
import os
import pathlib
import pickle
import shutil
import sys
import threading
from typing import List, Tuple

import tqdm

from .static import DEFAULT_PARTS_LIST_FILE_NAME, DEFAULT_DISTRIBUTED_DOWNLOADED_TARFILE, Meta
from .rangespec import UNIT, DParts

_LOCAL = threading.local()


def precheck_missing_block(
        path: pathlib.Path,
        files: List[pathlib.Path]
) -> Tuple[int, List[pathlib.Path], List[str]]:
    # Currently consider the last block as healthy.

    missing_size = 0
    missing_blocks = []
    cursor_seq = []
    for file in files[0: -1]:
        # Missing handler for non-existent fragments
        # Despite the size, we always keep the filename.
        slash_format_string = file.name.split("@bytes=")[-1]
        cursor_seq.extend(map(int, slash_format_string.split('-')))

        size = os.path.getsize(file)
        if size != UNIT:
            missing_blocks.append(file)
            missing_size += UNIT

    # Meta
    # Un-download parts
    try:
        meta = Meta.load(path)
    except FileNotFoundError as fnfe:
        if _LOCAL.__getattribute__("without_meta"):
            logging.info("Ignoring meta file inexistency.")
            un_download = []
        else:
            logging.exception(f"Cannot find meta file from your directory, "
                              f"maybe you could try with --without_meta. Excption = {str(fnfe)}")
            exit(-1)
    else:
        cursor_seq.sort()
        un_download = []
        if cursor_seq[-1] != meta.content_length:
            un_download.append(f"{cursor_seq[-1]}-{meta.content_length}")
        if cursor_seq[0] != 0:
            missing_blocks.append(f"{0}-{cursor_seq[0]}")

        last_value = None
        for idx in range(0, len(cursor_seq) - 1, 2):
            if last_value is None:
                last_value = cursor_seq[idx + 1]
                continue
            if last_value + 1 != cursor_seq[idx]:
                un_download.append(f"{last_value + 1}-{cursor_seq[idx] - 1}")
            last_value = cursor_seq[idx + 1]

    return missing_size, missing_blocks, un_download


# Missing handler for existed fragment but unhealthy
def missing_handler_existed(total: List[pathlib.Path], missing: List[pathlib.Path], ud: List[str] = None):
    # Create a folder
    if len(missing) == 0 and ud is None:
        return

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

    # UD
    if ud:
        missing_bytes_range.extend(ud)

    # Save the targets as a pickle file.
    with open(temp_dir_path / (_f_name + DEFAULT_PARTS_LIST_FILE_NAME), "wb") as mpf:
        pickle.dump(missing_bytes_range, mpf)

    fn = _f_name + DEFAULT_DISTRIBUTED_DOWNLOADED_TARFILE
    tarball_cmd = f"tar -zcvf {fn} {str(temp_dir_path.absolute())}"
    logging.info(f"[MissingHandler] Archiving fragments using cmd: {tarball_cmd!r}")
    os.system(tarball_cmd)


def concat(path, **kwargs):
    # threading.local
    for k, v in kwargs.items():
        _LOCAL.__setattr__(k, v)

    if not os.path.isdir(path):
        raise FileNotFoundError(path)
    p = pathlib.Path(path)
    files = [file for file in p.glob("*.*") if "@bytes" in file.name]
    if files.__len__() <= 0:
        logging.info(f"Noting to do with path : {path!r}")
        return

    if kwargs.get("export"):
        # d = DParts(path)
        # parts = d.as_list()
        # ms, mbs, ud = precheck_missing_block(path, files)
        files.sort(key=lambda x: int(str(x.name).rsplit("-", maxsplit=1)[-1]))
        #
        try:
            meta = Meta.load(path)
            length = meta.content_length
        except Exception:  # noqa
            logging.exception(f"Broken, you cannot use this function without meta.")
            exit(-1)

        slices = []
        for file in files:
            a, b = file.name.rsplit("@bytes=", maxsplit=1)[-1].split("-")
            slices.append(int(a))
            slices.append(int(b))

        un_download = []
        if slices[0] != 0:
            un_download.append(f"{0}-{slices[0]}")
            logging.info(f"\033[31m[N]\033[0m  PART {0}-{slices[0]}")
        last_value = None
        for idx in range(0, len(slices) - 1, 2):
            logging.info(f"\033[34m[Y]\033[0m  PART {slices[idx]}-{slices[idx + 1]}")
            if last_value is None:
                last_value = slices[idx + 1]
                continue
            if last_value + 1 != slices[idx]:
                un_download.append(f"{last_value + 1}-{slices[idx] - 1}")
                logging.info(f"\033[31m[N]\033[0m  PART {last_value + 1}-{slices[idx] - 1}")
            last_value = slices[idx + 1]

        if slices[-1] != length:
            un_download.append(f"{slices[-1]}-{meta.content_length}")
        exit(0)

    if not kwargs.get("force"):
        ms, mbs, ud = precheck_missing_block(path, files)
        if ms > 0 or len(ud) > 0:
            if ms > 0:
                logging.warning(f"Currently we found {ms} bytes of missing, collecting peaceful ones.")
            if len(ud) > 0:
                logging.warning(f"Plus we found {len(ud)} parts of un-downloaded file. UD = {ud}")
            missing_handler_existed(files, mbs, ud)
            logging.info(f"Successfully created dparts info, exiting.")
            exit(1)

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
