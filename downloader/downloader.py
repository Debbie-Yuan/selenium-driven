import io
import json
import pathlib
import pickle
import time
import logging
from typing import Optional

import requests
import os
import queue

from .rangespec import RangeSlicer, DParts
from .static import REPORT_FREQUENCY, NS, CHUNK_SIZE, SLICING, DEFAULT_META_FILE_NAME

rs = RangeSlicer()
LAST_REPORT_TIME = None
Bytes = 0
Time = 0  # s


def report(bytes_amt, time_ns):
    global LAST_REPORT_TIME, Bytes, Time

    if LAST_REPORT_TIME is None:
        LAST_REPORT_TIME = time.time_ns()
    # logging.debug(f"[REPORT] Before add, Bytes = {Bytes},
    # Time = {Time}, bytes_amt = {bytes_amt}, timens = {time_ns}")
    ct = time.time_ns()
    if (time.time_ns() - LAST_REPORT_TIME) < REPORT_FREQUENCY:
        Bytes += bytes_amt
        Time += time_ns
        return

    logging.debug(f"[REPORT] Current speed : {Bytes / Time * NS / 1000 :.2f} KBytes/s")
    # logging.debug(f"[REPORT] bytes_amt = {bytes_amt}, time_ns = {time_ns},
    # Current speed : {bytes_amt / time_ns * NS / 1000} KBytes/s")
    # Clear and reset the cursor.
    LAST_REPORT_TIME = ct
    Bytes = 0
    Time = 0


def clear_report():
    global LAST_REPORT_TIME
    LAST_REPORT_TIME = None


def _download(
        url: str,
        name: str,
        s: requests.Session,
        headers: dict = None,
        data=None
):
    # Timeout = UNIT bytes // 5 kbps * 1024 bytes + 1
    try:
        start_req = time.time_ns()
        resp = s.get(url=url, headers=headers, data=data,
                     timeout=1229, verify=False, stream=True)

        downloaded_bytes = 0
        # Using an IO Buffer for speeding up caching.
        with io.BytesIO() as buffer:
            total_length = resp.headers.get('content-length')
            if total_length is None:
                buffer.write(resp.content)
            else:
                for chunk in resp.iter_content(CHUNK_SIZE):
                    buffer.write(chunk)

                    # Audit - bytes
                    downloaded_bytes += len(chunk)
                    current_time = time.time_ns()
                    report(len(chunk), time.time_ns() - start_req)
                    start_req = current_time

            with open(name, "wb") as f:
                f.write(buffer.getvalue())

    except requests.exceptions.Timeout:
        logging.info(f"Timeout when downloading file, url = {url}, name = {name}.")
        return 1
    except IOError:
        logging.info(f"IOError when saving buffered content, url = {url}, name = {name}.")
        return 2
    except Exception as be:
        logging.info(f"Unhandled exception occurred, exception = {be}, url = {url}, name = {name}.")
        return 3
    else:
        return 0


def check_slices(slices):
    if slices is None:
        raise Exception("Headed request failed.")


def name_handler(path, name, range_info, url: str, with_path=True):
    # Name Order, User-Specified name > URL name
    # Path: Use when have
    name = name or url.rsplit('/', maxsplit=1)[-1]

    if path and os.path.isdir(path) and with_path:
        name = os.path.join(path, name)

    if range_info:
        name += f"@{range_info['Range']}"

    return name


def path_specify(path, name=None, suffix='failed'):
    if path and os.path.isdir(path):
        meta_info_name = os.path.join(path, f"{name}.{suffix}")
    else:
        meta_info_name = f"{name}.{suffix}"

    return meta_info_name


def save_meta(**kwargs):
    # Downloaded meta
    # Should include:
    #   Start time, the timestamp at this calling point
    cp = time.time()
    kwargs['start-time'] = cp
    # url
    # path
    # name
    # headers
    # data
    # content_length
    path = kwargs.get("path")
    if path and os.path.isdir(path):
        path = pathlib.Path(path)
    else:
        path = pathlib.Path(".")

    meta_file = path / DEFAULT_META_FILE_NAME
    with open(meta_file, "w") as meta:
        json.dump(kwargs, meta)
    logging.info(f"[Download] [Meta] Meta saved : {kwargs}.")


# Support for parts range guided download.
# A range guided download needs to pass in the list of range.
def download(
        url: str, path=None, name=None,
        headers=None, data=None, retry_timeout=3600,
        dparts: Optional[DParts] = None
):
    s = requests.session()
    headers = headers or {}
    raw_name = name_handler(path=path, name=name, range_info=None, url=url, with_path=False)

    # SLICING loggingIC
    # DParts has been configured:
    if dparts:
        slices = dparts.get_range_slices(url=url, session=s)
        logging.info(
            f"[Download] [DPART] Reading ranges form dparts, with length = {len(dparts)}, enabling direct slicing."
        )
        direct_slicing = True
        content_length, _ = rs.make_head_request(url, s)
    else:
        # With no DParts told.
        if SLICING:
            slices = rs.get_range_slices(url, s)
        else:
            slices = rs.get_range_slices(url, s, not_slicing=True)
        direct_slicing = False
        content_length = slices[-1]
    check_slices(slices)  #

    # Save download meta info
    save_meta(
        url=url, path=path, name=None, headers=None,
        data=None, content_length=content_length,
        dparts=True if dparts else False
    )

    checklist = {}  # dict typed
    # Fast Write Back FD
    fn = path_specify(path, name=raw_name, suffix="ok")
    if os.path.exists(fn):
        with open(fn, "rb") as _f:
            try:
                checklist = pickle.load(_f)
            except EOFError:
                logging.warning("[Download] [FWB] Failed to load checklist file, the file might be edited.")
    # Used to save simultaneously
    checklist_fast_write_fp = open(fn, "wb")
    pickle.dump(checklist, checklist_fast_write_fp)
    retrylist = queue.SimpleQueue()

    epoch = 1
    # Download by slice
    logging.debug(f"[Download] [Slices] slices[0:11] = {slices[0:11]}")
    for low, high in rs.iterate_over_slices(slices, direct=direct_slicing):
        range_info = rs.gen_range_headers(low, high)

        # Fast-forward check
        # TODO Pre-check all fast-forwarded fragments.
        if range_info["Range"] in checklist:
            logging.info(f"[Download][{epoch}/{len(slices) - 1}] [Fast-forward] "
                         f"File of range {range_info['Range']} existed, continue.")
            continue

        # Mix into headers
        headers.update(range_info)
        _name = name_handler(path=path, name=name, range_info=range_info, url=url)
        logging.info(f"[Download][{epoch}/{len(slices) - 1}] "
                     f"Starting with url = {url}, name = {_name}, headers = {headers}")
        st = time.time()
        code = _download(url, name=_name, s=s, headers=headers, data=data)
        duration = time.time() - st
        logging.info(
            f"[Download][{epoch}/{len(slices) - 1}] "
            f"Ended with code = {code}, used {duration} secs @ {(high - low) / 1024 / duration:.2f}KB/s."
        )

        # Successful queue
        if code == 0:
            logging.debug(f"[DEBUG][{epoch}/{len(slices) - 1}] ** ** ** "
                          f"checklist = {checklist.keys()[-6:]}, range_info = {range_info}")
            checklist[range_info["Range"]] = name
            pickle.dump(checklist, checklist_fast_write_fp)
        else:
            retrylist.put((url, name, headers, data))

        # Counter
        epoch += 1

    # Check for the retry list, and retry for those failed items.
    retry_checkpoint = time.time()
    while retrylist.empty() is False:
        failed = retrylist.get(block=False)

        # Timeout breakpoint
        if time.time() - retry_checkpoint > retry_timeout:
            break

        # Retry
        code = _download(*failed)
        if code == 0:
            checklist[failed[-2]["Range"]] = name
        else:
            retrylist.put(failed)

    # Save the unsuccessful items into a pickle file.
    totally_failed = []
    while retrylist.empty() is False:
        totally_failed.append(retrylist.get())

    # if path and os.path.isdir(path):
    #     meta_info_name = os.path.join(path, f"{_name}.failed")
    # else:
    #     meta_info_name = f"{_name}.failed"
    meta_info_name = path_specify(path, name=raw_name)
    with open(meta_info_name, "wb") as pf:
        pickle.dump(totally_failed, pf)

    return checklist, totally_failed
