# from webserver
import concurrent.futures
import logging
import math
import os.path
import pathlib
import reprlib
import threading
import time
from typing import Optional

import requests

from urllib.parse import urljoin, unquote
from lxml import etree
from lxml.etree import _Element  # noqa


def seconds_friendly(secs):
    if secs == math.nan:
        return str(math.nan)
    # s
    if secs < 60:
        return f"{secs}s"
    # m:s
    elif 60 <= secs < 3600:
        m = secs // 60
        s = secs % 60
        return f"{m}m:{s}s"
    # h:m:s
    elif 3600 <= secs < 3600 * 24:
        h = secs // 3600
        hl = secs % 3600
        m = hl // 60
        s = hl % 60
        return f"{h}h:{m}m:{s}s"
    # D:H:M:S
    else:
        d = secs // (3600 * 24)
        dl = secs % (3600 * 24)
        h = dl // 3600
        hl = dl % 3600
        m = hl // 60
        s = hl % 60
        return f"{d}d:{h}h:{m}m:{s}s"


class WebServerMigrator:
    def __init__(self, url: str):
        self._tp = concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix="migrator")
        self._url = url
        if not self._url.endswith('/'):
            self._url += '/'

        # Bytes audit
        self._tp_lock = threading.Lock()
        self._current_amt = 0
        self._total_amt = 0

        # Task audit
        self._finished_length_lock = threading.Lock()
        self.__targets_length = 0
        self._finished_length = 0

        self.__single_thread: Optional[threading.Thread] = None

    def _download(self, url, name, path: pathlib.Path):
        with open(path / name, "wb") as buffer:
            resp = requests.get(url, stream=True)
            for chunk in resp.iter_content(chunk_size=2048):
                buffer.write(chunk)
                with self._tp_lock:
                    self._current_amt += 2048
        with self._finished_length_lock:
            self._finished_length += 1
        logging.info(f"[_download] Finish task, url = {url}.")

    def gen_tasks(self, targets):
        for link, n in targets.items():
            yield urljoin(self._url, unquote(link, encoding="utf8")), n

    def calc_total_size(self, targets):
        size = 0
        tasks = [(a, b) for a, b in self.gen_tasks(targets)]
        current = 1
        total = len(tasks) + 1
        for link, _ in tasks:
            resp = requests.head(link)
            heads = resp.headers
            if resp.status_code != 200:
                logging.warning(f"[Migrator] [{current}/{total}] Head request to {link} "
                                f"failed with status_code = {resp.status_code}, ignored.")
                current += 1
                continue
            try:
                size += int(heads["Content-Length"])
                logging.info(f"f[Migrator] [{current}/{total}] Head to {link} got heads = {heads}.")
            except KeyError:
                logging.warning(
                    f"[Migrator] [{current}/{total}] Head request to link {link} "
                    f"didn't respond with 'Content-Length', ignored. Heads = {heads}."
                )
            finally:
                current += 1

        self._total_amt = size
        return size

    def _single_threaded(self, targets, path):
        for link, n in targets.items():
            link = urljoin(self._url, unquote(link, encoding="utf-8"))
            self._download(link, n, path)

    def single_threaded_download(self, targets, path) -> threading.Thread:
        return threading.Thread(target=self._single_threaded, args=(targets, path), name="Worker")

    def main_receiver(self):
        downloaded = 0
        while self.__single_thread and self.__single_thread.is_alive():
            time.sleep(1)
            with self._tp_lock:
                speed = self._current_amt
                self._current_amt = 0
            downloaded += speed
            speed_kilo = speed / 1000
            eta = (self._total_amt - downloaded) // speed if speed != 0 else math.nan  # seconds
            logging.info(f"[Migrator] [{self._finished_length}/{self.__targets_length}] "
                         f"Downloaded {downloaded}/{self._total_amt} bytes @ {speed_kilo:.2f} kbps, "
                         f"ETA : {seconds_friendly(eta)}.")

    def migrate(self, to=None, mkdir=True, **kwargs):  # noqa
        if not (to and os.path.exists(to)):
            to = pathlib.Path(os.getcwd())
        else:
            to = pathlib.Path(to)
        # Download nested
        # Depth first
        if mkdir:
            dir_name = self._url.rsplit('/', maxsplit=2)[1]
            destiny = to / dir_name
            try:
                os.mkdir(destiny)
            except FileExistsError as e:
                logging.exception(str(e))
                exit(1)
        else:
            destiny = to
        resp = requests.get(self._url)
        tree: _Element = etree.HTML(resp.text)
        targets = {b.attrib['href']: b.text for b in tree.xpath("//a") if b.text not in {"../", "./"}}
        logging.info(f"[Migrator] Detected targets {reprlib.repr(targets)} with length = {len(targets)}.")
        self.__targets_length = len(targets)
        self.calc_total_size(targets)
        logging.info(f"[Migrator] Created with {self.__targets_length} targets, size = {self._total_amt}.")

        logging.info(f"[Migrator] Begin downloading...")
        # Calling
        self.__single_thread = self.single_threaded_download(targets, path=destiny)
        self.__single_thread.start()
        self.main_receiver()
