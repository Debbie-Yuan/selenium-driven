import os
import pathlib
import urllib3
import logging
import argparse

from downloader import download
from downloader.rangespec import DParts

logging.basicConfig(filename="tests.log", level=logging.DEBUG)
# logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


base = argparse.ArgumentParser()
base.add_argument("url")
base.add_argument("-c", "--dparts", help="Folder or specific parts list file path.")


def test(**kwargs):
    # Mixin
    keywords = {'path': None, 'name': None, 'headers': None, 'data': None, 'retry_timeout': 3600, 'dparts': None}

    # Arguments check
    url = kwargs.get('url')
    if not url:
        raise ValueError("No url provided.")
    name = url.rsplit("/", maxsplit=1)[-1]
    if "." in name:
        name = name.split(".")[0]

    # Path & dir preparation
    path = str(pathlib.Path(__file__).absolute().parent / name)
    logging.info(f"[ENV] Preparing env with file {name}, dir {path} created, begin to download.")
    if not os.path.exists(path):
        os.mkdir(path)
        keywords["path"] = path

    # DParts
    dparts = kwargs.get("dparts")
    if dparts:
        dp = DParts(dparts)
        keywords["dparts"] = dp

    cl, tf = download(url, **keywords)
    logging.info(tf)


if __name__ == '__main__':
    urllib3.disable_warnings()
    base = argparse.ArgumentParser()
    base.add_argument("url")
    base.add_argument("-c", "--dparts")
    args = base.parse_args()
    test(**args.__dict__)
