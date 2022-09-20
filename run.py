import os
import pathlib
import urllib3
import logging
import argparse

from downloader import download, concat
from downloader.rangespec import DParts

logging.basicConfig(filename="tests.log", level=logging.DEBUG)
# logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


def download_wrapper(**kwargs):
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


def concat_wrapper(**kwargs):
    # Arguments check
    path = kwargs.get('path')
    if not os.path.exists(path):
        raise FileNotFoundError(f"Path {path} not found.")
    concat(path)


def get_argparser():
    base = argparse.ArgumentParser()

    # Download subcommand
    _download = base.add_subparsers()
    download_parser = _download.add_parser("download")
    download_parser.add_argument("url")
    download_parser.add_argument("-c", "--dparts", help="Folder or specific parts list file path.")
    download_parser.set_defaults(func=download_wrapper)

    # Concat subcommand
    _concat = base.add_subparsers()
    concat_parser = _concat.add_parser("concat")
    concat_parser.add_argument("path")
    concat_parser.set_defaults(func=concat_wrapper)

    return base


if __name__ == '__main__':
    base = get_argparser()

    # Disable warnings
    urllib3.disable_warnings()

    # Parse args & execute
    args = base.parse_args()
    args.func(**args.__dict__)
    exit(0)
