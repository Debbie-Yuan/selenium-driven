import os
import pathlib
import sys

import urllib3
import logging
import argparse

from downloader import download, concat
from downloader.rangespec import DParts

logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.root.setLevel(logging.DEBUG)


def download_wrapper(**kwargs):
    logging.basicConfig(filename="tests.log", level=logging.DEBUG)
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
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    # Arguments check
    path = kwargs.get('path')
    if not os.path.exists(path):
        raise FileNotFoundError(f"Path {path} not found.")
    concat(path)


def get_argparser():
    _base = argparse.ArgumentParser()
    _base.set_defaults(func=lambda **kwargs: print(_base.format_help()))

    # Download subcommand
    subparser = _base.add_subparsers()
    download_parser = subparser.add_parser("download")
    download_parser.add_argument("url")
    download_parser.add_argument("-c", "--dparts", help="Folder or specific parts list file path.")
    download_parser.add_argument("-p", "--path", help="Folder to store the file.")
    download_parser.add_argument("-n", "--name", help="Name of the file.")
    download_parser.set_defaults(func=download_wrapper)

    # Concat subcommand
    concat_parser = subparser.add_parser("concat")
    concat_parser.add_argument("path")
    concat_parser.set_defaults(func=concat_wrapper)

    return _base


if __name__ == '__main__':
    base = get_argparser()

    # Disable warnings
    urllib3.disable_warnings()

    # Parse args & execute
    args = base.parse_args()
    args.func(**args.__dict__)
    exit(0)
