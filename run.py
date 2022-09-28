import os
import pathlib
import sys

import urllib3
import logging
import argparse

from downloader import download, concat
from downloader.rangespec import DParts
from utils.migrate import WebServerMigrator
from statics import LOGGING_FORMAT

logging.getLogger("requests").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("urllib").setLevel(logging.ERROR)
logging.root.setLevel(logging.DEBUG)


def download_wrapper(**kwargs):
    logging.basicConfig(
        filename="tests.log",
        level=logging.DEBUG,
        format=LOGGING_FORMAT
    )
    # Mixin
    keywords = {'path': None, 'name': None, 'headers': None,
                'data': None, 'retry_timeout': 3600, 'dparts': None, 'block_index': None}

    # Arguments check
    url = kwargs.get('url')
    if not url:
        raise ValueError("No url provided.")
    name = url.rsplit("/", maxsplit=1)[-1]
    if "." in name:
        name = name.split(".")[0]

    # Path & dir preparation
    if kwargs["path"] and not os.path.exists(kwargs["path"]):
        path = kwargs["path"]
    else:
        path = str(pathlib.Path(__file__).absolute().parent / name)
    logging.info(f"[ENV] Preparing env with file {name}, dir {path} created, begin to download.")
    if not os.path.exists(path):
        os.mkdir(path)
        keywords["path"] = path

    # DParts
    dparts = kwargs.get("dparts")
    if dparts:
        dp = DParts(dparts)
        keywords["dparts"] = dp  # NOQA

    # block_index
    block_index = kwargs.get("block_index")
    if block_index:
        logging.info("[ENV] Ignoring DParts thus enabling index guided download.")
        keywords["dparts"] = None
        keywords["block_index"] = int(block_index)

    cl, tf = download(url, **keywords)
    logging.info(tf)


def concat_wrapper(**kwargs):
    logging.basicConfig(
        stream=sys.stdout,
        level=logging.DEBUG,
        format=LOGGING_FORMAT
    )
    # Arguments check
    path = kwargs.get('path')
    if not os.path.exists(path):
        raise FileNotFoundError(f"Path {path} not found.")
    concat(**kwargs)


def migrate_wrapper(**kwargs):
    logging.basicConfig(
        stream=sys.stdout,
        level=logging.DEBUG,
        format=LOGGING_FORMAT
    )
    wm = WebServerMigrator(kwargs.get("url"))
    wm.migrate(**kwargs)


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
    download_parser.add_argument("-I", "--block_index", help="Integers of fragment index.", type=int)
    download_parser.set_defaults(func=download_wrapper)

    # Concat subcommand
    concat_parser = subparser.add_parser("concat")
    concat_parser.add_argument("path")
    concat_parser.add_argument("-f", "--without_meta", action="store_true", help="Continue without meta file.")
    concat_parser.add_argument("-F", "--force", action="store_true", help="Don't check mission, just concat.")
    concat_parser.add_argument("-E", "--export", action="store_true", help="Export digest only.")
    concat_parser.set_defaults(func=concat_wrapper)

    # Migrate subcommand
    migrate_parser = subparser.add_parser("migrate")
    migrate_parser.add_argument("url")
    migrate_parser.add_argument("-t", "--to", help="Local store directory, default is the current working directory.")
    migrate_parser.add_argument(
        "-m",
        "--mkdir",
        action="store_true",
        help="Make another directory based on to, or current working directory."
    )
    migrate_parser.set_defaults(func=migrate_wrapper)

    return _base


if __name__ == '__main__':
    base = get_argparser()

    # Disable warnings
    urllib3.disable_warnings()

    # Parse args & execute
    args = base.parse_args()
    args.func(**args.__dict__)
    exit(0)
