import os
import sys
import pathlib
import urllib3
import logging

from downloader import download


logging.basicConfig(filename="tests.log", level=logging.DEBUG)


def test():

    if len(sys.argv) == 1:
        raise ValueError("No url provided.")
    url = sys.argv[-1]
    name = url.rsplit("/", maxsplit=1)[-1]
    if "." in name:
        name = name.split(".")[0]

    path = str(pathlib.Path(__file__).absolute().parent / name)
    logging.info(f"[ENV] Preparing env with file {name}, dir {path} created, begin to download.")
    if not os.path.exists(path):
        os.mkdir(path)
    cl, tf = download(url, path=path)
    logging.info(tf)


if __name__ == '__main__':
    urllib3.disable_warnings()
    test()
