import json
import logging
import os
import pathlib
import time

DEFAULT_DISTRIBUTED_DOWNLOADED_TARFILE = ".dparts.tgz"
DEFAULT_PARTS_LIST_FILE_NAME = ".dparts"
DEFAULT_META_FILE_NAME = ".dmeta"
NS = 1000000000  # S
REPORT_FREQUENCY = int(0.5 * NS)  # 0.5S
SLICING = True
THREADED = True
# CHUNK_SIZE = 1 << 16
CHUNK_SIZE = 1 << 10


class Meta:
    def _instant_save(self, **kwargs):
        # Downloaded meta
        # Should include:
        # Start time, the timestamp at this calling point
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
        logging.info(f"[Meta] Meta saved : {kwargs}.")

    def __init__(self, instant_save=False, **kwargs):
        self.url = kwargs.get("url")
        self.path = kwargs.get("path")
        if self.path is not None and isinstance(self.path, pathlib.Path):
            self.path = str(self.path)
            kwargs["path"] = self.path
        self.name = kwargs.get("name")
        self.headers = kwargs.get("headers")
        self.data = kwargs.get("data")
        self.dparts = kwargs.get("dparts")
        self.content_length = kwargs.get("content_length")
        # Save & load
        self.start_time = time.time() if instant_save else kwargs.get("start_time")
        if instant_save:
            kwargs['start_time'] = self.start_time
            self._instant_save(**kwargs)

    @classmethod
    def load(cls, path) -> "Meta":
        path = pathlib.Path(path)
        if path.is_dir():
            possible_metas = [_meta for _meta in path.glob(f"*{DEFAULT_META_FILE_NAME}")]
            if len(possible_metas) != 1:
                raise FileNotFoundError(f"No meta file found in path : {path}.")
            path = possible_metas[0]

        if path.is_file():
            with open(pathlib.Path(path), "r") as meta:
                d = json.load(meta)
        else:
            raise FileNotFoundError(f"File not found for URI : {str(path.absolute())}")
        o = cls.__new__(cls)
        o.__init__(**d)
        logging.info(f"[Meta] Meta object loaded successfully, kw = {d}.")
        return o

