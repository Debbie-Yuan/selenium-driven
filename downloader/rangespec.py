import os.path
import pathlib
import pickle
from typing import List, Optional
import requests
import logging


KB = 1 << 10
HALF_MB = 1 << 19
MB = 1 << 20
UNIT = int(MB * 3)


class RangeSlicer:

    def __init__(self, unit: int = UNIT):
        self.UNIT = unit

    @classmethod
    def get_range_slices(
            cls,
            url: str,
            s: requests.Session = None,
            not_slicing: bool = False,
            specified_low: int = 0
    ) -> Optional[List[int]]:
        """
        Decide the file slices by knowing whether the server support file range spec,
        and then calculating the optimal slicing result by given unit.
        :param specified_low: Used to support the user-liked low range cursor.
        :param not_slicing: If the switch is on, the whole range of that file will be returned.
        :param url: str, url you want to download from.
        :param s: requests.Session, a session object from which the HEAD pre-query request is to be sent.
        :return:
        """
        s = s or requests.Session()
        try:
            resp = s.head(url, verify=False)
        except Exception as be:
            logging.info(be)
            return None
        # content_type = resp.headers.get("Content-Type")
        content_length = int(resp.headers.get("Content-Length", 0))  # bytes
        if specified_low and specified_low < content_length:
            return [specified_low, content_length]

        if "Accept-Ranges" in resp.headers and resp.headers["Accept-Ranges"] != "none":
            range_types = resp.headers.get("Accept-Ranges")
        else:
            range_types = None
        logging.info(f"[RangeSpec] [HeadSniffing] Content-Length = {content_length}, Accept-Ranges = {range_types}.")

        if range_types and not_slicing is False:
            slices = [b for b in range(0, content_length, UNIT)]

            # The built-in range will stop while not reach the last number.
            # Such circumstances can be told from comparing the last element with the content-length.
            if slices[-1] < content_length:
                slices.append(content_length)
        else:
            slices = (0, content_length)
        logging.info(resp.headers)
        logging.info(slices)

        return slices

    @classmethod
    def gen_range_headers(cls, low: int, high: int, range_type="bytes") -> dict[str, str]:
        return {"Range": f"{range_type}={low}-{high}"}

    @classmethod
    def iterate_over_slices(
            cls,
            slices: List[int],
            direct=False
    ):
    # ) -> Generator[None, int, int]:  # noqa
        if not direct:
            if slices[0] != -1:
                slices[0] = -1
            for idx in range(0, len(slices) - 1):
                yield slices[idx] + 1, slices[idx + 1]
        else:
            for idx in range(0, len(slices) - 1):
                yield slices[idx], slices[idx + 1]


class DParts:
    """
    A DParts represent a target block tasks hierarchy,
    the class need a picked file path as its all parameter,
    from which it will try to read the content, and then
    convert into a set for speeding up comparing.

    A recommended usage is still downloading with the original
    slices order, and check every range info using "in" operator,
    if the object return True, then you should continue.

    Every bytes-range is represented as <low-high>, the result of
    stripping "@bytes=" from the original name.
    """

    def __init__(self, fpath: str):
        from .static import DEFAULT_PARTS_LIST_FILE_NAME
        fpath = pathlib.Path(fpath)

        if not os.path.exists(fpath):
            raise FileNotFoundError

        if os.path.isdir(fpath):
            # Try to find a file suffixed with DEFAULT_PARTS_LIST_FILE_NAME
            target = [_f for _f in fpath.glob(f"*{DEFAULT_PARTS_LIST_FILE_NAME}")]
            if target.__len__() > 1 or target.__len__() == 0:
                raise FileNotFoundError(f"Can't find any valid parts list file in dir {fpath!r}")

            fpath = target[0].absolute()

        with open(fpath, 'rb') as tasks:
            self._dparts = set(pickle.load(tasks))

        # Lazyload
        self._slices = None

    def __contains__(self, item):
        if isinstance(item, str):
            return item in self._dparts
        return False

    def __len__(self):
        return self._dparts.__len__() // 2 if self._slices is None else self._slices.__len__()

    def as_list(self) -> List[str]:
        return list(self._dparts)

    def get_range_slices(self, **kwargs):
        # Lazyload
        if not self._slices:
            cache = set()
            slices = []
            for s in self._dparts:
                l, h = s.split("-")
                if l not in cache:
                    slices.append(l)
                    cache.add(l)
                if h not in cache:
                    slices.append(h)
                    cache.add(h)
            del cache
            slices.sort()
            self._slices = slices
        return self._slices
