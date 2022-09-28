import os.path
import pathlib
import pickle
import reprlib
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
    def make_head_request(cls, url: str, s: requests.Session = None):
        s = s or requests.Session()
        try:
            resp = s.head(url, verify=False)
        except Exception as be:
            logging.info(be)
            return None
        # content_type = resp.headers.get("Content-Type")
        content_length = int(resp.headers.get("Content-Length", 0))  # bytes

        if "Accept-Ranges" in resp.headers and resp.headers["Accept-Ranges"] != "none":
            range_types = resp.headers.get("Accept-Ranges")
        else:
            range_types = None

        logging.info(resp.headers)
        logging.info(f"[RangeSpec] [HeadSniffing] Content-Length = {content_length}, Accept-Ranges = {range_types}.")
        return content_length, range_types

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
        content_length, range_types = cls.make_head_request(url, s)

        if specified_low and specified_low < content_length:
            return [specified_low, content_length]

        if range_types and not_slicing is False:
            slices = [b for b in range(0, content_length, UNIT)]

            # The built-in range will stop while not reach the last number.
            # Such circumstances can be told from comparing the last element with the content-length.
            if slices[-1] < content_length:
                slices.append(content_length)
        else:
            slices = (0, content_length)
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
            # In direct mode, only two neighbored element can be treated as a valid range.
            for idx in range(0, len(slices) - 1, 2):
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

    Internally, get_slice method can change the formate from string
    liked <low-high> to slices like [low1, high1, low2, high2].

    A DPart file is generated when concatenating fragments.
    """

    def __init__(self, fpath: str):
        from .static import DEFAULT_PARTS_LIST_FILE_NAME
        fpath = pathlib.Path(fpath)

        if not os.path.exists(fpath):
            raise FileNotFoundError

        self.parts_folder: Optional[pathlib.Path] = None

        if os.path.isdir(fpath):
            self.parts_folder = fpath.absolute()
            # Try to find a file suffixed with DEFAULT_PARTS_LIST_FILE_NAME
            target = [_f for _f in fpath.glob(f"*{DEFAULT_PARTS_LIST_FILE_NAME}")]
            if target.__len__() > 1 or target.__len__() == 0:
                raise FileNotFoundError(f"Can't find any valid parts list file in dir {fpath!r}")

            fpath = target[0].absolute()
        else:
            self.parts_folder = fpath.parent.absolute()

        with open(fpath, 'rb') as tasks:
            self._dparts = set(pickle.load(tasks))

        self._dparts = list(self._dparts)
        self._dparts.sort(key=lambda x: int(x.split('-')[0]))
        # Lazyload
        self._slices = None

    def __contains__(self, item):
        if isinstance(item, str):
            return item in self._dparts
        return False

    def __len__(self):
        return self._dparts.__len__()  if self._slices is None else self._slices.__len__() // 2

    def as_list(self) -> List[str]:
        return list(self._dparts)

    @classmethod
    def range_slices_narrow_down(cls, _l, _h) -> List[int]:
        r = [_ for _ in range(_l, _h, UNIT)]
        if _h not in r:
            r.append(_h)
        return r

    def get_range_slices(self, **kwargs):
        # Lazyload
        if not self._slices:
            cache = set()
            slices = []
            for s in self._dparts:
                l, h = s.split("-")
                l, h = int(l), int(h)
                if h - l > UNIT:
                    cache.add(l)
                    cache.add(h)
                    slices.extend(_t := self.range_slices_narrow_down(l, h))
                    logging.info(f"[RangeSpec] Range {l}-{h} from "
                                 f"dparts larger than UNIT:{UNIT}, "
                                 f"cutting down to pieces: {reprlib.repr(_t)}.")
                if l not in cache:
                    slices.append(l)
                    cache.add(l)
                if h not in cache:
                    slices.append(h)
                    cache.add(h)
            del cache
            slices.sort(key=lambda x: int(x))
            self._slices = slices
        return self._slices


class BlockInterpreter:

    def _parse(self):
        # 首先检查是否有column
        if ":" in self._raw:
            tokens = self._raw.split(":")
        else:
            tokens = [self._raw]

        # 对于每个range item进行处理
        # 首先要循环找出大于或小于的范围匹配参数，以供精确值的修复使用
        for token in tokens:
            if "<" in token:
                try:
                    self._lower_bound = int(token.replace("<", ""))
                except Exception:  # noqa  如果int转换失败，对这个token报错
                    logging.exception(
                        f"[RangeSpec] [Block] Cannot convert the specified identifier {token!r} into range spec."
                    )
                    exit(-1)
            elif ">" in token:
                try:
                    self._upper_bound = int(token.replace(">", ""))
                except Exception:  # noqa
                    logging.exception(
                        f"[RangeSpec] [Block] Cannot convert the specified identifier {token!r} into range spec."
                    )
                    exit(-1)

        for token in tokens:
            if "<" in token or ">" in token:
                continue

            if "-" in token:
                try:
                    _l, _h = token.split('-')
                except Exception:  # noqa
                    logging.exception(
                        f"[RangeSpec] [Block] Cannot parse range {token!r} into exactly two bounds."
                    )
                    exit(-1)

                try:
                    _l, _h = int(_l), int(_h)  # noqa
                except Exception:  # noqa
                    logging.exception(
                        f"[[RangeSpec] [Block]] Cannot parse range {token!r} into integers."
                    )
                    exit(-1)
            else:
                try:
                    _l, _h = int(token), int(token)
                except Exception:  # noqa
                    logging.exception(
                        f"[RangeSpec] [Block] Cannot parse range {token!r} into integer."
                    )
                    exit(-1)

            assert isinstance(_l, int) and isinstance(_h, int)

            # 扩展此部分range
            for _i in range(_l, _h + 1):
                if self._lower_bound and _i <= self._lower_bound:
                    continue
                if self._upper_bound and _i >= self._upper_bound:
                    continue
                self._ranges.add(_i)

    def __init__(self, block_stmt: str):
        self._raw: str = block_stmt

        self._upper_bound: Optional[int] = None  # 包含自己
        self._lower_bound: Optional[int] = None  # 包含自己
        self._ranges = set()  # 精确命中

        self._parse()

    def __contains__(self, item):
        if not isinstance(item, int):
            return False

        # 精确搜索
        if item in self._ranges:
            return True

        # 范围搜索
        if (self._upper_bound and item >= self._upper_bound) or (self._lower_bound and item <= self._lower_bound):
            return True
        return False

    def __repr__(self):
        return super(BlockInterpreter, self).__repr__().rsplit(">", maxsplit=1)[0] + \
               f" == Lower: {self._lower_bound}, Upper: {self._upper_bound}, Ranges: {reprlib.repr(self._ranges)}>"

