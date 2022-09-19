from typing import List, Optional
import requests
import logging


KB = 1 << 10
HALF_MB = 1 << 19
MB = 1 << 20
UNIT = int(MB * 6)


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
            slices: List[int]
    ):
    # ) -> Generator[None, int, int]:  # noqa
        if slices[0] != -1:
            slices[0] = -1
        for idx in range(0, len(slices) - 1):
            yield slices[idx] + 1, slices[idx + 1]
