from io import BufferedReader, BufferedWriter

from downloader.rangespec import UNIT


def iterate_over_size(fp: BufferedReader, amt: int, chunk_size=UNIT):
    if not fp or not fp.readable():
        raise ValueError("File pointer not valid.")

    read = 0
    nbrk = True
    while nbrk:
        _read_size = chunk_size if read + chunk_size < amt else amt - read
        nbrk = False if read + chunk_size > amt else True
        yield fp.read(_read_size)
        read += _read_size


# 1853196977-2018370263-MISSING
def copy_to_file(src_fp: BufferedReader, dst_fp: BufferedWriter, amt: int):
    src_fp.seek(0)
    dst_fp.seek(0)
    for chunk in iterate_over_size(src_fp, amt):
        dst_fp.write(chunk)
