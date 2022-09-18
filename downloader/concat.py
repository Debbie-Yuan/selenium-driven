import os
import pathlib
import sys
import tqdm


def concat(path):
    if not os.path.isdir(path):
        raise FileNotFoundError(path)
    p = pathlib.Path(path)
    files = [file for file in p.glob("*.*") if "@bytes" in file.name]
    # @bytes=119537665-125829120
    files.sort(key=lambda x: int(str(x.name).rsplit("-", maxsplit=1)[-1]))
    real_name = files[0].name.rsplit("@", maxsplit=1)[0]
    final_path = p / real_name
    with open(final_path, "wb") as fp:
        for file in tqdm.tqdm(files):
            with open(file, "rb") as _tf:
                fp.write(_tf.read())


if __name__ == '__main__':
    concat(sys.argv[-1])
