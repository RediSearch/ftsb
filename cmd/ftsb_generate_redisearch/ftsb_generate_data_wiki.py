import gzip
import os
import os.path
import shutil
import urllib.request
from os import path
from zipfile import ZipFile
import xml.etree.ElementTree as ET

from tqdm import tqdm


class DownloadProgressBar(tqdm):
    def update_to(self, b=1, bsize=1, tsize=None):
        if tsize is not None:
            self.total = tsize
        self.update(b * bsize - self.n)


def download_url(url, output_path):
    with DownloadProgressBar(unit='B', unit_scale=True,
                             miniters=1, desc=url.split('/')[-1]) as t:
        urllib.request.urlretrieve(url, filename=output_path, reporthook=t.update_to)


origin = "https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-abstract1.xml.gz"
filename = "enwiki-latest-abstract1.xml.gz"
decompressed_fname = "enwiki-latest-abstract1.xml"


def decompress_file(filename):
    splitted = os.path.splitext(filename)
    stripped_fname = splitted[0]
    filetype = splitted[1]

    if (filetype == ".zip"):
        with ZipFile(filename, 'r') as zipObj:
            zipObj.extractall()

    # elif (filetype == ".bz2"):
    #     file = BZ2File(filename, mode)

    elif (filetype == ".gz"):
        with gzip.open(filename, 'rb') as f_in:
            with open(stripped_fname, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

if (__name__ == "__main__"):
    if path.exists(filename) is False:
        print
        download_url(origin, filename)

    if path.exists(decompressed_fname) is False:
        decompress_file(filename)

    docs = []
    tree = ET.iterparse(decompressed_fname)
    progress = tqdm(unit="docs")

    for event, elem in tree:
        if elem.tag == "doc":
            doc = {}
            doc["title"]=elem.findtext("title")
            doc["url"]=elem.findtext("url")
            doc["abstract"]=elem.findtext("abstract")
            docs.append(doc)
            elem.clear() # won't need the children any more
            progress.update()

    progress.close()