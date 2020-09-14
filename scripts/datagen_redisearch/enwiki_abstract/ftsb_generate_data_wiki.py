import xml.etree.ElementTree as ET
from os import path

# package local imports
from common import decompress_file, download_url
from tqdm import tqdm

origin = "https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-abstract1.xml.gz"
filename = "enwiki-latest-abstract1.xml.gz"
decompressed_fname = "enwiki-latest-abstract1.xml"

if (__name__ == "__main__"):
    if path.exists(filename) is False:
        download_url(origin, filename)

    if path.exists(decompressed_fname) is False:
        decompress_file(filename)

    docs = []
    tree = ET.iterparse(decompressed_fname)
    progress = tqdm(unit="docs")

    for event, elem in tree:
        if elem.tag == "doc":
            doc = {}
            doc["title"] = elem.findtext("title")
            doc["url"] = elem.findtext("url")
            doc["abstract"] = elem.findtext("abstract")
            docs.append(doc)
            elem.clear()  # won't need the children any more
            progress.update()

    progress.close()
