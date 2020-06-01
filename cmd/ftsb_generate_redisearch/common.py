import gzip
import os
import shutil
import tarfile
import urllib
from zipfile import ZipFile

from tqdm import tqdm


def decompress_file(filename):
    splitted = os.path.splitext(filename)
    stripped_fname = splitted[0]
    filetype = splitted[1]

    if (filetype == ".zip"):
        with ZipFile(filename, 'r') as zipObj:
            zipObj.extractall()

    elif (filetype == ".gz"):
        with gzip.open(filename, 'rb') as f_in:
            with open(stripped_fname, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)


def compress_files(files, archive_name):
    status = True
    compressed_size = 0
    uncompressed_size = 0
    tar = tarfile.open(archive_name, "w:gz")
    for file_name in files:
        tar.add(file_name, os.path.basename(file_name))
        uncompressed_size += os.path.getsize(file_name)
    tar.close()
    compressed_size = os.path.getsize(archive_name)
    return status, uncompressed_size, compressed_size


class DownloadProgressBar(tqdm):
    def update_to(self, b=1, bsize=1, tsize=None):
        if tsize is not None:
            self.total = tsize
        self.update(b * bsize - self.n)


def download_url(url, output_path):
    with DownloadProgressBar(unit='B', unit_scale=True,
                             miniters=1, desc=url.split('/')[-1]) as t:
        urllib.request.urlretrieve(url, filename=output_path, reporthook=t.update_to)


def generate_setup_json(json_version, use_case_specific_arguments, test_name, description, inputs, setup_commands, teardown_commands, used_indices,
                        total_commands, total_setup_commands, total_benchmark_commands, total_docs, total_writes,
                        total_updates, total_reads, total_deletes, benchmark_repetitions_require_teardown_and_resetup, setup_input_files,benchmark_input_files):
    setup_json = {
        "specifications-version": json_version,
        "name": test_name,
        "description": description,
        "use-case-specific-arguments" : use_case_specific_arguments,
        "inputs": inputs,
        "setup": {
            "commands": setup_commands,
            "input-files" : setup_input_files
        },
        "benchmark":{
                "repetitions-require-teardown-and-re-setup" : benchmark_repetitions_require_teardown_and_resetup,
                "input-files" : benchmark_input_files
        },
        "teardown": {
            "commands": teardown_commands
        },
        "used-indices": used_indices,
        "total-commands": total_commands,
        "total-setup-commands": total_setup_commands,
        "total-benchmark-commands": total_benchmark_commands,
        "command-category": {
            "setup-writes": total_docs,
            "writes": total_writes,
            "updates": total_updates,
            "reads": total_reads,
            "deletes": total_deletes,
        }
    }
    return setup_json
