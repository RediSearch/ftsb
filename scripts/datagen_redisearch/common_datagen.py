import gzip
import os
import shutil
import tarfile
import bz2
import urllib.request
from zipfile import ZipFile

import boto3
from tqdm import tqdm


def remove_file_if_exists(filename):
    if os.path.exists(filename):
        os.remove(filename)


def decompress_file(filename):
    splitted = os.path.splitext(filename)
    stripped_fname = splitted[0]
    filetype = splitted[1]
    if filetype == ".zip":
        with ZipFile(filename, "r") as zipObj:
            zipObj.extractall()

    elif filetype == ".gz":
        with gzip.open(filename, "rb") as f_in:
            with open(stripped_fname, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

    elif filetype == ".bz2":
        with bz2.open(filename, "rb") as f_in:
            with open(stripped_fname, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)


def init_deployment_requirement():
    dr = {"utilities": {}, "benchmark-tool": None, "redis-server": {"modules": {}}}
    return dr


def add_deployment_requirements_redis_server_module(
    deployment_requirements, module_name, module_dict
):
    deployment_requirements["redis-server"]["modules"][module_name] = module_dict


def add_deployment_requirements_utilities(
    deployment_requirements, utility_name, utility_dict
):
    deployment_requirements["utilities"][utility_name] = utility_dict


def add_deployment_requirements_benchmark_tool(deployment_requirements, tool):
    deployment_requirements["benchmark-tool"] = tool


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
    with DownloadProgressBar(
        unit="B", unit_scale=True, miniters=1, desc=url.split("/")[-1]
    ) as t:
        urllib.request.urlretrieve(url, filename=output_path, reporthook=t.update_to)


def upload_dataset_artifacts_s3(s3_bucket_name, s3_bucket_path, artifacts):
    print("-- uploading dataset artifacts to s3 -- ")
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(s3_bucket_name)
    progress = tqdm(unit="files", total=len(artifacts))
    for input in artifacts:
        object_key = "{bucket_path}{filename}".format(
            bucket_path=s3_bucket_path, filename=input
        )
        print(
            "https link: https://s3.amazonaws.com/{}/{}".format(
                s3_bucket_name, object_key
            )
        )
        bucket.upload_file(input, object_key)
        object_acl = s3.ObjectAcl(s3_bucket_name, object_key)
        response = object_acl.put(ACL="public-read")
        progress.update()
    progress.close()


def generate_setup_json(
    json_version,
    project,
    use_case_specific_arguments,
    test_name,
    description,
    run_stages,
    deployment_requirements,
    key_metrics,
    inputs,
    setup_commands,
    teardown_commands,
    used_keys,
    total_commands,
    total_setup_commands,
    total_benchmark_commands,
    total_setup_writes,
    total_writes,
    total_updates,
    total_reads,
    total_deletes,
    benchmark_repetitions_require_teardown_and_resetup,
    setup_input_files,
    benchmark_input_files,
):
    setup_json = {
        "specifications-version": json_version,
        "project": project,
        "name": test_name,
        "description": description,
        "run-stages": run_stages,
        "use-case-specific-arguments": use_case_specific_arguments,
        "deployment-requirements": deployment_requirements,
        "key-metrics": key_metrics,
        "inputs": inputs,
        "setup": {"commands": setup_commands, "input-files": setup_input_files},
        "benchmark": {
            "repetitions-require-teardown-and-re-setup": benchmark_repetitions_require_teardown_and_resetup,
            "input-files": benchmark_input_files,
        },
        "teardown": {"commands": teardown_commands},
        "used-keys": used_keys,
        "total-commands": total_commands,
        "total-setup-commands": total_setup_commands,
        "total-benchmark-commands": total_benchmark_commands,
        "command-category": {
            "setup-writes": total_setup_writes,
            "writes": total_writes,
            "updates": total_updates,
            "reads": total_reads,
            "deletes": total_deletes,
        },
    }
    return setup_json


""" Returns a human readable string reprentation of bytes"""


def humanized_bytes(bytes, units=[" bytes", "KB", "MB", "GB", "TB"]):
    return (
        str(bytes) + " " + units[0]
        if bytes < 1024
        else humanized_bytes(bytes >> 10, units[1:])
    )


def add_key_metric(
    key_metrics,
    step,
    metric_family,
    metric_json_path,
    metric_name,
    metric_unit,
    metric_type,
    comparison_mode_str,
    per_step_priority,
):
    km = {
        "step": step,
        "metric-family": metric_family,
        "metric-json-path": metric_json_path,
        "metric-name": metric_name,
        "unit": metric_unit,
        "metric-type": metric_type,
        "comparison": comparison_mode_str,
        "per-step-comparison-metric-priority": per_step_priority,
    }
    key_metrics.append(km)


def del_non_use_case_specific_keys(use_case_specific_arguments):
    del use_case_specific_arguments["upload_artifacts_s3"]
    del use_case_specific_arguments["test_name"]
    del use_case_specific_arguments["test_description"]
    return use_case_specific_arguments


def generate_inputs_dict_item(
    type,
    all_fname,
    description,
    remote_url,
    uncompressed_size,
    compressed_filename,
    compressed_size,
    total_commands,
    command_category,
):
    dict = {
        "local-uncompressed-filename": all_fname,
        "local-compressed-filename": compressed_filename,
        "type": type,
        "description": description,
        "remote-url": remote_url,
        "compressed-bytes": compressed_size,
        "compressed-bytes-humanized": humanized_bytes(compressed_size),
        "uncompressed-bytes": uncompressed_size,
        "uncompressed-bytes-humanized": humanized_bytes(uncompressed_size),
        "total-commands": total_commands,
        "command-category": command_category,
    }
    return dict
