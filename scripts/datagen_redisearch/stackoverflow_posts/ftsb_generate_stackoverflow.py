#!/usr/bin/python3

import argparse
import csv
import json
import os
import random

# package local imports
import sys
import uuid

import boto3
from tqdm import tqdm

sys.path.append(os.getcwd() + "/..")

from common_datagen import (
    download_url,
    generate_setup_json,
    compress_files,
    generate_inputs_dict_item,
    humanized_bytes,
    del_non_use_case_specific_keys,
    add_key_metric,
    upload_dataset_artifacts_s3,
    add_deployment_requirements_redis_server_module,
    add_deployment_requirements_benchmark_tool,
    add_deployment_requirements_utilities,
    init_deployment_requirement,
    remove_file_if_exists,
)
from pathlib import Path
import string


def str_to_float_or_zero(entry):
    val = 0.0
    try:
        val = float(entry)
    except ValueError as e:
        pass
    return val


def index_or_none(list, value):
    index = None
    try:
        index = list.index(value)
    except ValueError:
        pass
    return index


def rand_str(minN, maxN):
    return "".join(
        random.choices(
            string.ascii_uppercase + string.digits, k=random.randint(minN, maxN)
        )
    )


def rand_arr(minN, maxN):
    arr = []
    for x in range(1, random.randint(minN, maxN)):
        arr.append(rand_str(3, 10))
    return arr


def rand_numeric_arr(minN, maxN):
    arr = []
    for x in range(1, random.randint(minN, maxN)):
        arr.append(random.randint(0, 1000))
    return arr


def rand_date():
    return "2012-04-04T12:56:34.433"


def rand_post():
    post = {"date": rand_date(), "user": rand_str(5, 20)}
    return post


def rand_arr_post(minN, maxN):
    arr = []
    for x in range(1, random.randint(minN, maxN)):
        arr.append(rand_post())
    return arr


def use_case_csv_row_to_cmd():
    doc = {
        "title": rand_str(5, 50),
        "qid": uuid.uuid4().hex,
        "answers": rand_arr_post(2, 5),
        "tag": rand_arr(2, 10),
        "user": rand_str(5, 20),
        "numericArray": rand_numeric_arr(5, 10),
        "creationDate": rand_date(),
    }
    docid_str = "doc:{hash}:{n}".format(hash=uuid.uuid4().hex, n=total_docs)

    cmd = ["WRITE", "W1", 1, "JSON.SET", docid_str, ".", "{}".format(json.dumps(doc))]
    return docid_str, cmd


def human_format(num):
    magnitude = 0
    while abs(num) >= 1000:
        magnitude += 1
        num /= 1000.0
    # add more suffixes if you need them
    return "%.0f%s" % (num, ["", "K", "M", "G", "T", "P"][magnitude])


def arrappend_nested_obj_to_cmd(doc_id):
    return [
        "WRITE",
        "W1",
        1,
        "JSON.ARRAPPEND",
        doc_id,
        "$.answers",
        "{}".format(json.dumps(rand_post())),
    ]


def numincbry_neted_to_cmd(doc_id):
    return [
        "WRITE",
        "W2",
        1,
        "JSON.NUMINCRBY",
        doc_id,
        "$.numericArray[0]",
        "{}".format(random.randint(0, 1000)),
    ]


def update_nested_obj_to_cmd(doc_id):
    return [
        "WRITE",
        "W3",
        1,
        "JSON.SET",
        doc_id,
        "$.answers[0]",
        "{}".format(json.dumps(rand_post())),
    ]


def arrpop_nested_obj_to_cmd(doc_id):
    return [
        "WRITE",
        "W4",
        1,
        "JSON.ARRPOP",
        doc_id,
        "$.answers",
        "{}".format(0),
    ]


def del_nested_obj_to_cmd(doc_id):
    return ["DELETE", "D1", 1, "JSON.DEL", doc_id, "$.tag"]


ARRAPPEND = "ARRAPPEND-NESTED-OBJ"
ARRPOP = "ARRPOP-NESTED-OBJ"
NUMINCRBY = "NUMINCRBY-NESTED"
UPDATE = "UPDATE-NESTED-OBJ"
DEL = "DEL-NESTED-OBJ"
choices_str = "{},{},{},{},{}".format(ARRAPPEND, NUMINCRBY, UPDATE, DEL, ARRPOP)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="RediSearch FTSB data generator.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--project", type=str, default="redisjson", help="the project being tested"
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=12345,
        help="the random seed used to generate random deterministic outputs",
    )
    parser.add_argument(
        "--query-choices",
        type=str,
        default=choices_str,
        help="comma separated list of queries to produce. one of: {}".format(
            choices_str
        ),
    )
    parser.add_argument(
        "--doc-limit",
        type=int,
        default=10000000,
        help="the total documents to generate to be added in the setup stage",
    )
    parser.add_argument(
        "--total-benchmark-commands",
        type=int,
        default=1000000,
        help="the total commands to generate to be issued in the benchmark stage",
    )
    parser.add_argument(
        "--test-name",
        type=str,
        default="stackoverflow_posts-json",
        help="the name of the test",
    )
    parser.add_argument(
        "--test-description",
        type=str,
        default="benchmark making usage of POST format as the dump of StackOverflow posts.",
        help="the full description of the test",
    )
    parser.add_argument(
        "--upload-artifacts-s3",
        default=False,
        action="store_true",
        help="uploads the generated dataset files and configuration file to public benchmarks.redislabs bucket. Proper credentials are required",
    )
    parser.add_argument(
        "--upload-artifacts-s3-uncompressed",
        action="store_true",
        help="uploads the generated dataset files and configuration file to public benchmarks.redislabs bucket. Proper credentials are required",
    )
    parser.add_argument(
        "--temporary-work-dir",
        type=str,
        default="./tmp",
        help="The temporary dir to use as working directory for file download, compression,etc... ",
    )

    args = parser.parse_args()
    use_case_specific_arguments = del_non_use_case_specific_keys(dict(args.__dict__))
    query_choices = args.query_choices.split(",")
    total_benchmark_commands = args.total_benchmark_commands
    # generate the temporary working dir if required
    working_dir = args.temporary_work_dir
    Path(working_dir).mkdir(parents=True, exist_ok=True)
    seed = args.seed
    project = args.project
    doc_limit = args.doc_limit
    test_name = args.test_name
    description = args.test_description
    test_name = "{}-{}".format(human_format(doc_limit), test_name)
    s3_bucket_name = "benchmarks.redislabs"
    s3_bucket_path = "redisearch/datasets/{}/".format(test_name)
    s3_uri = "https://s3.amazonaws.com/{bucket_name}/{bucket_path}".format(
        bucket_name=s3_bucket_name, bucket_path=s3_bucket_path
    )

    benchmark_output_file = "{test_name}.{project}.commands".format(
        test_name=test_name, project=project
    )
    benchmark_config_file = "{test_name}.{project}.cfg.json".format(
        test_name=test_name, project=project
    )
    bench_fname = "{}.BENCH.csv".format(benchmark_output_file, "__".join(query_choices))
    setup_fname = "{}.SETUP.csv".format(benchmark_output_file)

    ## remove previous files if they exist
    remove_file_if_exists(benchmark_config_file)
    remove_file_if_exists(bench_fname)
    remove_file_if_exists(setup_fname)

    used_indices = []
    setup_commands = []
    teardown_commands = []
    key_metrics = []

    total_writes = 0
    total_reads = 0
    total_updates = 0
    total_deletes = 0

    json_version = "0.1"
    benchmark_repetitions_require_teardown_and_resetup = True

    print("-- Benchmark: {} -- ".format(test_name))
    print("-- Description: {} -- ".format(description))

    total_docs = 0

    print("Using random seed {0}".format(args.seed))
    random.seed(args.seed)

    total_docs = 0
    doc_ids = []

    progress = tqdm(unit="docs", total=doc_limit)
    all_csvfile = open(setup_fname, "a", newline="")
    all_csv_writer = csv.writer(all_csvfile, delimiter=",")
    for row_n in range(0, doc_limit):
        docid, cmd = use_case_csv_row_to_cmd()
        all_csv_writer.writerow(cmd)
        progress.update()
        doc_ids.append(docid)
    progress.close()
    all_csvfile.close()
    progress = tqdm(unit="docs", total=total_benchmark_commands)
    all_csvfile = open(bench_fname, "a", newline="")
    all_csv_writer = csv.writer(all_csvfile, delimiter=",")
    len_docs = len(doc_ids)
    row_n = 0
    while row_n < total_benchmark_commands:
        doc_id = doc_ids[random.randint(0, len_docs - 1)]
        choice = random.choices(query_choices)[0]
        if choice == ARRAPPEND:
            cmd = arrappend_nested_obj_to_cmd(doc_id)
        elif choice == NUMINCRBY:
            cmd = numincbry_neted_to_cmd(doc_id)
        elif choice == UPDATE:
            cmd = update_nested_obj_to_cmd(doc_id)
        elif choice == DEL:
            cmd = del_nested_obj_to_cmd(doc_id)
        elif choice == ARRPOP:
            # ensure we add an element before we pop from it
            cmd = arrappend_nested_obj_to_cmd(doc_id)
            all_csv_writer.writerow(cmd)
            row_n = row_n + 1
            progress.update()
            cmd = arrpop_nested_obj_to_cmd(doc_id)
        row_n = row_n + 1
        all_csv_writer.writerow(cmd)
        progress.update()
    progress.close()
    all_csvfile.close()

    if args.upload_artifacts_s3:
        artifacts = [setup_fname, bench_fname]
        upload_dataset_artifacts_s3(s3_bucket_name, s3_bucket_path, artifacts)

    print("############################################")
    print("All artifacts generated.")
