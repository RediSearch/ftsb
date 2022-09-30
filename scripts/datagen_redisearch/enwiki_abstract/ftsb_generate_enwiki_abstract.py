import re
import xml.etree.ElementTree as ET
from os import path

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

# package local imports

sys.path.append(os.getcwd() + "/..")
field_tokenization = ",.<>{}[]\"':;!@#$%^&*()-+=~"

SIMPLE_WORD_QUERY = "simple-1word-query"
SIMPLE_2WORD_UNION_QUERY = "2word-union-query"
SIMPLE_2WORD_INT_QUERY = "2word-intersection-query"
WILDCARD_QUERY = "wildcard"
SUFFIX_QUERY = "suffix"
CONTAINS_QUERY = "contains"
PREFIX_QUERY = "prefix"

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
    decompress_file,
)

from tqdm import tqdm
from pathlib import Path

origin = "https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-abstract1.xml.gz"
filename = "enwiki-latest-abstract1.xml.gz"
decompressed_fname = "enwiki-latest-abstract1.xml"


def generate_enwiki_abstract_index_type():
    types = {}
    for f in ["title", "url", "abstract"]:
        types[f] = "text"
    return types


def generate_ft_create_row(index, index_types, use_ftadd):
    if use_ftadd:
        cmd = ['"FT.CREATE"', '"{index}"'.format(index=index), '"SCHEMA"']
    else:
        cmd = [
            '"FT.CREATE"',
            '"{index}"'.format(index=index),
            '"ON"',
            '"HASH"',
            '"SCHEMA"',
        ]
    for f, v in index_types.items():
        cmd.append('"{}"'.format(f))
        cmd.append('"{}"'.format(v))
        cmd.append('"SORTABLE"')
    return cmd


def generate_ft_drop_row(index):
    cmd = ["FT.DROP", "{index}".format(index=index), "DD"]
    return cmd


def EscapeTextFileString(field):
    for char_escape in field_tokenization:
        field = field.replace(char_escape, "\\{}".format(char_escape))
    return field


def use_case_to_cmd(use_ftadd, title, url, abstract, total_docs):
    hash = {
        "title": EscapeTextFileString(title),
        "url": EscapeTextFileString(url),
        "abstract": EscapeTextFileString(abstract),
    }
    docid_str = "doc:{hash}:{n}".format(hash=uuid.uuid4().hex, n=total_docs)
    fields = []
    for f, v in hash.items():
        if v is not None:
            fields.append(f)
            fields.append(v)
    if use_ftadd is False:
        cmd = ["WRITE", "W1", 1, "HSET", docid_str]
    else:
        cmd = ["WRITE", "W1", 2, "FT.ADD", indexname, docid_str, "1.0", "FIELDS"]
    for x in fields:
        cmd.append(x)
    return cmd


def getQueryWords(doc, stop_words, size):
    words = doc["abstract"]
    words = re.sub("[^0-9a-zA-Z]+", " ", words)
    words = words.split(" ")
    queryWords = []
    totalQueryWords = 0
    for word in words:
        word = word.lstrip().rstrip()
        if len(word) > 3 and word not in stop_words and word != "Wikipedia":
            queryWords.append(word)
            totalQueryWords = totalQueryWords + 1

        if totalQueryWords > size:
            break
    return queryWords, totalQueryWords


def generate_benchmark_commands(
    total_benchmark_commands,
    bench_fname,
    all_fname,
    indexname,
    docs,
    stop_words,
    search_no_content,
    query_choices,
):
    all_csvfile = open(all_fname, "a", newline="")
    bench_csvfile = open(bench_fname, "w", newline="")
    all_csv_writer = csv.writer(all_csvfile, delimiter=",", quoting=csv.QUOTE_ALL)
    bench_csv_writer = csv.writer(bench_csvfile, delimiter=",", quoting=csv.QUOTE_ALL)
    progress = tqdm(unit="docs", total=total_benchmark_commands)
    total_docs = len(docs)
    generated_commands = 0
    while generated_commands < total_benchmark_commands:
        random_doc_pos = random.randint(0, total_docs - 1)
        doc = docs[random_doc_pos]
        words, totalW = getQueryWords(doc, stop_words, 2)
        choice = random.choices(query_choices)[0]
        if len(words) < 1:
            continue
        term = words[0]
        len_w1 = len(term)
        prefix_min = 3
        prefix_max = 3
        generated_row = None
        if choice == SIMPLE_WORD_QUERY and len(words) >= 1:
            generated_row = generate_ft_search_row(
                indexname, SIMPLE_WORD_QUERY, words[0], search_no_content
            )
        elif choice == WILDCARD_QUERY and len(term) >= prefix_max:
            generated_row = generate_wildcard_row(
                indexname,
                WILDCARD_QUERY,
                term,
                prefix_min,
                prefix_max,
                search_no_content,
            )
        elif choice == PREFIX_QUERY and len(term) >= prefix_max:
            generated_row = generate_prefix_row(
                indexname,
                PREFIX_QUERY,
                term,
                prefix_min,
                prefix_max,
                search_no_content,
            )
        elif choice == SUFFIX_QUERY and len(term) >= prefix_max:
            generated_row = generate_suffix_row(
                indexname,
                SUFFIX_QUERY,
                term,
                prefix_min,
                prefix_max,
                search_no_content,
            )
        elif choice == CONTAINS_QUERY and len(term) >= prefix_max:
            generated_row = generate_contains_row(
                indexname,
                CONTAINS_QUERY,
                term,
                prefix_min,
                prefix_max,
                search_no_content,
            )
        elif choice == SIMPLE_2WORD_UNION_QUERY and len(words) >= 2:
            generated_row = generate_ft_search_row(
                indexname,
                SIMPLE_2WORD_UNION_QUERY,
                "{} {}".format(words[0], words[1]),
                search_no_content,
            )
        elif choice == SIMPLE_2WORD_INT_QUERY and len(words) >= 2:
            generated_row = generate_ft_search_row(
                indexname,
                SIMPLE_2WORD_INT_QUERY,
                "{}|{}".format(words[0], words[1]),
                search_no_content,
            )
        if generated_row != None:
            all_csv_writer.writerow(generated_row)
            bench_csv_writer.writerow(generated_row)
            progress.update()
            generated_commands = generated_commands + 1
    progress.close()
    bench_csvfile.close()
    all_csvfile.close()


def generate_wildcard_row(
    index, query_name, query, prefix_min, prefix_max, search_no_content
):
    if (prefix_max - 2) <= prefix_min:
        prefix_max = prefix_min + 2
    term = query[:prefix_min] + "*" + query[prefix_min + 1 : prefix_max]
    cmd = [
        "READ",
        query_name,
        1,
        "FT.SEARCH",
        "{index}".format(index=index),
        "{query}".format(query=term),
    ]
    if search_no_content:
        cmd.append("NOCONTENT")
    return cmd


def generate_prefix_row(
    index, query_name, query, prefix_min, prefix_max, search_no_content
):
    term = query[:prefix_min] + "*"
    cmd = [
        "READ",
        query_name,
        1,
        "FT.SEARCH",
        "{index}".format(index=index),
        "{query}".format(query=term),
    ]
    if search_no_content:
        cmd.append("NOCONTENT")
    return cmd


def generate_suffix_row(
    index, query_name, query, prefix_min, prefix_max, search_no_content
):
    term = "*" + query[:prefix_min]
    cmd = [
        "READ",
        query_name,
        1,
        "FT.SEARCH",
        "{index}".format(index=index),
        "{query}".format(query=term),
    ]
    if search_no_content:
        cmd.append("NOCONTENT")
    return cmd


def generate_contains_row(
    index, query_name, query, prefix_min, prefix_max, search_no_content
):
    term = "*" + query[:prefix_min] + "*"
    cmd = [
        "READ",
        query_name,
        1,
        "FT.SEARCH",
        "{index}".format(index=index),
        "{query}".format(query=term),
    ]
    if search_no_content:
        cmd.append("NOCONTENT")
    return cmd


def generate_ft_search_row(index, query_name, query, search_no_content):
    cmd = [
        "READ",
        query_name,
        1,
        "FT.SEARCH",
        "{index}".format(index=index),
        "{query}".format(query=query),
    ]
    if search_no_content:
        cmd.append("NOCONTENT")
    return cmd


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="RediSearch FTSB data generator.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--project", type=str, default="redisearch", help="the project being tested"
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=12345,
        help="the random seed used to generate random deterministic outputs",
    )
    parser.add_argument(
        "--doc-limit",
        type=int,
        default=1000000,
        help="the total documents to generate to be added in the setup stage",
    )
    parser.add_argument(
        "--total-benchmark-commands",
        type=int,
        default=1000000,
        help="the total commands to generate to be issued in the benchmark stage",
    )
    parser.add_argument(
        "--stop-words",
        type=str,
        default="a,is,the,an,and,are,as,at,be,but,by,for,if,in,into,it,no,not,of,on,or,such,that,their,then,there,these,they,this,to,was,will,with",
        help="When searching, stop-words are ignored and treated as if they were not sent to the query processor. Therefore, to be 100% correct we need to prevent those words to enter a query",
    )
    parser.add_argument(
        "--index-name",
        type=str,
        default="enwiki_abstract",
        help="the name of the RediSearch index to be used",
    )
    parser.add_argument(
        "--test-name",
        type=str,
        default="1M-enwiki_abstract-hashes",
        help="the name of the test",
    )
    parser.add_argument(
        "--test-description",
        type=str,
        default="benchmark focused on full text search queries performance, making usage of English-language Wikipedia:Database page abstracts",
        help="the full description of the test",
    )
    parser.add_argument(
        "--query-choices",
        type=str,
        default=",".join(
            [
                SIMPLE_WORD_QUERY,
                SIMPLE_2WORD_UNION_QUERY,
                SIMPLE_2WORD_INT_QUERY,
            ]
        ),
        help="comma separated list of queries to produce. one of: {}".format(
            [
                SIMPLE_WORD_QUERY,
                SIMPLE_2WORD_UNION_QUERY,
                SIMPLE_2WORD_INT_QUERY,
                PREFIX_QUERY,
                SUFFIX_QUERY,
                CONTAINS_QUERY,
                WILDCARD_QUERY,
            ]
        ),
    )
    parser.add_argument(
        "--upload-artifacts-s3-uncompressed",
        action="store_true",
        help="uploads the generated dataset files and configuration file to public benchmarks.redislabs bucket. Proper credentials are required",
    )

    parser.add_argument(
        "--upload-artifacts-s3",
        default=False,
        action="store_true",
        help="uploads the generated dataset files and configuration file to public benchmarks.redislabs bucket. Proper credentials are required",
    )
    parser.add_argument(
        "--use-ftadd",
        default=False,
        action="store_true",
        help="Use FT.ADD instead of HSET",
    )
    parser.add_argument(
        "--search-no-content",
        default=False,
        action="store_true",
        help="When doing full text search queries, only return the document ids and not the content",
    )
    parser.add_argument(
        "--temporary-work-dir",
        type=str,
        default="./tmp",
        help="The temporary dir to use as working directory for file download, compression,etc... ",
    )

    args = parser.parse_args()
    use_case_specific_arguments = del_non_use_case_specific_keys(dict(args.__dict__))

    # generate the temporary working dir if required
    working_dir = args.temporary_work_dir
    Path(working_dir).mkdir(parents=True, exist_ok=True)
    seed = args.seed
    project = args.project
    doc_limit = args.doc_limit
    stop_words = args.stop_words.split(",")
    indexname = args.index_name
    test_name = args.test_name
    search_no_content = args.search_no_content
    query_choices = args.query_choices.split(",")
    if search_no_content:
        test_name += "-search-no-content"
    description = args.test_description
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
    all_fname = "{}.ALL.csv".format(benchmark_output_file)
    all_fname_compressed = "{}.ALL.tar.gz".format(benchmark_output_file)

    all_fname = "{}.ALL.csv".format(benchmark_output_file)
    setup_fname = "{}.SETUP.csv".format(benchmark_output_file)
    bench_fname = "{}.BENCH.QUERY_{}.csv".format(
        benchmark_output_file, "__".join(query_choices)
    )
    all_fname_compressed = "{}.ALL.tar.gz".format(benchmark_output_file)
    setup_fname_compressed = "{}.SETUP.tar.gz".format(benchmark_output_file)
    bench_fname_compressed = "{}.BENCH.tar.gz".format(benchmark_output_file)
    remote_url_all = "{}{}".format(s3_uri, all_fname_compressed)
    remote_url_setup = "{}{}".format(s3_uri, setup_fname_compressed)
    remote_url_bench = "{}{}".format(s3_uri, bench_fname_compressed)

    ## remove previous files if they exist
    all_artifacts = [
        all_fname,
        setup_fname,
        bench_fname,
        all_fname_compressed,
        setup_fname_compressed,
        bench_fname_compressed,
        benchmark_config_file,
    ]
    for artifact in all_artifacts:
        remove_file_if_exists(artifact)

    use_ftadd = args.use_ftadd
    total_benchmark_commands = args.total_benchmark_commands

    used_indices = [indexname]
    setup_commands = []
    teardown_commands = []
    key_metrics = []

    add_key_metric(
        key_metrics,
        "setup",
        "throughput",
        "OverallRates.overallOpsRate",
        "Overall writes query rate",
        "docs/sec",
        "numeric",
        "higher-better",
        1,
    )
    add_key_metric(
        key_metrics,
        "setup",
        "latency",
        "OverallQuantiles.allCommands.q50",
        "Overall writes query q50 latency",
        "ms",
        "numeric",
        "lower-better",
        2,
    )

    add_key_metric(
        key_metrics,
        "benchmark",
        "throughput",
        "OverallRates.overallOpsRate",
        "Overall writes query rate",
        "docs/sec",
        "numeric",
        "higher-better",
        1,
    )
    add_key_metric(
        key_metrics,
        "benchmark",
        "latency",
        "OverallQuantiles.allCommands.q50",
        "Overall writes query q50 latency",
        "ms",
        "numeric",
        "lower-better",
        2,
    )

    total_writes = 0
    total_reads = 0
    total_updates = 0
    total_deletes = 0

    json_version = "0.1"
    benchmark_repetitions_require_teardown_and_resetup = False

    print("-- Benchmark: {} -- ".format(test_name))
    print("-- Description: {} -- ".format(description))

    total_docs = 0

    print("Using random seed {0}".format(args.seed))
    random.seed(args.seed)

    print("Using the following stop-words: {0}".format(stop_words))

    index_types = generate_enwiki_abstract_index_type()
    print("-- generating the ft.create commands -- ")
    ft_create_cmd = generate_ft_create_row(indexname, index_types, use_ftadd)
    print("FT.CREATE command: {}".format(" ".join(ft_create_cmd)))
    setup_commands.append(ft_create_cmd)

    print("-- generating the ft.drop commands -- ")
    ft_drop_cmd = generate_ft_drop_row(indexname)
    teardown_commands.append(ft_drop_cmd)

    csv_filenames = []
    print(
        "Retrieving the required English-language Wikipedia:Database page abstracts data"
    )

    if path.exists(filename) is False:
        print("Downloading {} to {}".format(origin, filename))
        download_url(origin, filename)
    else:
        print("{} exists, no need to download again".format(filename))

    if path.exists(decompressed_fname) is False:
        print("Decompressing {}".format(filename))
        decompress_file(filename)

    docs = []
    tree = ET.iterparse(decompressed_fname)
    print("Reading {}\n".format(decompressed_fname))
    progress = tqdm(unit="docs")
    total_produced = 0
    for event, elem in tree:
        if elem.tag == "doc":
            doc = {}
            total_docs = total_docs + 1
            doc["title"] = elem.findtext("title")
            doc["url"] = elem.findtext("url")
            doc["abstract"] = elem.findtext("abstract")
            docs.append(doc)
            progress.update()
            total_produced = total_produced + 1
            if total_produced >= doc_limit and doc_limit > 0:
                print("stopping doc read process")
                break
            elem.clear()  # won't need the children any more
    progress.close()

    print("\n")
    setup_csvfile = open(setup_fname, "w", newline="")
    all_csvfile = open(all_fname, "a", newline="")
    all_csv_writer = csv.writer(all_csvfile, delimiter=",", quoting=csv.QUOTE_ALL)
    setup_csv_writer = csv.writer(setup_csvfile, delimiter=",", quoting=csv.QUOTE_ALL)
    print("\n")
    print("-- generating the setup commands -- \n")
    progress = tqdm(unit="docs", total=args.doc_limit)
    doc_limit = args.doc_limit
    total_docs = 0
    if doc_limit == 0:
        doc_limit = len(docs)
    while total_docs < doc_limit:
        total_docs = total_docs + 1
        random_doc_pos = random.randint(0, len(docs) - 1)
        doc = docs[random_doc_pos]
        cmd = use_case_to_cmd(
            use_ftadd, doc["title"], doc["url"], doc["abstract"], total_docs
        )
        progress.update()
        setup_csv_writer.writerow(cmd)
        all_csv_writer.writerow(cmd)

    progress.close()
    all_csvfile.close()
    setup_csvfile.close()

    print(
        "-- generating {} full text search commands -- ".format(
            total_benchmark_commands
        )
    )
    print("\t saving to {} and {}".format(bench_fname, all_fname))
    generate_benchmark_commands(
        total_benchmark_commands,
        bench_fname,
        all_fname,
        indexname,
        docs,
        stop_words,
        search_no_content,
        query_choices,
    )

    total_commands = total_docs
    total_setup_commands = total_docs
    cmd_category_all = {
        "setup-writes": total_docs,
        "writes": total_writes,
        "updates": total_updates,
        "reads": total_reads,
        "deletes": total_deletes,
    }
    cmd_category_setup = {
        "setup-writes": total_docs,
        "writes": 0,
        "updates": 0,
        "reads": 0,
        "deletes": 0,
    }
    cmd_category_benchmark = {
        "setup-writes": 0,
        "writes": total_writes,
        "updates": total_updates,
        "reads": total_reads,
        "deletes": total_deletes,
    }

    status, uncompressed_size, compressed_size = compress_files(
        [all_fname], all_fname_compressed
    )
    inputs_entry_all = generate_inputs_dict_item(
        "all",
        all_fname,
        "contains both setup and benchmark commands",
        remote_url_all,
        uncompressed_size,
        all_fname_compressed,
        compressed_size,
        total_commands,
        cmd_category_all,
    )

    status, uncompressed_size, compressed_size = compress_files(
        [setup_fname], setup_fname_compressed
    )
    inputs_entry_setup = generate_inputs_dict_item(
        "setup",
        setup_fname,
        "contains only the commands required to populate the dataset",
        remote_url_setup,
        uncompressed_size,
        setup_fname_compressed,
        compressed_size,
        total_setup_commands,
        cmd_category_setup,
    )

    status, uncompressed_size, compressed_size = compress_files(
        [bench_fname], bench_fname_compressed
    )
    inputs_entry_benchmark = generate_inputs_dict_item(
        "benchmark",
        bench_fname,
        "contains only the benchmark commands (requires the dataset to have been previously populated)",
        remote_url_bench,
        uncompressed_size,
        bench_fname_compressed,
        compressed_size,
        total_benchmark_commands,
        cmd_category_benchmark,
    )

    inputs = {
        "all": inputs_entry_all,
        "setup": inputs_entry_setup,
        "benchmark": inputs_entry_benchmark,
    }

    deployment_requirements = init_deployment_requirement()
    add_deployment_requirements_redis_server_module(
        deployment_requirements, "search", {}
    )
    add_deployment_requirements_utilities(
        deployment_requirements, "ftsb_redisearch", {}
    )
    add_deployment_requirements_benchmark_tool(
        deployment_requirements, "ftsb_redisearch"
    )

    run_stages = ["benchmark"]

    run_stages = ["setup", "benchmark"]
    with open(benchmark_config_file, "w") as setupf:
        setup_json = generate_setup_json(
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
            used_indices,
            total_commands,
            total_setup_commands,
            total_benchmark_commands,
            total_docs,
            total_writes,
            total_updates,
            total_reads,
            total_deletes,
            benchmark_repetitions_require_teardown_and_resetup,
            ["setup"],
            ["benchmark"],
        )

        json.dump(setup_json, setupf, indent=2)

    if args.upload_artifacts_s3:
        artifacts = [
            benchmark_config_file,
            all_fname_compressed,
            setup_fname_compressed,
            bench_fname_compressed,
        ]
        upload_dataset_artifacts_s3(s3_bucket_name, s3_bucket_path, artifacts)
    if args.upload_artifacts_s3_uncompressed:
        artifacts = [setup_fname, bench_fname]
        upload_dataset_artifacts_s3(s3_bucket_name, s3_bucket_path, artifacts)

    print("############################################")
    print("All artifacts generated.")
