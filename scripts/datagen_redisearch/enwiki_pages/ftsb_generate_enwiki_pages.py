import re
import xml.etree.ElementTree as ET
from os import path
from collections import Counter

import argparse
import csv
import json
import os
import random

# package local imports
import sys
import uuid
import matplotlib.pyplot as plt
import math
from dateutil.parser import parse
from tdigest import TDigest

import numpy as np
import boto3
from tqdm import tqdm

# package local imports
sys.path.append(os.getcwd() + "/..")
field_tokenization = ",.<>{}[]\"':;!@#$%^&*()-+=~"

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

origin = "https://dumps.wikimedia.org/enwiki/20210501/enwiki-20210501-pages-articles1.xml-p1p41242.bz2"
filename = "enwiki-20210501-pages-articles1.xml-p1p41242.bz2"
decompressed_fname = "enwiki-20210501-pages-articles1.xml-p1p41242"


def generate_enwiki_pages_index_type():
    types = {}
    for f in ["title", "text", "comment"]:
        types[f] = "text"
    for f in ["username"]:
        types[f] = "tag"
    for f in ["timestamp"]:
        types[f] = "numeric"
    return types


def generate_lognormal_dist(n_elements):
    mu, sigma = 0.0, 1
    s = np.random.lognormal(mu, sigma, n_elements)

    min_s = min(s)
    max_s = max(s)

    diff = max_s - min_s
    s = s - min_s
    s = s / diff
    return s


def generate_ft_create_row(index, index_types, use_ftadd, no_index_list):
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
        if f in no_index_list:
            cmd.append('"NOINDEX"')
        else:
            cmd.append("SORTABLE")
            cmd.append('"SORTABLE"')
    return cmd


def generate_ft_drop_row(index):
    cmd = ["FT.DROP", "{index}".format(index=index), "DD"]
    return cmd


def EscapeTextFileString(field):
    for char_escape in field_tokenization:
        field = field.replace(char_escape, "\\{}".format(char_escape))
    field = field.replace("\n", " \\n")
    return field


def use_case_to_cmd(use_ftadd, title, text, comment, username, timestamp, total_docs):
    escaped_title = EscapeTextFileString(title)
    escaped_text = EscapeTextFileString(text)
    escaped_comment = EscapeTextFileString(comment)
    size = len(escaped_title) + len(escaped_text) + len(escaped_comment) + len(username)
    unprunned_hash = {
        "title": title,
        "text": text,
        "comment": comment,
        "username": username,
        "timestamp": timestamp,
    }
    # print(len(text),size)
    hash = {
        "title": escaped_title,
        "text": escaped_text,
        "comment": escaped_comment,
        "username": username,
        "timestamp": timestamp,
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
    return cmd, size


def getQueryWords(doc, stop_words, size):
    words = doc["comment"]
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
    use_numeric_range_searchs,
    ts_digest,
    p_writes,
    query_choices,
):
    total_benchmark_reads = 0
    total_benchmark_writes = 0
    all_csvfile = open(all_fname, "a", newline="")
    bench_csvfile = open(bench_fname, "w", newline="")
    all_csv_writer = csv.writer(all_csvfile, delimiter=",", quoting=csv.QUOTE_ALL)
    bench_csv_writer = csv.writer(bench_csvfile, delimiter=",", quoting=csv.QUOTE_ALL)
    progress = tqdm(unit="docs", total=total_benchmark_commands)
    total_docs = len(docs)

    ## timestamp related
    timestamps_pdist = generate_lognormal_dist(total_benchmark_commands)
    min_ts = ts_digest.percentile(0.0)
    max_ts = ts_digest.percentile(100.0)
    query_range_digest = TDigest()

    generated_commands = 0
    while generated_commands < total_benchmark_commands:
        query_ts_pdist = timestamps_pdist[generated_commands]
        percentile = (1.0 - query_ts_pdist) * 100.0
        query_min_ts = ts_digest.percentile(percentile)

        random_doc_pos = random.randint(0, total_docs - 1)
        doc = docs[random_doc_pos]
        # decide read or write
        p_cmd = random.random()
        if p_cmd < p_writes:
            ## WRITE
            total_benchmark_writes = total_benchmark_writes + 1
            generated_row, doc_size = use_case_to_cmd(
                use_ftadd,
                doc["title"],
                doc["text"],
                doc["comment"],
                doc["username"],
                doc["timestamp"],
                generated_commands,
            )

        else:
            ## READ
            total_benchmark_reads = total_benchmark_reads + 1
            words, totalW = getQueryWords(doc, stop_words, 2)

            choice = random.choices(query_choices)[0]
            generated_row = None
            numeric_range_str = ""
            if use_numeric_range_searchs:
                numeric_range_str = "@timestamp:[{} {}] ".format(query_min_ts, max_ts)
                query_range_digest.update(int(max_ts - query_min_ts))
            if choice == "simple-1word-query" and len(words) >= 1:
                generated_row = generate_ft_search_row(
                    indexname,
                    "simple-1word-query",
                    "{}{}".format(numeric_range_str, words[0]),
                )
            elif choice == "2word-union-query" and len(words) >= 2:
                generated_row = generate_ft_search_row(
                    indexname,
                    "2word-union-query",
                    "{}{} {}".format(numeric_range_str, words[0], words[1]),
                )
            elif choice == "2word-intersection-query" and len(words) >= 2:
                generated_row = generate_ft_search_row(
                    indexname,
                    "2word-intersection-query",
                    "{}{}|{}".format(numeric_range_str, words[0], words[1]),
                )
        if generated_row != None:
            #             all_csv_writer.writerow(generated_row)
            #             bench_csv_writer.writerow(generated_row)
            progress.update()
            generated_commands = generated_commands + 1
    progress.close()
    bench_csvfile.close()
    all_csvfile.close()

    #     print()
    xx = []
    yy = []
    p90 = query_range_digest.percentile(90.0)
    dataset_percent = ts_digest.cdf(p90)

    print(
        "90% of the read queries target at max {} percent o keyspace".format(
            dataset_percent
        )
    )
    print(
        "100% of the read queries target at max {} percent o keyspace".format(
            ts_digest.cdf(max_ts - min_ts)
        )
    )
    for centroid in query_range_digest.centroids_to_list():
        ts_m = centroid["m"]
        xx.append(ts_m)
        yy.append(query_range_digest.cdf(ts_m))
    plt.scatter(xx, yy)

    plt.title("EnWiki pages Query time range")
    plt.xlabel("Query time range")
    plt.ylabel("cdf")
    plt.xscale("log")
    plt.show()

    return total_benchmark_reads, total_benchmark_writes


def generate_ft_search_row(index, query_name, query):
    cmd = [
        "READ",
        query_name,
        1,
        "FT.SEARCH",
        "{index}".format(index=index),
        "{query}".format(query=query),
    ]
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
        "--read-ratio", type=int, default=10, help="query time read ratio"
    )
    parser.add_argument(
        "--write-ratio", type=int, default=1, help="query time write ratio"
    )
    parser.add_argument(
        "--min-doc-len",
        type=int,
        default=1024,
        help="Discard any generated document bellow the specified value",
    )
    parser.add_argument(
        "--doc-limit",
        type=int,
        default=100000,
        help="the total documents to generate to be added in the setup stage",
    )
    parser.add_argument(
        "--total-benchmark-commands",
        type=int,
        default=100000,
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
        default="enwiki_pages",
        help="the name of the RediSearch index to be used",
    )
    parser.add_argument(
        "--test-name",
        type=str,
        default="100K-enwiki_pages-hashes",
        help="the name of the test",
    )
    parser.add_argument(
        "--test-description",
        type=str,
        default="benchmark focused on full text search queries performance, making usage of English-language Wikipedia:Database page revisions",
        help="the full description of the test",
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
        "--query-use-ts-numeric-range-filter",
        default=False,
        action="store_true",
        help="Use a numeric range filter on queries to simulate searchs that imply a log-normal keyspace access (very hot data and some cold data)",
    )
    parser.add_argument(
        "--big-text-field-noindex",
        default=False,
        action="store_true",
        help="On index creation mark the largest text field as no index. If a field has NOINDEX and doesn't have SORTABLE, it will just be ignored by the index. This is usefull to test RoF for example.",
    )
    parser.add_argument(
        "--temporary-work-dir",
        type=str,
        default="./tmp",
        help="The temporary dir to use as working directory for file download, compression,etc... ",
    )
    parser.add_argument(
        "--query-choices",
        type=str,
        default="simple-1word-query,2word-union-query,2word-intersection-query",
        help="comma separated list of queries to produce. one of: simple-1word-query,2word-union-query,2word-intersection-query",
    )
    parser.add_argument(
        "--upload-artifacts-s3-uncompressed",
        action="store_true",
        help="uploads the generated dataset files and configuration file to public benchmarks.redislabs bucket. Proper credentials are required",
    )
    args = parser.parse_args()
    query_choices = args.query_choices.split(",")
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
    use_numeric_range_searchs = args.query_use_ts_numeric_range_filter
    no_index_list = []
    big_text_field_noindex = args.big_text_field_noindex
    if big_text_field_noindex:
        test_name += "-big-text-field-noindex"
        no_index_list = ["text"]
    if use_numeric_range_searchs:
        test_name += "-lognormal-numeric-range-searchs"
    min_doc_len = args.min_doc_len
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
    bench_fname = "{}.BENCH.QUERY_{}_write_{}_to_read_{}.csv".format(
        benchmark_output_file,
        "__".join(query_choices),
        args.write_ratio,
        args.read_ratio,
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
    # 1:10
    p_writes = float(args.write_ratio) / (
        float(args.read_ratio) + float(args.write_ratio)
    )

    json_version = "0.1"
    benchmark_repetitions_require_teardown_and_resetup = False

    print("-- Benchmark: {} -- ".format(test_name))
    print("-- Description: {} -- ".format(description))

    total_docs = 0

    print("Using random seed {0}".format(args.seed))
    random.seed(args.seed)

    print("Using the following stop-words: {0}".format(stop_words))

    index_types = generate_enwiki_pages_index_type()
    print("-- generating the ft.create commands -- ")
    ft_create_cmd = generate_ft_create_row(
        indexname, index_types, use_ftadd, no_index_list
    )
    print("FT.CREATE command: {}".format(" ".join(ft_create_cmd)))

    setup_commands.append(ft_create_cmd)

    print("-- generating the ft.drop commands -- ")
    ft_drop_cmd = generate_ft_drop_row(indexname)
    teardown_commands.append(ft_drop_cmd)

    csv_filenames = []
    print(
        "Retrieving the required English-language Wikipedia:Database page edition data"
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

    doc = {}
    text = None
    comment = None
    username = None
    timestamp = None
    ts_digest = TDigest()
    for event, elem in tree:
        if elem.tag == "{http://www.mediawiki.org/xml/export-0.10/}page":
            doc = {}
            doc["title"] = elem.findtext(
                "{http://www.mediawiki.org/xml/export-0.10/}title"
            )
            doc["text"] = text
            doc["comment"] = comment
            doc["username"] = username
            doc["timestamp"] = int(timestamp)
            ts_digest.update(int(timestamp))
            if (
                doc["text"] is not None
                and doc["comment"] is not None
                and doc["username"] is not None
                and doc["timestamp"] is not None
            ):
                total_docs = total_docs + 1
                docs.append(doc)
                progress.update()
                elem.clear()  # won't need the children any more
        if elem.tag == "{http://www.mediawiki.org/xml/export-0.10/}revision":
            text = elem.findtext("{http://www.mediawiki.org/xml/export-0.10/}text")
            comment = elem.findtext(
                "{http://www.mediawiki.org/xml/export-0.10/}comment"
            )
            ts = elem.findtext("{http://www.mediawiki.org/xml/export-0.10/}timestamp")
            dt = parse(ts)
            timestamp = dt.timestamp()
        if elem.tag == "{http://www.mediawiki.org/xml/export-0.10/}contributor":
            username = elem.findtext(
                "{http://www.mediawiki.org/xml/export-0.10/}username"
            )

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
    docs_sizes = []
    total_docs = 0
    if doc_limit == 0:
        doc_limit = len(docs)
    while total_docs < doc_limit:

        random_doc_pos = random.randint(0, len(docs) - 1)
        doc = docs[random_doc_pos]
        cmd, doc_size = use_case_to_cmd(
            use_ftadd,
            doc["title"],
            doc["text"],
            doc["comment"],
            doc["username"],
            doc["timestamp"],
            total_docs,
        )
        if doc_size >= min_doc_len:
            total_docs = total_docs + 1
            docs_sizes.append(doc_size)
            progress.update()
    #             setup_csv_writer.writerow(cmd)
    #             all_csv_writer.writerow(cmd)
    # fixed bin size
    bins = np.linspace(
        math.ceil(min(docs_sizes)), math.floor(max(docs_sizes)), 200
    )  # fixed number of bins

    plt.xlim([1, max(docs_sizes) + 5])

    plt.hist(docs_sizes, bins=bins, alpha=0.5)
    plt.title(
        "EnWiki pages document size frequency. Avg document size: {} Bytes".format(
            int(np.average(docs_sizes))
        )
    )
    plt.xlabel("Document Size in Bytes")
    plt.ylabel("count")
    plt.xscale("log")

    plt.show()

    xx = []
    yy = []

    for centroid in ts_digest.centroids_to_list():
        # print(centroid)
        ts_m = centroid["m"]
        xx.append(ts_m)
        yy.append(ts_digest.cdf(ts_m))
    plt.scatter(xx, yy)

    plt.title("EnWiki pages timestamp range")
    plt.xlabel("timestamp")
    plt.ylabel("cdf")
    #     plt.xscale('log')
    plt.show()

    progress.close()
    all_csvfile.close()
    setup_csvfile.close()

    print(
        "-- generating {} full text search commands -- ".format(
            total_benchmark_commands
        )
    )
    print("\t saving to {} and {}".format(bench_fname, all_fname))
    total_benchmark_reads, total_benchmark_writes = generate_benchmark_commands(
        total_benchmark_commands,
        bench_fname,
        all_fname,
        indexname,
        docs,
        stop_words,
        use_numeric_range_searchs,
        ts_digest,
        p_writes,
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
        "writes": total_benchmark_writes,
        "updates": total_updates,
        "reads": total_benchmark_reads,
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
