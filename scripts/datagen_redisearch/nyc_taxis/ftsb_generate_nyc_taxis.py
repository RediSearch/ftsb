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


def generate_nyc_taxis_index_type():
    types = {}
    for f in [
        "vendor_id",
        "payment_type",
        "trip_type",
        "rate_code_id",
        "store_and_fwd_flag",
    ]:
        types[f] = "tag"
    for f in ["pickup_datetime", "dropoff_datetime"]:
        types[f] = "text"
    for f in ["pickup_location_long_lat", "dropoff_location_long_lat"]:
        types[f] = "geo"
    for f in [
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "mta_tax",
        "extra",
        "improvement_surcharge",
        "tip_amount",
        "tolls_amount",
        "total_amount",
    ]:
        types[f] = "numeric"
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


def use_case_csv_row_to_cmd(
    row,
    index_types,
    use_ftadd,
    total_amount_pos,
    improvement_surcharge_pos,
    pickup_longitude_pos,
    pickup_latitude_pos,
    pickup_datetime_pos,
    dropoff_datetime_pos,
    rate_code_id_pos,
    tolls_amount_pos,
    dropoff_longitude_pos,
    dropoff_latitude_pos,
    passenger_count_pos,
    fare_amount_pos,
    extra_pos,
    trip_distance_pos,
    tip_amount_pos,
    store_and_fwd_flag_pos,
    payment_type_pos,
    mta_tax_pos,
    vendor_id_pos,
):
    pickup_location_long = (
        None
        if pickup_longitude_pos is None or pickup_longitude_pos > (len(row) - 1)
        else str_to_float_or_zero(row[pickup_longitude_pos])
    )
    pickup_location_lat = (
        None
        if pickup_latitude_pos is None or pickup_latitude_pos > (len(row) - 1)
        else str_to_float_or_zero(row[pickup_latitude_pos])
    )
    if pickup_location_lat is not None and (
        pickup_location_lat < -85.05112878 or pickup_location_lat > 85.05112878
    ):
        pickup_location_lat = None
    if pickup_location_long is not None and (
        pickup_location_long < -180.0 or pickup_location_long > 180
    ):
        pickup_location_long = None

    dropoff_location_long = (
        None
        if dropoff_longitude_pos is None or dropoff_longitude_pos > (len(row) - 1)
        else str_to_float_or_zero(row[dropoff_longitude_pos])
    )
    dropoff_location_lat = (
        None
        if dropoff_latitude_pos is None or dropoff_latitude_pos > (len(row) - 1)
        else str_to_float_or_zero(row[dropoff_latitude_pos])
    )
    if dropoff_location_lat is not None and (
        dropoff_location_lat < -85.05112878 or dropoff_location_lat > 85.05112878
    ):
        dropoff_location_lat = None
    if dropoff_location_long is not None and (
        dropoff_location_long < -180.0 or dropoff_location_long > 180
    ):
        dropoff_location_long = None

    hash = {
        "total_amount": None
        if total_amount_pos is None
        else str_to_float_or_zero(row[total_amount_pos]),
        "improvement_surcharge": None
        if improvement_surcharge_pos is None
        else str_to_float_or_zero(row[improvement_surcharge_pos]),
        "pickup_location_long_lat": None
        if (pickup_location_long is None or pickup_location_lat is None)
        else "{:.6f},{:.6f}".format(pickup_location_long, pickup_location_lat),
        "pickup_datetime": None
        if pickup_datetime_pos is None
        else '"{}"'.format(row[pickup_datetime_pos]),
        "trip_type": "1",
        "dropoff_datetime": None
        if dropoff_datetime_pos is None
        else '"{}"'.format(row[dropoff_datetime_pos]),
        "rate_code_id": None if rate_code_id_pos is None else row[rate_code_id_pos],
        "tolls_amount": None
        if tolls_amount_pos is None
        else str_to_float_or_zero(row[tolls_amount_pos]),
        "dropoff_location_long_lat": None
        if (dropoff_location_long is None or dropoff_location_lat is None)
        else "{:.6f},{:.6f}".format(dropoff_location_long, dropoff_location_lat),
        "passenger_count": None
        if passenger_count_pos is None
        else row[passenger_count_pos],
        "fare_amount": None
        if fare_amount_pos is None
        else str_to_float_or_zero(row[fare_amount_pos]),
        "extra": None if extra_pos is None else str_to_float_or_zero(row[extra_pos]),
        "trip_distance": None
        if trip_distance_pos is None
        else str_to_float_or_zero(row[trip_distance_pos]),
        "tip_amount": None
        if tip_amount_pos is None
        else str_to_float_or_zero(row[tip_amount_pos]),
        "store_and_fwd_flag": None
        if store_and_fwd_flag_pos is None
        else row[store_and_fwd_flag_pos],
        "payment_type": None if payment_type_pos is None else row[payment_type_pos],
        "mta_tax": None
        if mta_tax_pos is None
        else str_to_float_or_zero(row[mta_tax_pos]),
        "vendor_id": None if vendor_id_pos is None else row[vendor_id_pos],
    }
    docid_str = "doc:{hash}:{n}".format(hash=uuid.uuid4().hex, n=total_docs)
    # for k in hash.keys():
    #     assert k in index_types.keys()

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


def human_format(num):
    magnitude = 0
    while abs(num) >= 1000:
        magnitude += 1
        num /= 1000.0
    # add more suffixes if you need them
    return "%.0f%s" % (num, ["", "K", "M", "G", "T", "P"][magnitude])


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
        default=10000000,
        help="the total documents to generate to be added in the setup stage",
    )
    parser.add_argument(
        "--yellow-tripdata-start-year",
        type=int,
        default=2015,
        help="the start year of the yellow trip data to fetch",
    )
    parser.add_argument(
        "--yellow-tripdata-end-year",
        type=int,
        default=2015,
        help="the end year of the yellow trip data to fetch",
    )
    parser.add_argument(
        "--yellow-tripdata-start-month",
        type=int,
        default=1,
        help="the start month of the yellow trip data to fetch",
    )
    parser.add_argument(
        "--yellow-tripdata-end-month",
        type=int,
        default=1,
        help="the start month of the yellow trip data to fetch",
    )
    parser.add_argument(
        "--index-name",
        type=str,
        default="nyc_taxis",
        help="the name of the RediSearch index to be used",
    )
    parser.add_argument(
        "--test-name",
        type=str,
        default="nyc_taxis-hashes",
        help="the name of the test",
    )
    parser.add_argument(
        "--test-description",
        type=str,
        default="benchmark focused on write performance, making usage of TLC Trip Record Data that contains the rides that have been performed in yellow taxis in New York in 2015",
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
        "--use-ftadd",
        default=False,
        action="store_true",
        help="Use FT.ADD instead of HSET",
    )
    parser.add_argument(
        "--nyc-tlc-s3-bucket-prefix",
        type=str,
        default="https://s3.amazonaws.com/nyc-tlc/trip+data",
        help="The s3 bucket prefix to fetch the input files containing the origin CSV datasets to read the data from.",
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
    indexname = args.index_name
    test_name = args.test_name
    description = args.test_description
    use_ftadd = args.use_ftadd
    if use_ftadd:
        test_name = "nyc_taxis-ftadd"
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
    all_fname = "{}.ALL.csv".format(benchmark_output_file)
    all_fname_compressed = "{}.ALL.tar.gz".format(benchmark_output_file)
    remote_url_all = "{}{}".format(s3_uri, all_fname_compressed)

    ## remove previous files if they exist
    remove_file_if_exists(benchmark_config_file)
    remove_file_if_exists(all_fname)
    remove_file_if_exists(all_fname_compressed)

    start_y = args.yellow_tripdata_start_year
    end_y = args.yellow_tripdata_end_year
    start_m = args.yellow_tripdata_start_month
    end_m = args.yellow_tripdata_end_month

    used_indices = [indexname]
    setup_commands = []
    teardown_commands = []
    key_metrics = []

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
    benchmark_repetitions_require_teardown_and_resetup = True

    print("-- Benchmark: {} -- ".format(test_name))
    print("-- Description: {} -- ".format(description))

    total_docs = 0

    print("Using random seed {0}".format(args.seed))
    random.seed(args.seed)

    index_types = generate_nyc_taxis_index_type()
    print("-- generating the ft.create commands -- ")
    ft_create_cmd = generate_ft_create_row(indexname, index_types, use_ftadd)
    setup_commands.append(ft_create_cmd)
    print("FT.CREATE command: {}".format(" ".join(ft_create_cmd)))

    print("-- generating the ft.drop commands -- ")
    ft_drop_cmd = generate_ft_drop_row(indexname)
    teardown_commands.append(ft_drop_cmd)

    csv_filenames = []
    print("Retrieving the required TLC record data")
    for y in range(start_y, end_y + 1):
        for m in range(start_m, end_m + 1):
            print(y, m)
            origin = "{}/yellow_tripdata_{}-{:02d}.csv".format(
                args.nyc_tlc_s3_bucket_prefix, y, m
            )
            filename = "{}/yellow_tripdata_{}-{:02d}.csv".format(working_dir, y, m)
            csv_filenames.append(filename)
            print("Checking if {} exists".format(filename))
            if os.path.isfile(filename) is False:
                print("Downloading {} to {}".format(origin, filename))
                download_url(origin, filename)
            else:
                print("{} exists, no need to download again".format(filename))

    total_docs = 0

    progress = tqdm(unit="docs")
    all_csvfile = open(all_fname, "a", newline="")
    all_csv_writer = csv.writer(all_csvfile, delimiter=",")
    for input_csv_filename in csv_filenames:
        with open(input_csv_filename) as csvfile:
            print("Processing csv {}".format(input_csv_filename))
            csvreader = csv.reader(csvfile)
            header = next(csvreader)
            total_amount_pos = index_or_none(header, "total_amount")
            improvement_surcharge_pos = index_or_none(header, "improvement_surcharge")
            pickup_longitude_pos = index_or_none(header, "pickup_longitude")
            pickup_latitude_pos = index_or_none(header, "pickup_latitude")
            pickup_datetime_pos = index_or_none(header, "tpep_pickup_datetime")
            dropoff_datetime_pos = index_or_none(header, "tpep_dropoff_datetime")
            rate_code_id_pos = index_or_none(header, "RateCodeID")
            tolls_amount_pos = index_or_none(header, "tolls_amount")
            dropoff_longitude_pos = index_or_none(header, "dropoff_longitude")
            dropoff_latitude_pos = index_or_none(header, "dropoff_latitude")
            passenger_count_pos = index_or_none(header, "passenger_count")
            fare_amount_pos = index_or_none(header, "fare_amount")
            extra_pos = index_or_none(header, "extra")
            trip_distance_pos = index_or_none(header, "trip_distance")
            tip_amount_pos = index_or_none(header, "tip_amount")
            store_and_fwd_flag_pos = index_or_none(header, "store_and_fwd_flag")
            payment_type_pos = index_or_none(header, "payment_type")
            mta_tax_pos = index_or_none(header, "mta_tax")
            vendor_id_pos = index_or_none(header, "VendorID")
            for row in csvreader:
                cmd = use_case_csv_row_to_cmd(
                    row,
                    index_types,
                    use_ftadd,
                    total_amount_pos,
                    improvement_surcharge_pos,
                    pickup_longitude_pos,
                    pickup_latitude_pos,
                    pickup_datetime_pos,
                    dropoff_datetime_pos,
                    rate_code_id_pos,
                    tolls_amount_pos,
                    dropoff_longitude_pos,
                    dropoff_latitude_pos,
                    passenger_count_pos,
                    fare_amount_pos,
                    extra_pos,
                    trip_distance_pos,
                    tip_amount_pos,
                    store_and_fwd_flag_pos,
                    payment_type_pos,
                    mta_tax_pos,
                    vendor_id_pos,
                )
                all_csv_writer.writerow(cmd)
                total_docs = total_docs + 1
                progress.update()
                if args.doc_limit > 0 and total_docs >= args.doc_limit:
                    break
    progress.close()
    all_csvfile.close()

    total_commands = total_docs
    cmd_category_all = {
        "setup-writes": 0,
        "writes": total_docs,
        "updates": 0,
        "reads": 0,
        "deletes": 0,
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
    #
    inputs = {"all": inputs_entry_all, "benchmark": inputs_entry_all}

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
            0,
            total_docs,
            total_docs,
            total_docs,
            0,
            0,
            0,
            benchmark_repetitions_require_teardown_and_resetup,
            ["setup"],
            ["benchmark"],
        )
        json.dump(setup_json, setupf, indent=2)

    if args.upload_artifacts_s3:
        artifacts = [benchmark_config_file, all_fname_compressed]
        upload_dataset_artifacts_s3(s3_bucket_name, s3_bucket_path, artifacts)
    if args.upload_artifacts_s3_uncompressed:
        artifacts = [all_fname]
        upload_dataset_artifacts_s3(s3_bucket_name, s3_bucket_path, artifacts)

    print("############################################")
    print("All artifacts generated.")
