#!/usr/bin/python3

import argparse
import csv
import json
import os
import random
# package local imports
import sys

import boto3
from tqdm import tqdm

sys.path.append(os.getcwd() + '/..')

from common import download_url, generate_setup_json, compress_files
from pathlib import Path

""" Returns a human readable string reprentation of bytes"""


def humanized_bytes(bytes, units=[' bytes', 'KB', 'MB', 'GB', 'TB']):
    return str(bytes) + " " + units[0] if bytes < 1024 else humanized_bytes(bytes >> 10, units[1:])

def generate_inputs_dict_item(type, all_fname, description, remote_url, uncompressed_size, compressed_filename,
                              compressed_size, total_commands, command_category):
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

def generate_nyc_taxis_index_type():
    types = {}
    for f in ["vendor_id", "payment_type", "trip_type", "rate_code_id", "store_and_fwd_flag"]:
        types[f] = 'tag'
    for f in ["pickup_datetime", "dropoff_datetime"]:
        types[f] = 'text'
    for f in ["pickup_location_long_lat", "dropoff_location_long_lat"]:
        types[f] = 'geo'
    for f in ["passenger_count", "trip_distance", "fare_amount", "mta_tax", "extra", "improvement_surcharge",
              "tip_amount", "tolls_amount", "total_amount"]:
        types[f] = 'numeric'
    return types



def generate_ft_create_row(index, index_types):
    cmd = ["FT.CREATE", "{index}".format(index=index),"ON","HASH", "SCHEMA"]
    for f, v in index_types.items():
        cmd.append(f)
        cmd.append(v)
        cmd.append("SORTABLE")
    return cmd

def generate_ft_drop_row(index):
    cmd = ["FT.DROP", "{index}".format(index=index),"DD"]
    return cmd

if (__name__ == "__main__"):
    parser = argparse.ArgumentParser(description='RediSearch FTSB data generator.',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--seed', type=int, default=12345,
                        help='the random seed used to generate random deterministic outputs')
    parser.add_argument('--doc-limit', type=int, default=0,
                        help='the total documents to generate to be added in the setup stage')
    parser.add_argument('--yellow-tripdata-start-year', type=int, default=2015,
                        help='the start year of the yellow trip data to fetch')
    parser.add_argument('--yellow-tripdata-end-year', type=int, default=2015,
                        help='the end year of the yellow trip data to fetch')
    parser.add_argument('--yellow-tripdata-start-month', type=int, default=1,
                        help='the start month of the yellow trip data to fetch')
    parser.add_argument('--yellow-tripdata-end-month', type=int, default=12,
                        help='the start month of the yellow trip data to fetch')
    parser.add_argument('--index-name', type=str, default="nyc_taxis",
                        help='the name of the RediSearch index to be used')
    parser.add_argument('--test-name', type=str, default="nyc_taxis", help='the name of the test')
    parser.add_argument('--test-description', type=str,
                        default="benchmark focused on write performance, making usage of TLC Trip Record Data that contains the rides that have been performed in yellow taxis in New York in 2015",
                        help='the full description of the test')
    parser.add_argument('--benchmark-output-file-prefix', type=str, default="nyc_taxis.redisearch.commands",
                        help='prefix to be used when generating the artifacts')
    parser.add_argument('--benchmark-config-file', type=str, default="nyc_taxis.redisearch.cfg.json",
                        help='name of the output config file used to store the full benchmark suite steps and description')
    parser.add_argument('--upload-artifacts-s3', default=False, action='store_true',
                        help="uploads the generated dataset files and configuration file to public benchmarks.redislabs bucket. Proper credentials are required")
    parser.add_argument('--nyc-tlc-s3-bucket-prefix', type=str,
                        default="https://s3.amazonaws.com/nyc-tlc/trip+data",
                        help='The s3 bucket prefix to fetch the input files containing the origin CSV datasets to read the data from.')
    parser.add_argument('--temporary-work-dir', type=str,
                        default="./tmp",
                        help='The temporary dir to use as working directory for file download, compression,etc... ')

    args = parser.parse_args()
    use_case_specific_arguments = dict(args.__dict__)
    del use_case_specific_arguments["upload_artifacts_s3"]
    del use_case_specific_arguments["test_name"]
    del use_case_specific_arguments["test_description"]
    del use_case_specific_arguments["benchmark_config_file"]
    del use_case_specific_arguments["benchmark_output_file_prefix"]

    # generate the temporary working dir if required
    working_dir = args.temporary_work_dir
    Path(working_dir).mkdir(parents=True, exist_ok=True)
    seed = args.seed

    doc_limit = args.doc_limit
    indexname = args.index_name
    benchmark_output_file = args.benchmark_output_file_prefix
    benchmark_config_file = args.benchmark_config_file

    start_y = args.yellow_tripdata_start_year
    end_y = args.yellow_tripdata_end_year
    start_m = args.yellow_tripdata_start_month
    end_m = args.yellow_tripdata_end_month

    used_indices = [indexname]
    setup_commands = []
    teardown_commands = []
    key_metrics = [
        {
            "step": "benchmark",
            "metric-family": "throughput",
            "metric-json-path": "OverallRates.overallOpsRate",
            "metric-name": "Overall writes query rate",
            "unit": "docs/sec",
            "metric-type": "numeric",
            "comparison": "higher-better",
            "per-step-comparison-metric-priority": 1,
        }, {
            "step": "benchmark",
            "metric-family": "latency",
            "metric-json-path": "OverallQuantiles.allCommands.q50",
            "metric-name": "Overall writes query q50 latency",
            "unit": "ms",
            "metric-type": "numeric",
            "comparison": "lower-better",
            "per-step-comparison-metric-priority": 2,
        }

    ]
    total_writes = 0
    total_reads = 0
    total_updates = 0
    total_deletes = 0
    description = args.test_description
    test_name = args.test_name
    s3_bucket_name = "benchmarks.redislabs"
    s3_bucket_path = "redisearch/datasets/{}/".format(test_name)
    s3_uri = "https://s3.amazonaws.com/{bucket_name}/{bucket_path}".format(bucket_name=s3_bucket_name,
                                                                           bucket_path=s3_bucket_path)
    all_fname = "{}.ALL.csv".format(benchmark_output_file)
    all_fname_compressed = "{}.ALL.tar.gz".format(benchmark_output_file)
    remote_url_all = "{}{}".format(s3_uri, all_fname_compressed)
    json_version = "0.1"
    benchmark_repetitions_require_teardown_and_resetup = True

    print("-- Benchmark: {} -- ".format(test_name))
    print("-- Description: {} -- ".format(description))

    total_docs = 0

    print("Using random seed {0}".format(args.seed))
    random.seed(args.seed)

    index_types = generate_nyc_taxis_index_type()
    print("-- generating the ft.create commands -- ")
    ft_create_cmd = generate_ft_create_row(indexname, index_types)
    setup_commands.append(ft_create_cmd)

    print("-- generating the ft.drop commands -- ")
    ft_drop_cmd = generate_ft_drop_row(indexname)
    teardown_commands.append(ft_drop_cmd)

    csv_filenames = []
    print("Retrieving the required TLC record data")
    for y in range(start_y, end_y + 1):
        for m in range(start_m, end_m + 1):
            print(y, m)
            origin = "{}/yellow_tripdata_{}-{:02d}.csv".format(args.nyc_tlc_s3_bucket_prefix, y, m)
            filename = "{}/yellow_tripdata_{}-{:02d}.csv".format(working_dir, y, m)
            csv_filenames.append(filename)
            print("Checking if {} exists".format(filename))
            if os.path.isfile(filename) is False:
                print("Downloading {} to {}".format(origin, filename))
                download_url(origin, filename)
            else:
                print("{} exists, no need to download again".format(filename))

    total_docs = 0
    docs = []

    progress = tqdm(unit="docs")
    all_csvfile = open(all_fname, 'a', newline='')
    all_csv_writer = csv.writer(all_csvfile, delimiter=',')
    for input_csv_filename in csv_filenames:
        with open(input_csv_filename) as csvfile:
            print("Processing csv {}".format(input_csv_filename))
            csvreader = csv.reader(csvfile)
            header = next(csvreader)
            total_amount_pos = header.index("total_amount")
            improvement_surcharge_pos = header.index("improvement_surcharge")
            pickup_longitude_pos = header.index("pickup_longitude")
            pickup_latitude_pos = header.index("pickup_latitude")
            pickup_datetime_pos = header.index("tpep_pickup_datetime")
            dropoff_datetime_pos = header.index("tpep_dropoff_datetime")
            rate_code_id_pos = header.index("RateCodeID")
            tolls_amount_pos = header.index("tolls_amount")
            dropoff_longitude_pos = header.index("dropoff_longitude")
            dropoff_latitude_pos = header.index("dropoff_latitude")
            passenger_count_pos = header.index("passenger_count")
            fare_amount_pos = header.index("fare_amount")
            extra_pos = header.index("extra")
            trip_distance_pos = header.index("trip_distance")
            tip_amount_pos = header.index("tip_amount")
            store_and_fwd_flag_pos = header.index("store_and_fwd_flag")
            payment_type_pos = header.index("payment_type")
            mta_tax_pos = header.index("mta_tax")
            vendor_id_pos = header.index("VendorID")
            for row in csvreader:
                hash = {
                    "total_amount": row[total_amount_pos],
                    "improvement_surcharge": row[improvement_surcharge_pos],
                    "pickup_location_long_lat": "{},{}".format(row[pickup_longitude_pos],row[pickup_latitude_pos]),
                    "pickup_datetime": row[pickup_datetime_pos],
                    "trip_type": "1",
                    "dropoff_datetime": row[dropoff_datetime_pos],
                    "rate_code_id": row[rate_code_id_pos],
                    "tolls_amount": row[tolls_amount_pos],
                    "dropoff_location_long_lat": "{},{}".format(row[dropoff_longitude_pos],row[dropoff_latitude_pos]),
                    "passenger_count": row[passenger_count_pos],
                    "fare_amount": row[fare_amount_pos],
                    "extra": row[extra_pos],
                    "trip_distance": row[trip_distance_pos],
                    "tip_amount": row[tip_amount_pos],
                    "store_and_fwd_flag": row[store_and_fwd_flag_pos],
                    "payment_type": row[payment_type_pos],
                    "mta_tax": row[mta_tax_pos],
                    "vendor_id": row[vendor_id_pos],
                }
                for k in hash.keys():
                    assert k in index_types.keys()
                total_docs = total_docs + 1
                fields = []
                for f,v in hash.items():
                    fields.append(f)
                    fields.append(v)
                cmd = ["WRITE", "W1", "HSET", "doc:{n}".format(n=total_docs)]
                for x in fields:
                    cmd.append(x)
                # docs.append(cmd)
                all_csv_writer.writerow(cmd)
                progress.update()
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
    status, uncompressed_size, compressed_size = compress_files([all_fname], all_fname_compressed)
    inputs_entry_all = generate_inputs_dict_item("all", all_fname, "contains both setup and benchmark commands",
                                                 remote_url_all, uncompressed_size, all_fname_compressed,
                                                 compressed_size, total_commands, cmd_category_all)
#
    inputs = {"all": inputs_entry_all, "benchmark": inputs_entry_all}

    deployment_requirements = {"utilities": { "ftsb_redisearch" : {} }, "benchmark-tool": "ftsb_redisearch", "redis-server": {"modules": {"ft": {}}}}
    run_stages = ["benchmark"]
    with open(benchmark_config_file, "w") as setupf:
        setup_json = generate_setup_json(json_version, use_case_specific_arguments, test_name, description,
                                         run_stages,
                                         deployment_requirements,
                                         key_metrics, inputs,
                                         setup_commands,
                                         teardown_commands,
                                         used_indices,
                                         total_commands,
                                         0, total_docs, total_docs, total_docs, 0, 0, 0,
                                         benchmark_repetitions_require_teardown_and_resetup,
                                         ["setup"],
                                         ["benchmark"]
                                         )
        json.dump(setup_json, setupf, indent=2)

    if args.upload_artifacts_s3:
        print("-- uploading dataset to s3 -- ")
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(s3_bucket_name)
        artifacts = [benchmark_config_file, all_fname_compressed]
        progress = tqdm(unit="files", total=len(artifacts))
        for input in artifacts:
            object_key = '{bucket_path}{filename}'.format(bucket_path=s3_bucket_path, filename=input)
            bucket.upload_file(input, object_key)
            object_acl = s3.ObjectAcl(s3_bucket_name, object_key)
            response = object_acl.put(ACL='public-read')
            progress.update()
        progress.close()

    artifacts = [benchmark_config_file, all_fname_compressed]

    print("############################################")
    print("All artifacts generated.")
