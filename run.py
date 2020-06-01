#!/usr/bin/python3
# FTSB benchmark suite
# Python 3.X
# Version 0.1

import argparse
import gzip
import json
import os
import os.path
import shutil
import subprocess
import sys
from zipfile import ZipFile

import redis
import requests
from cpuinfo import cpuinfo


def whereis(program):
    for path in os.environ.get('PATH', '').split(':'):
        if os.path.exists(os.path.join(path, program)) and \
                not os.path.isdir(os.path.join(path, program)):
            return os.path.join(path, program)
    return None


# Check if system has the required utilities: ftsb_redisearch, etc
def required_utilities(utility_list):
    result = 1
    for index in utility_list:
        if whereis(index) == None:
            print('Cannot locate ' + index + ' in path!')
            result = 0
    return result


def decompress_file(compressed_filename, uncompressed_filename):
    splitted = os.path.splitext(compressed_filename)
    filetype = splitted[1]

    if (filetype == ".zip"):
        with ZipFile(compressed_filename, 'r') as zipObj:
            zipObj.extractall()

    elif (filetype == ".gz"):
        with gzip.open(compressed_filename, 'rb') as f_in:
            with open(uncompressed_filename, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)


if (__name__ == "__main__"):
    parser = argparse.ArgumentParser(
        description='RediSearch FTSB benchmark run helper. A wrapper around ftsb_redisearch.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--benchmark-config-file', required=True, type=str,
                        help="benchmark config file to read instructions from. can be a local file or a remote link")
    parser.add_argument('--workers', type=str, default=0,
                        help='number of workers to use during the benchark. If set to 0 it will auto adjust based on the machine number of VCPUs')
    parser.add_argument('--repetitions', type=int, default=1,
                        help='number of repetitions to run')
    parser.add_argument('--benchmark-requests', type=int, default=0,
                        help='Number of total requests to issue (0 = all of the present in input file)')
    parser.add_argument('--upload-results-s3', default=False, action='store_true',
                        help="uploads the result files and configuration file to public benchmarks.redislabs bucket. Proper credentials are required")
    parser.add_argument('--redis-url', type=str, default="redis://localhost:6379", help='The url for Redis connection')
    parser.add_argument('--local-dir', type=str, default="./", help='local dir to use as storage')
    parser.add_argument('--benchmark-output-file', default="results.json", type=str,
                        help="benchmark output file containing the overall results")

    args = parser.parse_args()
    use_case_specific_arguments = dict(args.__dict__)
    benchmark_config = None
    required_utilities_list = ["ftsb_redisearch"]
    if required_utilities(required_utilities_list) == 0:
        print('Utilities Missing! Exiting..')
        sys.exit(1)
    ftsb_redisearch_path = whereis("ftsb_redisearch")
    local_path = os.path.abspath(args.local_dir)
    workers = args.workers
    benchmark_machine_info = cpuinfo.get_cpu_info()
    total_cores = benchmark_machine_info['count']

    benchmark_suite_result = {"benchmark_machine_info": benchmark_machine_info, "redisearch_machine_info": None}

    if workers == 0:
        print('Setting number of workers equal to machine VCPUs {}'.format(total_cores))
        workers = total_cores

    if args.benchmark_config_file.startswith("http"):
        print("retrieving benchmark config file from remote url {}".format(args.benchmark_config_file))
        r = requests.get(args.benchmark_config_file)
        benchmark_config = r.json()
        remote_filename = args.benchmark_config_file[args.benchmark_config_file.rfind('/') + 1:]
        local_config_file = "{}/{}".format(local_path, remote_filename)
        open(local_config_file, 'wb').write(r.content)
        print("To avoid fetching again the config file use the option --benchmark-config-file {}".format(
            local_config_file))

    else:
        with open(args.benchmark_config_file, "r") as json_file:
            benchmark_config = json.load(json_file)

    print("Preparing to run test: {}.\nDescription: {}.".format(benchmark_config["name"],
                                                                benchmark_config["description"]))

    run_stages = ["setup", "benchmark"]
    run_stages_inputs = {

    }
    run_stages_outputs = {
        "setup":{},"benchmark":{}
    }

    print("Checking required inputs are in place...")
    for stage, input_description in benchmark_config["inputs"].items():
        remote_url = input_description["remote-url"]
        local_uncompressed_filename = input_description["local-uncompressed-filename"]
        local_compressed_filename = input_description["local-compressed-filename"]
        local_uncompressed_filename_path = "{}/{}".format(local_path, local_uncompressed_filename)
        local_compressed_filename_path = "{}/{}".format(local_path, local_compressed_filename)
        local_uncompressed_exists = os.path.isfile(local_uncompressed_filename_path)
        local_compressed_exists = os.path.isfile(local_compressed_filename_path)
        if stage in run_stages:
            # if the local uncompressed file exists dont need to do work
            if local_uncompressed_exists:
                print(
                    "\tLocal uncompressed file {} exists at {}. Nothing to do here".format(local_uncompressed_filename,
                                                                                           local_uncompressed_filename_path))
            # if the local compressed file exists then we need to uncompress it
            elif local_compressed_exists:
                print("\tLocal compressed file {} exists at {}. Uncompressing it".format(local_uncompressed_filename,
                                                                                         local_uncompressed_filename_path))
                decompress_file(local_compressed_filename_path, local_uncompressed_filename_path)

            elif remote_url is not None:
                print("\tRetrieving {} and saving to {}".format(remote_url, local_compressed_filename_path))
                r = requests.get(remote_url)
                open(local_compressed_filename_path, 'wb').write(r.content)
                decompress_file(local_compressed_filename_path, local_uncompressed_filename_path)

            else:
                print('\tFor stage {}, unable to retrieve required file {}! Exiting..'.format(stage,
                                                                                              local_uncompressed_filename))
                sys.exit(1)

            run_stages_inputs[stage] = local_uncompressed_filename_path

    aux_client = None
    print("Checking RediSearch is reachable at {}".format(args.redis_url))
    try:
        aux_client = redis.from_url(args.redis_url)
        aux_client.ping()
    except redis.connection.ConnectionError as e:
        print('Error establishing connection to Redis at {}! Message: {} Exiting..'.format(args.redis_url, e.__str__()))
        sys.exit(1)

    print("Running setup steps...")
    for command in benchmark_config["setup"]["commands"]:
        try:
            aux_client.execute_command(" ".join(command))
        except redis.connection.ConnectionError as e:
            print('Error while issuing setup command to Redis.Command {}! Error message: {} Exiting..'.format(command,
                                                                                                              e.__str__()))
            sys.exit(1)

    ###############################
    # Go client stage starts here #
    ###############################
    environ = os.environ.copy()
    stdoutPipe = subprocess.PIPE
    stderrPipe = subprocess.STDOUT
    stdinPipe = subprocess.PIPE
    options = {
        'stderr': stderrPipe,
        'env': environ,
    }

    for repetition in range(1, args.repetitions + 1):
        benchmark_repetitions_require_teardown = benchmark_config["benchmark"][
            "repetitions-require-teardown-and-re-setup"]
        if benchmark_repetitions_require_teardown is True or repetition == 1:
            ##################
            # Setup commands #
            ##################
            setup_run_key = "setup-run-{}.json".format(repetition)
            setup_run_json_output_fullpath = "{}/{}".format(local_path, setup_run_key)
            ftsb_args = []
            ftsb_args += [ftsb_redisearch_path, "--host={}".format(args.redis_url),
                          "--file={}".format(run_stages_inputs["setup"]), "--workers={}".format(workers),
                          "--json-out-file={}".format(setup_run_json_output_fullpath)]

            ftsb_process = subprocess.Popen(args=ftsb_args, **options)

            if ftsb_process.poll() is not None:
                print('Error while issuing setup commands. FTSB process is not alive. Exiting..')
                sys.exit(1)

            ftsb_process.communicate()
            # run_stages_outputs["setup"]
            with open(setup_run_json_output_fullpath) as json_result:
                run_stages_outputs["setup"][setup_run_key]=json.load(json_result)

        ######################
        # Benchmark commands #
        ######################
        ftsb_args = []
        benchmark_run_key = "benchmark-run-{}.json".format(repetition)
        benchmark_run_json_output_fullpath = "{}/{}".format(local_path, benchmark_run_key)
        ftsb_args += [ftsb_redisearch_path, "--host={}".format(args.redis_url),
                      "--file={}".format(run_stages_inputs["benchmark"]), "--workers={}".format(workers),
                      "--json-out-file={}".format(benchmark_run_json_output_fullpath), "--requests={}".format(args.benchmark_requests)]

        ftsb_process = subprocess.Popen(args=ftsb_args, **options)

        if ftsb_process.poll() is not None:
            print('Error while issuing benchmark commands. FTSB process is not alive. Exiting..')
            sys.exit(1)

        ftsb_process.communicate()
        with open(benchmark_run_json_output_fullpath) as json_result:
            run_stages_outputs["benchmark"][benchmark_run_key]=json.load(json_result)

    with open(args.benchmark_output_file,"w") as json_out_file:
        json.dump(run_stages_outputs, json_out_file, indent=2)