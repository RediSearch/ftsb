#!/usr/bin/python3
# FTSB benchmark suite wrapper. used for automation
# Python 3.X
# Version 0.1

import argparse
import datetime as dt
import gzip
import json
import operator
import os
import os.path
import shutil
import subprocess
import sys
import tarfile
from functools import reduce
from zipfile import ZipFile

import humanize
import pandas as pd
import redis
import requests
from cpuinfo import cpuinfo

EPOCH = dt.datetime.utcfromtimestamp(0)


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
    if compressed_filename.endswith( ".zip"):
        with ZipFile(compressed_filename, 'r') as zipObj:
            zipObj.extractall()

    elif compressed_filename.endswith("tar.gz"):
        tar = tarfile.open(compressed_filename, "r:gz")
        tar.extractall()
        tar.close()

    elif compressed_filename.endswith("tar"):
        tar = tarfile.open(compressed_filename, "r:")
        tar.extractall()
        tar.close()


def findJsonPath(element, json):
    return reduce(operator.getitem, element.split('.'), json)


def ts_milli(at_dt):
    return int((at_dt - dt.datetime(1970, 1, 1)).total_seconds() * 1000)


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
    parser.add_argument('--deployment-type', type=str, default="docker-oss",
                        help='one of docker-oss,docker-oss-cluster,docker-enterprise,oss,oss-cluster,enterprise')
    parser.add_argument('--deployment-shards', type=int, default=1,
                        help='number of database shards used in the deployment')
    parser.add_argument('--output-file-prefix', type=str, default="", help='prefix to quickly tag some files')

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
    redisearch_version = None
    redisearch_git_sha = None

    benchmark_infra = {"total-benchmark-machines": 0, "benchmark-machines": {}, "total-db-machines": 0,
                       "db-machines": {}}
    benchmark_machine_1 = {"machine_info": benchmark_machine_info}
    benchmark_infra["benchmark-machines"]["benchmark-machine-1"] = benchmark_machine_1
    benchmark_infra["total-benchmark-machines"] += 1

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
    benchmark_output_dict = {
        "key-configs": {"deployment-type": args.deployment_type, "deployment-shards": args.deployment_shards,
                        "redisearch-version": None, "git_sha": None},
        "key-results":{},
        "benchmark-config": benchmark_config,
        "setup": {},
        "benchmark": {},
        "infastructure": benchmark_infra
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
        found_redisearch = False
        aux_client = redis.from_url(args.redis_url)
        module_list_reply = aux_client.execute_command("module list")
        for module in module_list_reply:
            module_name = module[1].decode()
            module_version = module[3]
            if module_name == "ft":
                found_redisearch = True
                redisearch_version = module_version
                debug_gitsha_reply = aux_client.execute_command("ft.debug git_sha")
                redisearch_git_sha = debug_gitsha_reply.decode()
                print(
                    'Found RediSearch Module at {}! version: {} git_sha: {}'.format(args.redis_url, redisearch_version,
                                                                                    redisearch_git_sha))
        if found_redisearch is False:
            print('Unable to find RediSearch Module at {}! Exiting..'.format(args.redis_url))
            sys.exit(1)
        benchmark_output_dict["key-configs"]["redisearch-version"] = redisearch_version
        benchmark_output_dict["key-configs"]["git_sha"] = redisearch_git_sha

        server_info = aux_client.info("Server")
        db_machine_1 = {"machine_info": None, "redis_info": server_info}
        benchmark_infra["db-machines"]["db-machine-1"] = db_machine_1
        benchmark_infra["total-db-machines"] += 1
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
    start_time = dt.datetime.now()

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
                benchmark_output_dict["setup"][setup_run_key] = json.load(json_result)

        ######################
        # Benchmark commands #
        ######################
        ftsb_args = []
        benchmark_run_key = "benchmark-run-{}.json".format(repetition)
        benchmark_run_json_output_fullpath = "{}/{}".format(local_path, benchmark_run_key)
        ftsb_args += [ftsb_redisearch_path, "--host={}".format(args.redis_url),
                      "--file={}".format(run_stages_inputs["benchmark"]), "--workers={}".format(workers),
                      "--json-out-file={}".format(benchmark_run_json_output_fullpath),
                      "--requests={}".format(args.benchmark_requests)]

        ftsb_process = subprocess.Popen(args=ftsb_args, **options)

        if ftsb_process.poll() is not None:
            print('Error while issuing benchmark commands. FTSB process is not alive. Exiting..')
            sys.exit(1)

        ftsb_process.communicate()
        with open(benchmark_run_json_output_fullpath) as json_result:
            benchmark_output_dict["benchmark"][benchmark_run_key] = json.load(json_result)

        if benchmark_repetitions_require_teardown is True or repetition == args.repetitions:
            print("Running tear down steps...")
            for command in benchmark_config["teardown"]["commands"]:
                try:
                    aux_client.execute_command(" ".join(command))
                except redis.connection.ConnectionError as e:
                    print(
                        'Error while issuing teardown command to Redis.Command {}! Error message: {} Exiting..'.format(
                            command,
                            e.__str__()))
                    sys.exit(1)

    end_time = dt.datetime.now()

    ##################################
    # Repetitions Results Comparison #
    ##################################

    step_df_dict = {}
    benchmark_output_dict["results-comparison"] = {}

    for step in ["setup","benchmark"]:
        step_df_dict[step] = {}
        step_df_dict[step]["df_dict"] = {"run-name": []}
        step_df_dict[step]["sorting_metric_names"] = []
        step_df_dict[step]["sorting_metric_sorting_direction"] = []
        step_df_dict[step]["metric_json_path"] = []

    for metric in benchmark_config["key-metrics"]:
        step = metric["step"]
        metric_name = metric["metric-name"]
        metric_json_path = metric["metric-json-path"]
        step_df_dict[step]["sorting_metric_names"].append(metric_name)
        step_df_dict[step]["metric_json_path"].append(metric_json_path)
        step_df_dict[step]["df_dict"][metric_name] = []
        step_df_dict[step]["sorting_metric_sorting_direction"].append(False if metric["comparison"] == "higher-better" else True)

    for step in ["setup","benchmark"]:
        for run_name, result_run in benchmark_output_dict[step].items():
            step_df_dict[step]["df_dict"]["run-name"].append(run_name)
            for pos, metric_json_path in enumerate(step_df_dict[step]["metric_json_path"]):
                metric_name = step_df_dict[step]["sorting_metric_names"][pos]
                metric_value = findJsonPath(metric_json_path, result_run)
                step_df_dict[step]["df_dict"][metric_name].append(metric_value)
        dfObj = pd.DataFrame(step_df_dict[step]["df_dict"])
        dfObj.sort_values(step_df_dict[step]["sorting_metric_names"], ascending=step_df_dict[step]["sorting_metric_sorting_direction"], inplace=True)
        print(dfObj)
        # stddev of the dataframe
        print(dfObj.std())
        # variance of the dataframe
        print(dfObj.var())
        benchmark_output_dict["key-results"][step]= {}
        benchmark_output_dict["key-results"][step]["table"] = json.loads(dfObj.to_json(orient='records'))
        benchmark_output_dict["key-results"][step]["reliability-analysis"] = {'var': json.loads(dfObj.var().to_json()),
                                                                               'stddev': json.loads(dfObj.std().to_json())}

    #####################
    # Run Info Metadata #
    #####################
    start_time_str = start_time.strftime("%Y-%m-%d-%H-%M-%S")
    end_time_str = end_time.strftime("%Y-%m-%d-%H-%M-%S")
    duration_ms = ts_milli(end_time) - ts_milli(start_time)
    start_time_ms = ts_milli(start_time)
    end_time_ms = ts_milli(end_time)
    duration_humanized = humanize.naturaldelta((end_time - start_time))

    run_info = {"start-time-ms": start_time_ms, "start-time-humanized": start_time_str, "end-time-ms": end_time_ms,
                "end-time-humanized": end_time_str, "duration-ms": duration_ms,
                "duration-humanized": duration_humanized}
    benchmark_output_dict["run-info"] = run_info

    benchmark_output_filename = "{prefix}{time_str}-{deployment_type}-{use_case}-{version}-{git_sha}.json".format(
        prefix=args.output_file_prefix,
        time_str=start_time_str, deployment_type=args.deployment_type, use_case=benchmark_config["name"],
        version=redisearch_version, git_sha=redisearch_git_sha)

    with open(benchmark_output_filename, "w") as json_out_file:
        json.dump(benchmark_output_dict, json_out_file, indent=2)
