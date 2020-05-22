import argparse
import csv
import json
import random
import re
import time
import uuid

from tqdm import tqdm


def process_inventory(row, market_count, nodes, total_nodes, docs_map, product_ids, countries_alpha_3,
                      countries_alpha_p):
    # uniq_id,product_name,manufacturer,price,number_available_in_stock,number_of_reviews,number_of_answered_questions,average_review_rating,amazon_category_and_sub_category,customers_who_bought_this_item_also_bought,description,product_information,product_description,items_customers_buy_after_viewing_this_item,customer_questions_and_answers,customer_reviews,sellers
    added_docs = 0
    NUMERIC = "NUMERIC"
    GEO = "GEO"
    TAG = "TAG"
    TEXT = "TEXT"
    for inner_doc_pos in range(0, market_count):
        skuId = row[0]
        brand = row[2]
        sellers_raw = row[16]
        nodeType = "store"
        availableToSource = "true"
        standardAvailableToPromise = "true"
        bopisAvailableToPromise = "true"
        onHold = "false"
        exclusionType = "false"

        onhand = random.randint(0, 64000)
        allocated = random.randint(0, 64000)
        reserved = random.randint(0, 64000)
        storeAllocated = random.randint(0, 64000)
        transferAllocated = random.randint(0, 64000)
        storeReserved = random.randint(0, 64000)
        confirmedQuantity = random.randint(0, 64000)
        standardSafetyStock = random.randint(0, 64000)
        bopisSafetyStock = random.randint(0, 64000)
        virtualHold = random.randint(0, 64000)

        onhandLastUpdatedTimestamp = int(time.time() + random.randint(0, 24 * 60 * 60))
        allocatedLastUpdatedTimestamp = int(time.time() + random.randint(0, 24 * 60 * 60))
        reservedLastUpdatedTimestamp = int(time.time() + random.randint(0, 24 * 60 * 60))
        storeAllocatedLastUpdatedTimestamp = int(time.time() + random.randint(0, 24 * 60 * 60))
        transferAllocatedLastUpdatedTimestamp = int(time.time() + random.randint(0, 24 * 60 * 60))
        storeReservedLastUpdatedTimestamp = int(time.time() + random.randint(0, 24 * 60 * 60))

        pattern = re.compile('[\W_]+')

        sellers = re.findall(
            r'\"Seller_name_\d+\"=>\"([^"]+)\"', sellers_raw)
        if len(sellers) == 0:
            available = "false"

        for node in sellers:
            if node not in nodes:
                total_nodes = total_nodes + 1
                nodeId = total_nodes
                nodes[node] = nodeId

        nodesList = list(nodes.keys())
        if len(nodesList) > 0:
            # k = 5 if 5 <= len(nodesList) else len(nodesList)
            k = 10
            for node in random.choices(nodesList, k=k):
                # print(random.choices(nodesList, k=k))
                nodeId = nodes[node]
                did = str(uuid.uuid4()).replace("-", "")
                if skuId not in product_ids:
                    product_ids[skuId]=1
                else:
                    product_ids[skuId]+=1
                market = random.choices(countries_alpha_3, weights=countries_alpha_p)[0]
                doc_id = "{market}_{nodeId}_{skuId}".format(market=market, nodeId=nodeId, skuId=did)

                if doc_id not in docs_map:
                    doc = {"doc_id": doc_id,
                           "schema": {
                               "market": {"type": TAG, "value": market, "field_options": ["SORTABLE"]},
                               "nodeId": {"type": TAG, "value": nodeId, "field_options": ["SORTABLE"]},
                               "skuId": {"type": TAG, "value": skuId,
                                         "field_options": ["SORTABLE"]},
                               # onhand
                               "onhand": {"type": NUMERIC, "value": onhand,
                                          "field_options": ["SORTABLE", "NOINDEX"]},
                               "onhandLastUpdatedTimestamp": {"type": NUMERIC, "value": onhandLastUpdatedTimestamp,
                                                              "field_options": ["SORTABLE", "NOINDEX"]},
                               # allocated
                               "allocated": {"type": NUMERIC, "value": allocated,
                                             "field_options": ["SORTABLE", "NOINDEX"]},
                               "allocatedLastUpdatedTimestamp": {"type": NUMERIC, "value": allocatedLastUpdatedTimestamp,
                                                                 "field_options": ["SORTABLE", "NOINDEX"]},
                               # reserved
                               "reserved": {"type": NUMERIC, "value": reserved,
                                            "field_options": ["SORTABLE", "NOINDEX"]},
                               "reservedLastUpdatedTimestamp": {"type": NUMERIC, "value": reservedLastUpdatedTimestamp,
                                                                "field_options": ["SORTABLE", "NOINDEX"]},
                               # store allocated
                               "storeAllocated": {"type": NUMERIC, "value": storeAllocated,
                                                  "field_options": ["SORTABLE", "NOINDEX"]},
                               "storeAllocatedLastUpdatedTimestamp": {"type": NUMERIC,
                                                                      "value": storeAllocatedLastUpdatedTimestamp,
                                                                      "field_options": ["SORTABLE", "NOINDEX"]},
                               # transfer allocated
                               "transferAllocated": {"type": NUMERIC, "value": transferAllocated,
                                                     "field_options": ["SORTABLE", "NOINDEX"]},
                               "transferAllocatedLastUpdatedTimestamp": {"type": NUMERIC,
                                                                         "value": transferAllocatedLastUpdatedTimestamp,
                                                                         "field_options": ["SORTABLE", "NOINDEX"]},

                               # transfer allocated
                               "storeReserved": {"type": NUMERIC, "value": storeReserved,
                                                 "field_options": ["SORTABLE", "NOINDEX"]},
                               "storeReservedLastUpdatedTimestamp": {"type": NUMERIC,
                                                                     "value": storeReservedLastUpdatedTimestamp,
                                                                     "field_options": ["SORTABLE", "NOINDEX"]},

                               # store reserved
                               "confirmedQuantity": {"type": NUMERIC, "value": confirmedQuantity,
                                                     "field_options": ["SORTABLE", "NOINDEX"]},
                               "standardSafetyStock": {"type": NUMERIC, "value": standardSafetyStock,
                                                       "field_options": ["SORTABLE", "NOINDEX"]},
                               "bopisSafetyStock": {"type": NUMERIC, "value": bopisSafetyStock,
                                                    "field_options": ["SORTABLE", "NOINDEX"]},
                               "virtualHold": {"type": NUMERIC, "value": virtualHold,
                                               "field_options": ["SORTABLE", "NOINDEX"]},

                               # tags
                               "availableToSource": {"type": TAG, "value": pattern.sub('', availableToSource),
                                                     "field_options": []},
                               "standardAvailableToPromise": {"type": TAG,
                                                              "value": pattern.sub('', standardAvailableToPromise),
                                                              "field_options": []},
                               "bopisAvailableToPromise": {"type": TAG, "value": pattern.sub('', bopisAvailableToPromise),
                                                           "field_options": []},

                               "nodeType": {"type": TAG, "value": pattern.sub('', nodeType), "field_options": []},
                               "brand": {"type": TAG, "value": pattern.sub('', brand), "field_options": ["NOINDEX"]},

                               "onHold": {"type": TAG, "value": pattern.sub('', onHold), "field_options": []},
                               "exclusionType": {"type": TAG, "value": pattern.sub('', exclusionType), "field_options": []},
                           }
                           }
                    docs_map[doc_id] = doc
                    added_docs = added_docs + 1

    return nodes, total_nodes, docs_map, added_docs, product_ids


def generate_ft_aggregate_row(index, countries_alpha_3, countries_alpha_p, maxSkusList, skus, maxNodesList, nodes):
    product_id_list = []
    market = random.choices(countries_alpha_3, weights=countries_alpha_p)[0]

    skuId_list = random.choices(skus, k=maxSkusList)
    nodeId_list = random.choices(nodes, k=maxNodesList)

    cmd = ["READ", "FT.AGGREGATE", "{index}".format(index=index),
           "@market:{{{0}}} @skuId:{{{1}}} @nodeId:{{{2}}}".format(market,
                                                                   "|".join(skuId_list),
                                                                   "|".join(nodeId_list))
        , "LOAD", "21", "@market", "@skuId", "@nodeId", "@brand", "@nodeType", "@onhand", "@allocated",
           "@confirmedQuantity", "@reserved", "@virtualHold", "@availableToSource", "@standardAvailableToPromise",
           "@bopisAvailableToPromise", "@storeAllocated", "@bopisSafetyStock", "@transferAllocated",
           "@standardSafetyStock", "@storeReserved", "@availableToSource", "@exclusionType", "@onHold", "WITHCURSOR",
           "COUNT", "500"]
    return cmd


def generate_ft_add_row(index, doc):
    cmd = ["SETUP_WRITE", "FT.ADD", "{index}".format(index=index),
           "{index}-{doc_id}".format(index=index, doc_id=doc["doc_id"]), 1.0, "REPLACE", "FIELDS"]
    for f, v in doc["schema"].items():
        cmd.append(f)
        cmd.append(v["value"])
    return cmd


def generate_ft_create_row(index, doc):
    cmd = ["FT.CREATE", "{index}".format(index=index), "SCHEMA"]
    for f, v in doc["schema"].items():
        cmd.append(f)
        cmd.append(v["type"])
        if len(v["field_options"]) > 0:
            cmd.extend(v["field_options"])
    return cmd


def generate_ft_add_update_row(indexname, doc):
    cmd = ["UPDATE", "FT.ADD", "{index}".format(index=indexname),
           "{index}-{doc_id}".format(index=indexname, doc_id=doc["doc_id"]), 1.0,
           "REPLACE", "PARTIAL", "FIELDS"]
    TRUES = "true"
    FALSES = "false"
    standardAvailableToPromise = TRUES if bool(random.getrandbits(1)) == True else FALSES
    availableToSource = TRUES if bool(random.getrandbits(1)) == True else FALSES
    market = doc["schema"]["market"]["value"]
    nodeId = doc["schema"]["nodeId"]["value"]
    nodeType = doc["schema"]["nodeType"]["value"]
    new = [
        "market", market, "nodeId", nodeId, "nodeType", nodeType, "availableToSource", availableToSource,
        "standardAvailableToPromise", standardAvailableToPromise]
    cmd.extend(new)

    return cmd


def generate_setup_json(test_name, description, setup_commands, teardown_commands, used_indices,
                        total_benchmark_commands, total_docs, total_writes, total_updates, total_reads, total_deletes):
    setup_json = {
        "name": test_name,
        "description": description,
        "setup": {
            "commands": setup_commands
        },
        "teardown": {
            "commands": teardown_commands
        },
        "used_indices": used_indices,
        "total_benchmark_commands": total_benchmark_commands,
        "command_category": {
            "setup_writes": total_docs,
            "writes": total_writes,
            "updates": total_updates,
            "reads": total_reads,
            "deletes": total_deletes,
        }
    }
    return setup_json


if (__name__ == "__main__"):
    parser = argparse.ArgumentParser(description='RediSearch FTSB data generator.')
    parser.add_argument('--update_ratio', type=float, default=0.85,)
    parser.add_argument('--seed', type=int, default=12345,)
    parser.add_argument('--doc_limit', type=int, default=100000,)
    parser.add_argument('--total_benchmark_commands', type=int, default=100000,)
    parser.add_argument('--max_skus_per_aggregate', type=int, default=100,)
    parser.add_argument('--max_nodes_per_aggregate', type=int, default=100,)
    parser.add_argument('--indexname', type=str, default="inventory",)
    parser.add_argument('--benchmark_output_file_prefix', type=str, default="inventory.redisearch.commands",)
    parser.add_argument('--benchmark_config_file', type=str, default="inventory.redisearch.cfg.json",)
    parser.add_argument('--input_data_filename', type=str, default="./../../scripts/usecases/ecommerce/amazon_co-ecommerce_sample.csv",)
    args = parser.parse_args()
    seed = args.seed
    update_ratio = args.update_ratio
    read_ratio = 1 - update_ratio
    doc_limit = args.doc_limit
    total_benchmark_commands = args.total_benchmark_commands
    max_skus_per_aggregate = args.max_skus_per_aggregate
    max_nodes_per_aggregate = args.max_nodes_per_aggregate
    indexname = args.indexname
    input_data_filename = args.input_data_filename
    benchmark_output_file = args.benchmark_output_file_prefix
    benchmark_config_file = args.benchmark_config_file
    all_fname = "{}.ALL.csv".format(benchmark_output_file)
    setup_fname = "{}.SETUP.csv".format(benchmark_output_file)
    bench_fname = "{}.BENCH.csv".format(benchmark_output_file)
    used_indices = [indexname]
    setup_commands = []
    teardown_commands = []
    total_writes = 0
    total_reads = 0
    total_updates = 0
    total_deletes = 0
    description = "benchmark focused on updates and aggregate performance"
    test_name = "ecommerce-inventory"

    countries_alpha_3 = ["US", "CA", "FR", "IL", "UK"]
    countries_alpha_p = [0.8, 0.05, 0.05, 0.05, 0.05]
    docs_map = {}
    nodes = {}
    skusIds = {}
    total_nodes = 0
    total_docs = 0

    print("Using random seed {0}".format(args.seed))
    random.seed(args.seed)

    docs = []
    print("-- generating the write commands -- ")
    print("Reading csv data to generate docs")
    progress = tqdm(unit="docs", total=doc_limit)
    while total_docs < doc_limit:
        with open(input_data_filename, newline='') as csvfile:
            spamreader = csv.reader(csvfile, delimiter=',')
            for row in spamreader:
                nodes, total_nodes, docs_map, added_docs, skusIds = process_inventory(row, 5, nodes, total_nodes,
                                                                                      docs_map, skusIds, countries_alpha_3,
                                                                                      countries_alpha_p)
                total_docs = total_docs + added_docs
                if total_docs > doc_limit:
                    break
                progress.update(added_docs)
        if total_docs > doc_limit:
            break

    progress.close()
    total_skids = len(list(skusIds.keys()))
    print("Generated {} total docs with {} distinct skids and {} distinct nodes".format(total_docs, total_skids, total_nodes))
    print("-- generating the ft.create commands -- ")
    ft_create_cmd = generate_ft_create_row(indexname, list(docs_map.values())[0])
    print(" ".join(ft_create_cmd))
    setup_commands.append(ft_create_cmd)
    print("-- generating {} ft.add commands -- ".format(total_docs))
    print("\t saving to {} and {}".format(setup_fname, all_fname))

    all_csvfile = open( all_fname, 'w', newline='')
    setup_csvfile = open( setup_fname, 'w', newline='')
    all_csv_writer = csv.writer(all_csvfile, delimiter=',')
    setup_csv_writer = csv.writer(setup_csvfile, delimiter=',')
    progress = tqdm(unit="docs", total=total_docs)
    for doc in docs_map.values():
        generated_row = generate_ft_add_row(indexname, doc)
        all_csv_writer.writerow(generated_row)
        setup_csv_writer.writerow(generated_row)
        progress.update()
    progress.close()
    setup_csvfile.close()

    print("-- generating {} update/read commands -- ".format(total_benchmark_commands))
    print("\t saving to {} and {}".format(bench_fname, all_fname))
    bench_csvfile = open( bench_fname, 'w', newline='')
    bench_csv_writer = csv.writer(bench_csvfile, delimiter=',')

    docs_list = list(docs_map.values())
    skusIds_list = list(skusIds.keys())
    nodesIds = ["{}".format(x) for x in range(1, total_nodes)]
    csv_writer = csv.writer(csvfile, delimiter=',')
    progress = tqdm(unit="docs", total=total_benchmark_commands)
    for _ in range(0, total_benchmark_commands):
        choice = random.choices(["update", "read"], weights=[update_ratio, read_ratio])[0]
        if choice == "update":
            random_doc_pos = random.randint(0, total_docs - 1)
            doc = docs_list[random_doc_pos]
            generated_row = generate_ft_add_update_row(indexname, doc)
            total_updates = total_updates + 1
        elif choice == "read":
            generated_row = generate_ft_aggregate_row(indexname, countries_alpha_3, countries_alpha_p, max_skus_per_aggregate, skusIds_list,
                                                      max_nodes_per_aggregate, nodesIds)
            total_reads = total_reads + 1
        all_csv_writer.writerow(generated_row)
        bench_csv_writer.writerow(generated_row)
        progress.update()
    progress.close()

    bench_csvfile.close()
    all_csvfile.close()

    with open(benchmark_config_file, "w") as setupf:
        setup_json = generate_setup_json(test_name, description, setup_commands, teardown_commands, used_indices,
                                         total_benchmark_commands, total_docs, total_writes, total_updates, total_reads,
                                         total_deletes)
        json.dump(setup_json, setupf)
