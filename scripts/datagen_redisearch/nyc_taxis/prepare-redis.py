import csv
import tqdm

# Input and output file paths
input_file = "1M-nyc_taxis-hashes.redisearch.commands.ALL.csv"
output_file_search = (
    "1M-nyc_taxis-hashes.redisearch.commands.pickup_location_long_lat.csv"
)
output_file_redis = (
    "1M-nyc_taxis-hashes.redis.commands.geoadd.pickup_location_long_lat.csv"
)

# Open the input file
with open(input_file, "r") as infile, open(
    output_file_search, "w", newline=""
) as outfile_search, open(output_file_redis, "w", newline="") as outfile_redis:
    reader = csv.reader(infile)
    writer_search = csv.writer(outfile_search)
    writer_redis = csv.writer(outfile_redis)

    # Iterate through rows in the input file
    for row in tqdm.tqdm(reader):
        doc_id = row[4]
        pickup_location = row[10]
        pickup_location_lat = pickup_location.split(",")[0]
        pickup_location_lon = pickup_location.split(",")[1]
        writer_search.writerow(
            [
                "WRITE",
                "W1",
                "1",
                "HSET",
                doc_id,
                "pickup_location_long_lat",
                pickup_location,
            ]
        )
        writer_redis.writerow(
            [
                "WRITE",
                "W1",
                "1",
                "GEOADD",
                "pickup_location_long_lat",
                pickup_location_lat,
                pickup_location_lon,
                 doc_id,
            ]
        )

print(f"Extracted pickup_location_long_lat data saved to {output_file_search}")
print(f"Extracted pickup_location_long_lat data saved to {output_file_redis}")
