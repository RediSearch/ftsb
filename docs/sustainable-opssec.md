
#### Sustainable Throughput benchmark
To really understand a system behavior we also can't relay solely on doing the full percentile analysis while stressing the system to it's maximum RPS. 

We need to be able to compare the behavior under different throughput and/or configurations, to be able to get the best "Sustainable Throughput: The throughput achieved while safely maintaining service levels.
 To enabling full percentile spectrum and Sustainable Throughput analysis you can use:
- `--hdr-latencies` : enable writing the High Dynamic Range (HDR) Histogram of Response Latencies to the file with the name specified by this. By default no file will be saved.
- `--max-rps` : enable limiting the rate of queries per second, 0 = no limit. By default no limit is specified and the binaries will stress the DB up to the maximum. A normal "modus operandi" would be to initially stress the system ( no limit on RPS) and afterwards that we know the limit vary with lower rps configurations.
