FROM golang:1.13.5-buster AS builder

# Copy the code from the host and compile it
WORKDIR $GOPATH/src/github.com/RediSearch/ftsb
COPY . ./
RUN cd $GOPATH/src/github.com/RediSearch/ftsb/cmd && CGO_ENABLED=0 GOOS=linux go get ./...
RUN cd $GOPATH/src/github.com/RediSearch/ftsb/cmd/ftsb_generate_data && CGO_ENABLED=0 GOOS=linux go build -o /tmp/ftsb_generate_data
RUN cd $GOPATH/src/github.com/RediSearch/ftsb/cmd/ftsb_generate_queries  && CGO_ENABLED=0 GOOS=linux go build -o /tmp/ftsb_generate_queries
RUN cd $GOPATH/src/github.com/RediSearch/ftsb/cmd/ftsb_load_redisearch  && CGO_ENABLED=0 GOOS=linux go build -o /tmp/ftsb_load_redisearch
RUN cd $GOPATH/src/github.com/RediSearch/ftsb/cmd/ftsb_run_queries_redisearch  && CGO_ENABLED=0 GOOS=linux go build -o /tmp/ftsb_run_queries_redisearch

FROM golang:1.13.5-buster
ENV PATH ./:$PATH
COPY --from=builder /tmp/ftsb_generate_data ./
COPY --from=builder /tmp/ftsb_generate_queries ./
COPY --from=builder /tmp/ftsb_load_redisearch ./
COPY --from=builder /tmp/ftsb_run_queries_redisearch ./
RUN apt-get update && apt-get install redis-tools -y
RUN apt-get update && apt-get install bzip2 -y
COPY docker_entrypoint.sh ./
COPY scripts ./scripts
RUN chmod -R 751 scripts
RUN chmod 751 docker_entrypoint.sh
ENTRYPOINT ["./docker_entrypoint.sh"]