FROM alpine:latest

RUN mkdir -p /etc/opensds
COPY  ./pkg/examples/policy.json /etc/opensds/
COPY api /api
ENTRYPOINT ["/api"]
