FROM alpine:latest

RUN mkdir -p /etc/multicloud
COPY  ./pkg/examples/policy.json /etc/multicloud/
COPY api /api
ENTRYPOINT ["/api"]
