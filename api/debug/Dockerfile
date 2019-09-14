FROM golang:1.10-alpine

ENV GOPATH /opt/go:$GOPATH
ENV PATH /opt/go/bin:$PATH
WORKDIR /opt/go/src/github.com/opensds/multi-cloud

RUN mkdir -p /etc/opensds
RUN mkdir -p /opt/go/src
RUN apk add --no-cache ca-certificates dpkg
RUN apk add --no-cache ca-certificates gcc
RUN apk add --no-cache ca-certificates git
RUN apk add --no-cache ca-certificates musl-dev
RUN go get github.com/derekparker/delve/cmd/dlv

COPY  ./policy.json /etc/opensds/
COPY ./api-debug /api-debug

ADD ./api-debug /

CMD ["./api-debug"]
