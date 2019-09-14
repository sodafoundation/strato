cp ../pkg/examples/policy.json .
CGO_ENABLED=0 GOOS=linux go build -gcflags "all=-N -l" -ldflags '-extldflags "-static"' -o ./api-debug github.com/opensds/multi-cloud/api/cmd
chmod 755 api-debug
docker build -t opensdsio/multi-cloud-api-debug:latest .