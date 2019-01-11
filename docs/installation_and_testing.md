## Multi-cloud Local Cluster Installation and Testing
Here is a tutorial guiding users to get familiar with OpenSDS Multi-cloud by installing a simple containerized local environment.

### Pre-config (Ubuntu 16.04)
All the installation work is tested on Ubuntu 16.04, please make sure you have installed the right one. Root user is suggested for the installation.

* packages

Install following packages:
```bash
apt-get install -y libltdl7 libseccomp2 git curl wget make
```

* docker

Install docker:
```bash
wget https://download.docker.com/linux/ubuntu/dists/xenial/pool/stable/amd64/docker-ce_18.03.1~ce-0~ubuntu_amd64.deb
dpkg -i docker-ce_18.03.1~ce-0~ubuntu_amd64.deb 
```
* golang

Check golang version information:
```bash
root@proxy:~# go version
go version go1.11.2 linux/amd64
```
You can install golang by executing commands below:
```bash
wget https://storage.googleapis.com/golang/go1.11.2.linux-amd64.tar.gz
tar -C /usr/local -xzf go1.11.2.linux-amd64.tar.gz
mkdir 
echo 'export PATH=$PATH:/usr/local/go/bin' >> /etc/profile
echo 'export GOPATH=$HOME/gopath' >> /etc/profile
source /etc/profile
```
* docker-compose

Download and install the latest version of Docker Compose:
```bash
curl -L "https://github.com/docker/compose/releases/download/1.22.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose

# docker-compose --version
docker-compose version 1.22.0, build 1719ceb
```

###	Install and start OpenSDS Multi-cloud Server
Download the source code:
```bash
mkdir -p /root/gopath/src/github.com/opensds/
git clone https://github.com/opensds/multi-cloud.git
cd multi-cloud
```

Make and build the docker image
```bash
make docker
```

Execute `docker-compose up -d` to start multi-cloud local cluster, the successful result is supposed to be as follows: 
```bash
# docker-compose up -d

Creating multi-cloud_s3_1        ... done
Creating multi-cloud_datastore_1 ... done
Creating multi-cloud_api_1       ... done
Creating multi-cloud_backend_1   ... done
Creating multi-cloud_zookeeper_1 ... done
Creating multi-cloud_kafka_1     ... done
Creating multi-cloud_dataflow_1  ... done
Creating multi-cloud_datamover_1 ... done
```

You can also run the command `docker ps` to check the installation status.
```bash
# docker ps

CONTAINER ID        IMAGE                             COMMAND                  CREATED             STATUS              PORTS                                                NAMES
dc5f0f2f1ba2        opensdsio/multi-cloud-datamover   "/datamover"             15 minutes ago      Up 15 minutes                                                            multi-cloud_datamover_1
21b9a626f0b9        opensdsio/multi-cloud-dataflow    "/dataflow"              15 minutes ago      Up 15 minutes                                                            multi-cloud_dataflow_1
300d1ad826f6        wurstmeister/kafka                "start-kafka.sh"         15 minutes ago      Up 15 minutes       0.0.0.0:9092->9092/tcp                               multi-cloud_kafka_1
f1d81d7c0755        wurstmeister/zookeeper            "/bin/sh -c '/usr/sb…"   15 minutes ago      Up 15 minutes       22/tcp, 2888/tcp, 3888/tcp, 0.0.0.0:2181->2181/tcp   multi-cloud_zookeeper_1
2a6dad2ce1a7        opensdsio/multi-cloud-backend     "/backend"               15 minutes ago      Up 15 minutes                                                            multi-cloud_backend_1
e16f9c8af2b8        opensdsio/multi-cloud-s3          "/s3"                    15 minutes ago      Up 15 minutes                                                            multi-cloud_s3_1
d86ebaed8203        mongo                             "docker-entrypoint.s…"   15 minutes ago      Up 15 minutes       0.0.0.0:27017->27017/tcp                             multi-cloud_datastore_1
1063e7ed784c        opensdsio/multi-cloud-api         "/api"                   15 minutes ago      Up 15 minutes       0.0.0.0:8089->8089/tcp                               multi-cloud_api_1

```

### Test

1. Register AWS S3 as a storage backend
```bash
#Request
curl -H "Content-type: application/json" -X POST -d '
{
    "type": "aws-s3", 
    "endpoint": "s3.amazonaws.com", 
    "name": "awss3", 
    "region": "ap-northeast-1", 
    "bucketName": "wbtestbuckt", 
    "access": "access", 
    "security": "security"
}'  http://127.0.0.1:8089/v1/adminTenantId/backends

# Response

{
 "id": "5bd94a83d84b8000014a1309",
 "name": "awss3",
 "type": "aws-s3",
 "region": "ap-northeast-1",
 "endpoint": "s3.amazonaws.com",
 "bucketName": "wbtestbuckt",
 "access": "access",
 "security": "security"
}
```

2. Create bucket on a storage backend
```bash
## Request
curl -H "Content-type: application/xml" -X PUT -d '
<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <LocationConstraint>awss3</LocationConstraint>
</CreateBucketConfiguration>
' http://127.0.0.1:8089/v1/s3/bucket001

## Response
<?xml version="1.0" encoding="UTF-8"?>
 <BaseResponse>
  <ErrorCode></ErrorCode>
  <Msg>Create bucket successfully.</Msg>
  <XXX_NoUnkeyedLiteral></XXX_NoUnkeyedLiteral>
  <XXX_unrecognized></XXX_unrecognized>
  <XXX_sizecache>0</XXX_sizecache>
 </BaseResponse>
```

3. Upload object to a bucket
```bash
## before upload create a files firstly.
echo "Hello World." > test.txt
## Request
curl -H "Content-type: application/xml" -X PUT -T "test.txt" http://127.0.0.1:8089/v1/s3/bucket001/test.txt

## Response
<?xml version="1.0" encoding="UTF-8"?>
 <BaseResponse>
  <ErrorCode></ErrorCode>
  <Msg>Create object successfully.</Msg>
  <XXX_NoUnkeyedLiteral></XXX_NoUnkeyedLiteral>
  <XXX_unrecognized></XXX_unrecognized>
  <XXX_sizecache>0</XXX_sizecache>
 </BaseResponse>
```

4. Download object from a bucket
```bash
## Request
curl -H "Content-type: application/xml" -X GET -o "text.txt"  http://127.0.0.1:8089/v1/s3/bucket001/test.txt

## Response
## text.txt will be downloaded successfully.
```
