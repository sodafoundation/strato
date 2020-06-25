## Multi-cloud Local Cluster Installation and Testing
Here is a tutorial guiding users to get familiar with SODA Multi-cloud by installing a simple containerized local environment.

### Pre-config (Ubuntu 16.04/Ubuntu 18.04)
All the installation work is tested on Ubuntu 16.04 and Ubuntu 18.04, please make sure you have installed the right one. Root user is suggested for the installation.

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
SODA multi-cloud needs go version 1.13.x

Example:
```bash
root@proxy:~# go version
go version go1.13.9 linux/amd64
```
You can install golang by executing commands below:
[Using 1.13.9 here for example]
```bash
wget https://storage.googleapis.com/golang/go1.11.2.linux-amd64.tar.gz
tar -C /usr/local -xzf go1.13.9.linux-amd64.tar.gz
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
mkdir -p /root/gopath/src/github.com/sodafoundation/
git clone https://github.com/sodafoundation/multi-cloud.git
cd /root/gopath/src/github.com/sodafoundation/multi-cloud
```

Make and build the docker image
```bash
make docker
```

Execute `docker-compose up -d` to start multi-cloud local cluster, the successful result is supposed to be as follows: 
```bash
# docker-compose up -d

Creating redis                                ... done
Creating multi-cloud_block_1_87f15fb3fcf4     ... done
Creating tidb                                 ... done
Creating multi-cloud_zookeeper_1_77bbf2cb55a6 ... done
Creating multi-cloud_datastore_1_2341f44ebbba ... done
Creating multi-cloud_file_1_4885d7d3334d      ... done
Creating multi-cloud_backend_1_5ab6fd6feed4   ... done
Creating multi-cloud_s3api_1_395c90ae45c3     ... done
Creating multi-cloud_api_1_437a019a9278       ... done
Creating multi-cloud_s3_1_5cf4e4514669        ... done
Creating multi-cloud_kafka_1_ff762483f0b6     ... done
Creating multi-cloud_dataflow_1_592ca1c6f730  ... done
Creating multi-cloud_datamover_1_390d343d0e7c ... done
```

You can also run the command `docker ps` to check the installation status.
```bash
# docker ps

CONTAINER ID        IMAGE                                  COMMAND                  CREATED             STATUS              PORTS                                                NAMES
bbe74a5359eb        sodafoundation/multi-cloud-dataflow    "/dataflow"              40 seconds ago      Up 39 seconds                                                            multi-cloud_dataflow_1_8ce4de44c4a7
e18133da2d9f        sodafoundation/multi-cloud-datamover   "/datamover"             40 seconds ago      Up 39 seconds                                                            multi-cloud_datamover_1_1ae5af80e61f
ec8052d3db15        wurstmeister/kafka:2.11-2.0.1          "start-kafka.sh"         41 seconds ago      Up 39 seconds       0.0.0.0:9092->9092/tcp                               multi-cloud_kafka_1_74c5e392fd7d
5fb7560159b2        sodafoundation/multi-cloud-s3          "/initdb.sh"             41 seconds ago      Up 40 seconds                                                            multi-cloud_s3_1_59cd2512722b
b28213c1ddec        sodafoundation/multi-cloud-backend     "/backend"               42 seconds ago      Up 39 seconds                                                            multi-cloud_backend_1_a67a449c92a5
a47ec4ef770b        sodafoundation/multi-cloud-file        "/file"                  42 seconds ago      Up 40 seconds                                                            multi-cloud_file_1_cd84c1c9dad3
be2b170b2d68        sodafoundation/multi-cloud-api         "/api"                   42 seconds ago      Up 41 seconds       0.0.0.0:8090->8090/tcp                               multi-cloud_s3api_1_4698613dfd2e
f0f2419b98db        sodafoundation/multi-cloud-api         "/api"                   42 seconds ago      Up 41 seconds       0.0.0.0:8089->8089/tcp                               multi-cloud_api_1_aacf7b9a5141
ad7a85f51343        wurstmeister/zookeeper                 "/bin/sh -c '/usr/sb…"   42 seconds ago      Up 41 seconds       22/tcp, 2888/tcp, 3888/tcp, 0.0.0.0:2181->2181/tcp   multi-cloud_zookeeper_1_68df196d327a
3539d1a91298        mongo                                  "docker-entrypoint.s…"   42 seconds ago      Up 41 seconds       0.0.0.0:27017->27017/tcp                             multi-cloud_datastore_1_a2f8fb0deb9d
0722d5174b8f        pingcap/tidb:v2.1.16                   "/tidb-server --stor…"   42 seconds ago      Up 41 seconds       0.0.0.0:4000->4000/tcp, 0.0.0.0:10080->10080/tcp     tidb
e03e03d88dcb        sodafoundation/multi-cloud-block       "/block"                 42 seconds ago      Up 40 seconds                                                            multi-cloud_block_1_7adafa5c5b8d
edc2036f5e1f        redis                                  "docker-entrypoint.s…"   42 seconds ago      Up 41 seconds       0.0.0.0:6379->6379/tcp                               redis
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
}'  http://127.0.0.1:8089/v1/<adminTenantId>/backends

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
' http://127.0.0.1:8090/bucket001

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
curl -H "Content-type: application/xml" -X PUT -T "test.txt" http://127.0.0.1:8090/bucket001/test.txt

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
curl -H "Content-type: application/xml" -X GET -o "text.txt"  http://127.0.0.1:8090bucket001/test.txt

## Response
## text.txt will be downloaded successfully.
```
