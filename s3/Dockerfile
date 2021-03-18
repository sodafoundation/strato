FROM centos:7.6.1810

# Configure confluent source to install librdkafka-devel 1.0.0
RUN echo $'[Confluent.dist] \n\
name=Confluent repository (dist)\n\
baseurl=https://packages.confluent.io/rpm/5.2/7\n\
gpgcheck=1\n\
gpgkey=https://packages.confluent.io/rpm/5.2/archive.key\n\
enabled=1\n\
\n\
[Confluent]\n\
name=Confluent repository\n\
baseurl=https://packages.confluent.io/rpm/5.2\n\
gpgcheck=1\n\
gpgkey=https://packages.confluent.io/rpm/5.2/archive.key\n\
enabled=1' > /etc/yum.repos.d/confluent.repo

RUN rpm --import https://packages.confluent.io/rpm/5.2/archive.key && yum makecache && \
yum install -y epel-release && \
rpm -ivh https://download.ceph.com/rpm-luminous/el7/noarch/ceph-release-1-1.el7.noarch.rpm && \
yum --enablerepo=epel-testing install -y lttng-ust libradosstriper-devel librados2-devel librdkafka-devel && \
yum -y install mysql && \
yum clean all && \
rm -rf /var/cache/yum

COPY s3 /s3
COPY ./pkg/conf/tidb.sql /etc/config/tidb/tidb.sql
COPY ./pkg/datastore/yig/conf/yig.sql /etc/config/yig/yig.sql
COPY initdb.sh /initdb.sh
ENTRYPOINT ["/initdb.sh"]
