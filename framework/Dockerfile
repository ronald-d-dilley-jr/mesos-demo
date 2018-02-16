FROM centos:7.3.1611


LABEL description="(Processing Architecture) Mesos Framework"


# Update the OS packages to the latest versions
RUN yum update -y \
    && yum clean all


# Install the epel so we can get some good stuff
RUN yum install -y \
        epel-release \
    && yum clean all


# Initial Python Configuration
RUN yum install -y \
        python-pip \
        python-devel \
    && yum clean all


# Update pip
RUN pip install --upgrade pip


# The latest versions of Mesos require svn development package >= 1.9
COPY wandisco-svn.repo /etc/yum.repos.d/wandisco-svn.repo

# Install the mesosphere repo and install mesos from that repo
RUN yum -y install http://repos.mesosphere.io/el/7/noarch/RPMS/mesosphere-el-repo-7-3.noarch.rpm \
    && yum install -y mesos \
    && yum clean all


# Install python access to the protobuf
RUN pip install protobuf requests pika


COPY job-manager.py /usr/local/bin/


ENTRYPOINT ["/usr/local/bin/job-manager.py"]
