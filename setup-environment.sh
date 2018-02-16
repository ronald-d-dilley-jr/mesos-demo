
export DEMO_RABBITMQ_DEFAULT_USER=admin
export DEMO_RABBITMQ_DEFAULT_PASS=admin-123

export DEMO_MSG_USERNAME=proto
export DEMO_MSG_PASSWORD=proto-123
export DEMO_MSG_WORK_QUEUE=proto-work
export DEMO_MSG_STATUS_QUEUE=proto-status
export DEMO_MSG_SERVICE_CONNECTION_STRING=amqp://$DEMO_MSG_USERNAME:$DEMO_MSG_PASSWORD@localhost:5672

export DEMO_DOCKER_USER_ID=`id -u`
export DEMO_DOCKER_GROUP_ID=`id -g`
#export DEMO_DOCKER_CFG=/path/to/dockercfg.tar.gz
export DEMO_DOCKER_WORKDIR=/mnt/mesos/sandbox

export DEMO_MESOS_MAX_JOBS=10
export DEMO_MESOS_MASTER=zk://127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183/mesos
export DEMO_MESOS_USER=demo
export DEMO_MESOS_PRINCIPAL=demo
export DEMO_MESOS_ROLE=demo
export DEMO_MESOS_SECRET=randomnoise
export DEMO_MESOS_FRAMEWORK_NAME="Prototype Framework"
export DEMO_MESOS_EXECUTOR_NAME="Prototype Executor"
