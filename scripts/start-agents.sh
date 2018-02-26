port=5051
sudo /sbin/mesos-slave --master=zk://127.0.0.1:2181/mesos --work_dir=/home/rdilley/mesos/${port}/ --port=${port} --containerizers=docker --resources=file:///home/rdilley/agent.conf &
port=5052
sudo /sbin/mesos-slave --master=zk://127.0.0.1:2181/mesos --work_dir=/home/rdilley/mesos/${port}/ --port=${port} --containerizers=docker --resources=file:///home/rdilley/agent.conf &
port=5053
sudo /sbin/mesos-slave --master=zk://127.0.0.1:2181/mesos --work_dir=/home/rdilley/mesos/${port}/ --port=${port} --containerizers=docker --resources=file:///home/rdilley/agent.conf &
port=5054
sudo /sbin/mesos-slave --master=zk://127.0.0.1:2181/mesos --work_dir=/home/rdilley/mesos/${port}/ --port=${port} --containerizers=docker --resources=file:///home/rdilley/agent.conf &
port=5055
sudo /sbin/mesos-slave --master=zk://127.0.0.1:2181/mesos --work_dir=/home/rdilley/mesos/${port}/ --port=${port} --containerizers=docker --resources=file:///home/rdilley/agent.conf &
