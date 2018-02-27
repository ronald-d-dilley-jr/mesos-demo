sudo true
demo_cmd="sudo /sbin/mesos-slave --master=zk://127.0.0.1:2181/mesos --work_dir=/home/rdilley/mesos/DEMO_PORT/ --port=DEMO_PORT --containerizers=docker --docker_remove_delay=1min --resources=agent.conf &"

port=5051
cmd=`echo ${demo_cmd} | sed -e "s/DEMO_PORT/${port}/g"`
${cmd}
port=5052
cmd=`echo ${demo_cmd} | sed -e "s/DEMO_PORT/${port}/g"`
${cmd}
port=5053
cmd=`echo ${demo_cmd} | sed -e "s/DEMO_PORT/${port}/g"`
${cmd}
port=5054
cmd=`echo ${demo_cmd} | sed -e "s/DEMO_PORT/${port}/g"`
${cmd}
port=5055
cmd=`echo ${demo_cmd} | sed -e "s/DEMO_PORT/${port}/g"`
${cmd}
