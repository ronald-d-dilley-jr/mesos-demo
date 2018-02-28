sudo true
demo_cmd="sudo /sbin/mesos-slave --master=zk://127.0.0.1:2181/mesos --work_dir=/home/rdilley/mesos/DEMO_PORT/ --port=DEMO_PORT --containerizers=docker --docker_remove_delay=1mins --resources=file://$HOME/src/mesos-demo/scripts/agent.conf"

port=5051
cmd=`echo ${demo_cmd} | sed -e "s/DEMO_PORT/${port}/g"`
echo ${cmd}
${cmd} &

port=5052
cmd=`echo ${demo_cmd} | sed -e "s/DEMO_PORT/${port}/g"`
echo ${cmd}
${cmd} &

port=5053
cmd=`echo ${demo_cmd} | sed -e "s/DEMO_PORT/${port}/g"`
echo ${cmd}
${cmd} &

port=5054
cmd=`echo ${demo_cmd} | sed -e "s/DEMO_PORT/${port}/g"`
echo ${cmd}
${cmd} &

port=5055
cmd=`echo ${demo_cmd} | sed -e "s/DEMO_PORT/${port}/g"`
echo ${cmd}
${cmd} &

port=5056
cmd=`echo ${demo_cmd} | sed -e "s/DEMO_PORT/${port}/g"`
echo ${cmd}
${cmd} &

port=5057
cmd=`echo ${demo_cmd} | sed -e "s/DEMO_PORT/${port}/g"`
echo ${cmd}
${cmd} &

port=5058
cmd=`echo ${demo_cmd} | sed -e "s/DEMO_PORT/${port}/g"`
echo ${cmd}
${cmd} &

port=5059
cmd=`echo ${demo_cmd} | sed -e "s/DEMO_PORT/${port}/g"`
echo ${cmd}
${cmd} &

port=5060
cmd=`echo ${demo_cmd} | sed -e "s/DEMO_PORT/${port}/g"`
echo ${cmd}
${cmd} &
