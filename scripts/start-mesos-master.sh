sudo true
sudo rm -rf /var/lib/mesos
sudo rm -rf /home/rdilley/mesos
cmd="sudo systemctl start mesos-master"
echo ${cmd}
${cmd}
