#! /usr/bin/env python


import os
import sys
import json
import logging
import signal
from argparse import ArgumentParser
from collections import namedtuple
from threading import Thread
from time import sleep


import pika
import requests
from mesos.interface import Scheduler as MesosScheduler
from mesos.interface import mesos_pb2 as MesosPb2
from mesos.native import MesosSchedulerDriver


logger = None
SYSTEM = 'MESOS_DEMO'
COMPONENT = 'job-manager'

shutdown = None
accept_offers = None
driver = None


# Standard logging filter for using Mesos
class LoggingFilter(logging.Filter):
    def __init__(self, system='', component=''):
        super(LoggingFilter, self).__init__()

        self.system = system
        self.component = component

    def filter(self, record):
        record.system = self.system
        record.component = self.component

        return True


# Standard logging formatter with special execption formatting
class ExceptionFormatter(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None):
        std_fmt = ('%(asctime)s.%(msecs)03d'
                   ' %(levelname)-8s'
                   ' %(system)s'
                   ' %(component)s'
                   ' %(message)s')
        std_datefmt = '%Y-%m-%dT%H:%M:%S'

        if fmt is not None:
            std_fmt = fmt

        if datefmt is not None:
            std_datefmt = datefmt

        super(ExceptionFormatter, self).__init__(fmt=std_fmt,
                                                 datefmt=std_datefmt)

    def formatException(self, exc_info):
        result = super(ExceptionFormatter, self).formatException(exc_info)
        return repr(result)

    def format(self, record):
        s = super(ExceptionFormatter, self).format(record)
        if record.exc_text:
            s = s.replace('\n', ' ')
            s = s.replace('\\n', ' ')
        return s


# Configure the message logging components
def setup_logging(cfg):

    global logger

    # Setup the logging level
    logging_level = logging.INFO
    if cfg.debug:
        logging_level = cfg.debug

    handler = logging.StreamHandler(sys.stdout)
    msg_formatter = ExceptionFormatter()
    msg_filter = LoggingFilter(SYSTEM, COMPONENT)

    handler.setFormatter(msg_formatter)
    handler.addFilter(msg_filter)

    logger = logging.getLogger()
    logger.setLevel(logging_level)
    logger.addHandler(handler)


def retrieve_command_line():
    """Read and return the command line arguments
    """

    description = 'Mesos-Demo Job-Manager'
    parser = ArgumentParser(description=description)

    parser.add_argument('--starting-job-number',
                        action='store',
                        dest='starting_job_number',
                        required=False,
                        default=1,
                        help='Start incrementing from this number')

    parser.add_argument('--debug',
                        action='store',
                        dest='debug',
                        required=False,
                        type=int,
                        default=0,
                        choices=[0,10,20,30,40,50],
                        metavar='DEBUG_LEVEL',
                        help='Log debug messages')

    return parser.parse_args()


class Shutdown():
    # Framework shutdown control

    def __init__(self):
        self.flag = False
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    def shutdown(self, signum, frame):
        self.flag = True
        logger.info('Shutdown Requested - Signal')


class AcceptOffers():
    # Flag to set the driver to decline offers

    def __init__(self):
        self.flag = True

    def decline_offers(self, signum, frame):
        self.flag = False
        logger.info('Decline Offers Requested')

    def accept_offers(self, signum, frame):
        self.flag = True
        logger.info('Accept Offers Requested')


WorkInfo = namedtuple('WorkInfo',
                      ('method_frame', 'header_frame', 'body'))

class Work(object):

    def __init__(self, job_number, cfg, info):
        self.job_number = job_number
        self.cfg = cfg
        self.info = info
        self.container_info = dict()

    def check_resources(self, cpus, mem, disk):
        """Compare the work requirements against the offered resources
        """

        if (self.info.body['cpus'] <= cpus and
                self.info.body['mem'] <= mem and
                self.info.body['disk'] <= disk):
            return True

        return False

    def build_command_line(self):
        """Create the command line we will call
        """

        cmd = list()

        for part in self.info.body['command']:
            cmd.append(part)

        logger.info('Created Command: {}'.format(' '.join(cmd)))

        return ' '.join(cmd)

    def get_task_id(self):
        """For job identification within Mesos.  Must be unique between all of
           the jobs running withing the Mesos cluster.
        """

        return ('{}-{}-{}'.format(SYSTEM, self.info.body['id'], self.job_number))

    def get_task_name(self):
        """For job identification within Mesos
        """

        return ('{} {} {}'.format(SYSTEM, self.info.body['id'], self.job_number))

    def make_task(self, offer):
        """Create a Mesos task from the job information
        """

        # Create the container object
        container = MesosPb2.ContainerInfo()
        container.type = 1  # MesosPb2.ContainerInfo.Type.DOCKER

        # Create container volumes
        #volume = container.volumes.add()
        #volume.host_path = 'host/path'
        #volume.container_path = 'container/path'
        #volume.mode = 1  # MesosPb2.Volume.Mode.RW
        #volume.mode = 2  # MesosPb2.Volume.Mode.RO

        # Specify container Docker Image
        docker_cfg = self.info.body['docker']
        docker = MesosPb2.ContainerInfo.DockerInfo()
        docker.image = ':'.join([docker_cfg['image'], docker_cfg['tag']])
        ##docker.network = 2  # MesosPb2.ContainerInfo.DockerInfo.BRIDGE
        docker.network = 1  # MesosPb2.ContainerInfo.DockerInfo.HOST
        docker.force_pull_image = False

        # Specify who to run as within the Docker Container
        user_param = docker.parameters.add()
        user_param.key = 'user'
        user_param.value = '{}:{}'.format(self.cfg.docker.user_id,
                                          self.cfg.docker.group_id)

        # Specify the working directory
        # Typically you will want this to be /mnt/mesos/sandbox
        workdir_param = docker.parameters.add()
        workdir_param.key = 'workdir'
        workdir_param.value = self.cfg.docker.workdir

        # Add the Docker information to the container
        container.docker.MergeFrom(docker)

        # Create the task object
        task = MesosPb2.TaskInfo()
        task.task_id.value = self.get_task_id()
        task.slave_id.value = offer.slave_id.value
        task.name = self.get_task_name()

        # Add the container
        task.container.MergeFrom(container)

        # Specify the command line to execute within the Docker container
        command = MesosPb2.CommandInfo()
        command.value = self.build_command_line()
        command.user = self.cfg.mesos.user

        # Add the docker uri for logging into the remote repository
        #command.uris.add().value = self.cfg.docker.cfg

        # Add any required environment variables
        variable = command.environment.variables.add()
        variable.name = 'DEMO_WORKER_STANDARD_RANGE'
        variable.value = self.cfg.worker.std_range

        variable = command.environment.variables.add()
        variable.name = 'DEMO_WORKER_VARIATION_RANGE'
        variable.value = self.cfg.worker.var_range

        variable = command.environment.variables.add()
        variable.name = 'DEMO_WORKER_VARIATION_PERCENTAGE'
        variable.value = self.cfg.worker.var_percent

        '''
        The MergeFrom allows to create an object then to use this object
        in another one.  Here we use the new CommandInfo object and specify
        to use this instance for the parameter task.command.
        '''
        task.command.MergeFrom(command)

        # Setup the resources we are claiming
        cpus = task.resources.add()
        cpus.name = 'cpus'
        cpus.type = MesosPb2.Value.SCALAR
        cpus.scalar.value = self.info.body['cpus']

        mem = task.resources.add()
        mem.name = 'mem'
        mem.type = MesosPb2.Value.SCALAR
        mem.scalar.value = self.info.body['mem']

        disk = task.resources.add()
        disk.name = 'disk'
        disk.type = MesosPb2.Value.SCALAR
        disk.scalar.value = self.info.body['disk']

        return task

    def set_container_info(self, container_info):

        self.container_info = container_info


class OurMesosScheduler(MesosScheduler):
    """Implements a Mesos framework scheduler
    """

    def __init__(self, implicitAcknowledgements, executor, cfg):
        """Scheduler initialization

        Args:
            implicitAcknowledgements <int>: Input (Mesos API)
            executor <ExecutorInfo>: Input (Mesos API)
            cfg <ConfigInfo>: Input (Configuration)
        """

        self.implicitAcknowledgements = implicitAcknowledgements
        self.executor = executor

        self.tasks_launched = 0
        self.active_tasks = dict()

        self.framework_id = None
        self.master_node = None
        self.master_port = None
        self.agents_url = None
        self.tasks_url = None

        self.cfg = cfg

    def have_work(self):
        """Implemented in child
        """

        raise NotImplementedError('[{}] Requires implementation in the child'
                                  ' class'.format(self.have_work.__name__))

    def get_work(self):
        """Implemented in child
        """

        raise NotImplementedError('[{}] Requires implementation in the child'
                                  ' class'.format(self.get_work.__name__))

    def ack_work(self, work):
        """Implemented in child
        """

        raise NotImplementedError('[{}] Requires implementation in the child'
                                  ' class'.format(self.ack_work.__name__))

    def nack_work(self, work):
        """Implemented in child
        """

        raise NotImplementedError('[{}] Requires implementation in the child'
                                  ' class'.format(self.nack_work.__name__))

    def send_status(self, work):
        """Implemented in child
        """

        raise NotImplementedError('[{}] Requires implementation in the child'
                                  ' class'.format(self.send_status.__name__))

    def disposition_state(self, state):
        """Implemented in child
        """

        raise NotImplementedError('[{}] Requires implementation in the child'
                                  ' class'.format(self.disposition_state
                                                  .__name__))

    def get_mesos_endpoint(self, url):
        """Returns the json content from the specified url as a dictionary
        """

        session = requests.Session()

        session.mount('http://', requests.adapters.HTTPAdapter(max_retries=3))
        session.mount('https://', requests.adapters.HTTPAdapter(max_retries=3))

        req = session.get(url=url, verify=False)
        if not req.ok:
            logger.error('HTTP Content Retrieval - Failed')
            req.reaise_for_status()

        data = json.loads(req.content)

        del req
        del session

        return data

    def get_agent_hostname(self, agent_id):
        """Retrieves the hostname for the specified Agent ID

        Args:
            agent_id <str>: Agent ID to ge the hostname for

        Returns:
            <str>: Hostname for the Agent
        """

        data = self.get_mesos_endpoint(url=self.agents_url)

        for agent in data['slaves']:
            if agent['id'] == agent_id:
                return agent['hostname']

        return None

    def get_agent_id(self, framework_id, task_id):
        """Retrieves the Agent ID information for the specified Task ID

        Args:
            framework_id <str>: Framework ID for the Task ID
            task_id <str>: Task ID we are looking for

        Returns:
            <str>: Agent ID
        """

        data = self.get_mesos_endpoint(url=self.tasks_url)

        for task in data['tasks']:
            if (task['framework_id'] == framework_id and task['id'] == task_id):

                return task['slave_id']

        return None

    def registered(self, driver, frameworkId, masterInfo):
        """The framework was registered so log it

        Args:
            driver <MesosSchedulerDriver>: Input (Mesos API)
            frameworkId <?>: Input (Mesos API)
            masterInfo <?>: Input (Mesos API)
        """

        logger.info('Registered with framework ID {} on {}:{}'
                    .format(frameworkId.value, masterInfo.hostname,
                            masterInfo.port))

        # Update state
        self.framework_id = frameworkId.value
        self.master_node = masterInfo.hostname
        self.master_port = masterInfo.port
        self.agents_url = 'http://{}:{}/slaves'.format(self.master_node, self.master_port)
        self.tasks_url = 'http://{}:{}/tasks'.format(self.master_node, self.master_port)

    def reregistered(self, driver, masterInfo):
        """The framework was re-registered so log it

        Args:
            driver <MesosSchedulerDriver>: Input (Mesos API)
            masterInfo <?>: Input (Mesos API)
        """

        logger.info('Re-Registered to master on {}'
                    .format(masterInfo.getHostname()))

        # Update state
        self.master_node = masterInfo.hostname
        self.agents_url = 'https://{}:{}/slaves'.format(self.master_node, self.master_port)
        self.tasks_url = 'https://{}:{}/tasks'.format(self.master_node, self.master_port)

    def resourceOffers(self, driver, offers):
        """Evaluate resource offerings and launch tasks or decline the
           offerings

        Args:
            driver <MesosSchedulerDriver>: Input (Mesos API)
            offers <?>: Input (Mesos API)
        """

        # If a shutdown request has been issued or the job queue is empty,
        # there's nothing more to do, so decline all offers
        if shutdown.flag:
            if self.cfg.debug > 1:
                logger.debug('Shutdown Requested - Declining all offers')
            for offer in offers:
                driver.declineOffer(offer.id)
            if self.tasks_launched == 0:
                self._stop_event.set()
            return

        for offer in offers:
            if shutdown.flag:
                if self.cfg.debug > 1:
                    logger.debug('Shutdown Requested - Declining offer')
                driver.declineOffer(offer.id)
                continue

            if not self.have_work():
                if self.cfg.debug > 2:
                    logger.debug('No More Work Available - Declining offer')
                driver.declineOffer(offer.id)
                continue

            offerCpus = 0
            offerMem = 0
            offerDisk = 0

            for resource in offer.resources:
                if resource.name == 'cpus':
                    offerCpus += resource.scalar.value
                elif resource.name == 'mem':
                    offerMem += resource.scalar.value
                elif resource.name == 'disk':
                    offerDisk += resource.scalar.value

            if self.cfg.debug > 3:
                logger.debug('Offer CPU {}'.format(offerCpus))
                logger.debug('Offer MEM {}'.format(offerMem))
                logger.debug('Offer DISK {}'.format(offerDisk))

            tasks = list()
            while self.have_work():

                work = self.get_work()
                if work.check_resources(offerCpus, offerMem, offerDisk):

                    tasks.append(work.make_task(offer))

                    self.active_tasks[work.get_task_id()] = work

                    offerCpus -= work.info.body['cpus']
                    offerMem -= work.info.body['mem']
                    offerDisk -= work.info.body['disk']

                    # Send a message that we have created a task for this work
                    self.send_status(work, self.states.queued)

                else:
                    # Insufficient resources for this offer
                    # nack the work item so the next offer can try
                    self.nack_work(work)

                    # break out of this offer and move to the next.
                    break

            if len(tasks) > 0:
                driver.launchTasks(offer.id, tasks)
                self.tasks_launched += len(tasks)
            else:
                driver.declineOffer(offer.id)

            del tasks

        if self.cfg.debug > 0:
            logger.debug('Queued job count {}'.format(len(self.active_tasks)))
            logger.debug('Running job count {}'.format(self.tasks_launched))
            logger.debug('Have work {}'.format(self.have_work()))


    def statusUpdate(self, driver, status):
        """Update task status

        Args:
            driver <MesosSchedulerDriver>: Input (Mesos API)
            status <?>: Input (Mesos API)
        """

        task_id = status.task_id.value
        state = status.state

        logger.info('Task {} is in state {}'
                    .format(task_id, MesosPb2.TaskState.Name(state)))

        # Gather the container information for the running task, so it can be
        # used later during the finished or failed statuses
        if state == MesosPb2.TASK_RUNNING:
            container_info = json.loads('{{"data": {} }}'.format(status.data))
            self.active_tasks[task_id].set_container_info(container_info)
            self.send_status(self.active_tasks[task_id], self.states.running)

            if self.cfg.debug > 3:
                logger.debug('master_node {}'.format(self.master_node))
                agent_id = self.get_agent_id(self.framework_id, task_id)
                logger.debug('agent_id {}'.format(agent_id))

        if (state == MesosPb2.TASK_FINISHED or
                state == MesosPb2.TASK_LOST or
                state == MesosPb2.TASK_KILLED or
                state == MesosPb2.TASK_ERROR or
                state == MesosPb2.TASK_FAILED):

            #agent_id = self.get_agent_id(self.framework_id, task_id)
            #agent_hostname = self.get_agent_hostname(agent_id)

            #if self.cfg.debug > 3:
            #    logger.debug('agent_id {}'.format(agent_id))
            #    logger.debug('agent_hostname {}'.format(agent_hostname))

            #if task_id in self.active_tasks:
            #    if 'data' in self.active_tasks[task_id].container_info:
            #        for mount in self.active_tasks[task_id].container_info['data'][0]['Mounts']:
            #            if mount['Destination'] == '/mnt/mesos/sandbox':
            #                logger.info('Logfile Location = {}:{}'
            #                            .format(agent_hostname, mount['Source']))

            self.disposition_state(task_id, state)

            # Remove from the active tasks
            del self.active_tasks[task_id]

            self.tasks_launched -= 1

        if shutdown.flag and self.tasks_launched == 0:
            logger.info('All Tasks Accounted For')
            #if self.cfg.dev_mode:
            #    driver.stop()

    def frameworkMessage(self, driver, executorId, slaveId, message):
        """Recieved a framework message so log it

        Args:
            driver <MesosSchedulerDriver>: Input (Mesos API)
            executorId <?>: Input (Mesos API)
            slaveId <?>: Input (Mesos API)
            message <?>: Input (Mesos API)
        """

        logger.info('Received Framework Message: {} {} {}'
                    .format(executorId, slaveId, message))


WorkStateInfo = namedtuple('WorkStateInfo',
                           ('success', 'error', 'failed', 'lost',
                            'killed', 'queued', 'running'))


class OurJobScheduler(OurMesosScheduler):

    def __init__(self, cfg):

        executor = MesosPb2.ExecutorInfo()
        executor.executor_id.value = 'default'
        executor.name = cfg.mesos.executor_name

        self.cfg = cfg
        self.job_number = cfg.starting_job_number

        connection_parms = pika.connection.URLParameters(cfg.message.service_string)
        self.msg_connection = pika.BlockingConnection(connection_parms)

        self.queue_properties = pika.BasicProperties(delivery_mode=2)

        self.msg_work_channel = self.msg_connection.channel()
        self.msg_work_channel.queue_declare(queue=cfg.message.work_queue,
                                            durable=True)

        self.msg_status_channel = self.msg_connection.channel()
        self.msg_status_channel.queue_declare(queue=cfg.message.status_queue,
                                              durable=True)

        self.states = WorkStateInfo(success='SUCCESS',
                                    error='ERROR',
                                    failed='FAILED',
                                    lost='LOST',
                                    killed='KILLED',
                                    queued='QUEUED',
                                    running='RUNNING')

        super(OurJobScheduler, self).__init__(cfg.mesos.imp_ack, executor, cfg)

    def have_work(self):

        (method_frame,
         header_frame,
         body) = self.msg_work_channel.basic_get(self.cfg.message.work_queue)

        if method_frame:
            self.msg_work_channel.basic_nack(delivery_tag=method_frame.delivery_tag)
            return True

        return False

    def get_work(self):

        (method_frame,
         header_frame,
         body) = self.msg_work_channel.basic_get(self.cfg.message.work_queue)

        if method_frame:
            self.job_number += 1
            return Work(self.job_number, self.cfg,
                        WorkInfo(method_frame=method_frame,
                                 header_frame=header_frame,
                                 body=json.loads(body)))

        return None

    def ack_work(self, work):
        self.msg_work_channel.basic_ack(delivery_tag=work.info.method_frame.delivery_tag)

    def nack_work(self, work):
        self.msg_work_channel.basic_nack(delivery_tag=work.info.method_frame.delivery_tag)

    def send_status(self, work, status):
        job_id = work.info.body['id']
        message = {'id': job_id, 'status': status}
        message_json = json.dumps(message, ensure_ascii=False)
        self.msg_status_channel.basic_publish(exchange='',
                                              routing_key=self.cfg.message.status_queue,
                                              body=message_json,
                                              properties=self.queue_properties,
                                              mandatory=True)

    def disposition_state(self, task_id, state):
        # TODO TODO TODO - More research should be spent here with the
        # TODO TODO TODO - states to properly respond.

        # Send a status msg about the current state
        if state == MesosPb2.TASK_FINISHED:
            # This task finished remove the message out of the work queue
            self.send_status(self.active_tasks[task_id], self.states.success)
            self.ack_work(self.active_tasks[task_id])

        elif state == MesosPb2.TASK_ERROR:
            # This is probably a configuration issue
            self.send_status(self.active_tasks[task_id], self.states.error)
            self.nack_work(self.active_tasks[task_id])

        elif state == MesosPb2.TASK_FAILED:
            # TODO TODO TODO - Need to check the reason and send a better status
            self.send_status(self.active_tasks[task_id], self.states.failed)
            self.ack_work(self.active_tasks[task_id])

        elif (state == MesosPb2.TASK_LOST or
              state == MesosPb2.TASK_DROPPED or
              state == MesosPb2.TASK_UNREACHABLE or
              state == MesosPb2.TASK_GONE):
            # Mesos lost this task so try again
            self.send_status(self.active_tasks[task_id], self.states.lost)
            self.nack_work(self.active_tasks[task_id])

        elif state == MesosPb2.TASK_KILLED:
            # Mesos terminated this task so try again
            self.send_status(self.active_tasks[task_id], self.states.killed)
            self.nack_work(self.active_tasks[task_id])


def get_env_var(variable, default):
    result = os.environ.get(variable, default)
    if not result:
        raise RuntimeError('You must specify {} in the environment'
                           .format(variable))
    return result


def mesos_framework(cfg):
    """Establish framework information
    """

    framework = MesosPb2.FrameworkInfo()
    framework.user = cfg.mesos.user
    framework.name = cfg.mesos.framework_name
    #framework.principal = cfg.mesos.principal
    #framework.role = cfg.mesos.role

    return framework


def mesos_credentials(cfg):
    """Establish credential information
    """

    credentials = MesosPb2.Credential()
    credentials.principal = cfg.mesos.principal
    credentials.secret = cfg.mesos.secret

    return credentials


MessageInfo = namedtuple('MessageInfo',
                         ('service_string', 'work_queue', 'status_queue'))
DockerInfo = namedtuple('DockerInfo',
                        ('user_id', 'group_id', 'workdir'))
MesosInfo = namedtuple('MesosInfo',
                       ('max_jobs', 'master', 'user', 'principal', 'role', 'secret',
                        'framework_name', 'executor_name', 'imp_ack'))
WorkerInfo = namedtuple('WorkerInfo',
                        ('std_range', 'var_range', 'var_percent'))
ConfigInfo = namedtuple('ConfigInfo',
                        ('message', 'docker', 'mesos', 'worker', 'starting_job_number', 'debug'))


def get_configuration():

    # Example connection string: amqp://<username>:<password>@<host>:<port>
    msg_info = MessageInfo(service_string=get_env_var('DEMO_MSG_SERVICE_CONNECTION_STRING', None),
                           work_queue=get_env_var('DEMO_MSG_WORK_QUEUE', None),
                           status_queue=get_env_var('DEMO_MSG_STATUS_QUEUE', None))

    docker_info = DockerInfo(user_id=get_env_var('DEMO_DOCKER_USER_ID', None),
                             group_id=get_env_var('DEMO_DOCKER_GROUP_ID', None),
                             workdir=get_env_var('DEMO_DOCKER_WORKDIR', None))

    mesos_info = MesosInfo(max_jobs=get_env_var('DEMO_MESOS_MAX_JOBS', None),
                           master=get_env_var('DEMO_MESOS_MASTER', None),
                           user=get_env_var('DEMO_MESOS_USER', None),
                           principal=get_env_var('DEMO_MESOS_PRINCIPAL', None),
                           role=get_env_var('DEMO_MESOS_ROLE', None),
                           secret=get_env_var('DEMO_MESOS_SECRET', None),
                           framework_name=get_env_var('DEMO_MESOS_FRAMEWORK_NAME', None),
                           executor_name=get_env_var('DEMO_MESOS_EXECUTOR_NAME', None),
                           imp_ack=1)

    worker_info = WorkerInfo(std_range=get_env_var('DEMO_WORKER_STANDARD_RANGE', None),
                             var_range=get_env_var('DEMO_WORKER_VARIATION_RANGE', None),
                             var_percent=get_env_var('DEMO_WORKER_VARIATION_PERCENTAGE', None))


    args = retrieve_command_line()

    config_info = ConfigInfo(message=msg_info, docker=docker_info, mesos=mesos_info,
                             worker=worker_info,
                             starting_job_number=args.starting_job_number, debug=args.debug)

    return config_info


def main():

    global shutdown
    global accept_offers
    global driver

    cfg = get_configuration()

    # Configure logging
    setup_logging(cfg)

    framework = mesos_framework(cfg)
    #credentials = mesos_credentials(cfg)

    mesos_scheduler = OurJobScheduler(cfg)
    #driver = MesosSchedulerDriver(mesos_scheduler, framework,
    #                              cfg.mesos.master, cfg.mesos.imp_ack,
    #                              credentials)
    driver = MesosSchedulerDriver(mesos_scheduler, framework,
                                  cfg.mesos.master, cfg.mesos.imp_ack)

    shutdown = Shutdown()
    accept_offers = AcceptOffers()

    # driver.run() blocks, so run it in a separate thread.
    def run_driver_async():
        status = 0 if driver.run() == MesosPb2.DRIVER_STOPPED else 1

        if cfg.debug > 0:
            logger.debug('Stopping Driver')
        driver.stop()

        logger.info('Terminating Framework')
        sys.exit(status)

    framework_thread = Thread(target = run_driver_async, args = ())
    framework_thread.start()

    logger.info('Beginning Processing')

    while framework_thread.is_alive():
        # If a shutdown has been requested, suppress offers and wait for the
        # framework thread to complete.
        if shutdown.flag:
            if cfg.debug > 0:
                logger.debug('Suppressing Offers')
            driver.suppressOffers()

            while framework_thread.is_alive():
                logger.debug('Child Thread Still Alive')
                sleep(5)

            break

        # If there's no new work to be done or the max number of jobs are
        # already running, suppress offers and wait for some jobs to finish.
        if (mesos_scheduler.tasks_launched == cfg.mesos.max_jobs):
            driver.suppressOffers()

            if cfg.debug > 0:
                logger.debug('Suppressing Offers')

            # Sleep until we have room for more tasks
            while (not shutdown.flag and
                   mesos_scheduler.tasks_launched == cfg.mesos.max_jobs):
                if cfg.debug > 0:
                    logger.debug('Waiting for more available tasks')
                sleep(5)

            # Sleep until more processing is requested
            while not shutdown.flag and not mesos_scheduler.have_work():
                if cfg.debug > 0:
                    logger.debug('Waiting for more work')
                sleep(5)

            if not shutdown.flag:
                if cfg.debug > 0:
                    logger.debug('Reviving Offers')
                driver.reviveOffers()

            if shutdown.flag and mesos_scheduler.tasks_launched == 0:
                break

        if not shutdown.flag and not mesos_scheduler.have_work():
            driver.suppressOffers()

            # Sleep until more processing is requested
            while not shutdown.flag and not mesos_scheduler.have_work():
                if cfg.debug > 0:
                    logger.debug('Waiting for more work')
                sleep(5)

            if not shutdown.flag:
                if cfg.debug > 0:
                    logger.debug('Reviving Offers')
                driver.reviveOffers()

            if shutdown.flag and mesos_scheduler.tasks_launched == 0:
                break

    logger.info('Terminated Processing')


if __name__ == '__main__':
    main()
