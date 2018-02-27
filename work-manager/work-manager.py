#! /usr/bin/env python3

"""This is a prototype work manager which reads work requests from a file and
   submits them as messages to a RabbitMQ queue.

   This is development only.  For a real system, you would get work from a
   database or other entity.
"""

import os
import sys
import json
import logging
from argparse import ArgumentParser
from time import sleep


import pika


logger = None
SYSTEM = 'PROTO'
COMPONENT = 'work-manager'


MSG_SERVICE_STRING = None
MSG_WORK_QUEUE = None
MSG_STATUS_QUEUE = None


class LoggingFilter(logging.Filter):
    """Standard logging filter for using Mesos
    """

    def __init__(self, system='', component=''):
        super(LoggingFilter, self).__init__()

        self.system = system
        self.component = component

    def filter(self, record):
        record.system = self.system
        record.component = self.component

        return True


class ExceptionFormatter(logging.Formatter):
    """Standard logging formatter with special execption formatting
    """

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


def setup_logging(args):
    """Configure the message logging components
    """

    global logger

    # Setup the logging level
    logging_level = logging.INFO
    if args.debug:
        logging_level = args.debug

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

    description = 'Prototype Work Manager'
    parser = ArgumentParser(description=description)

    parser.add_argument('--job-filename',
                        action='store',
                        dest='job_filename',
                        required=False,
                        metavar='TEXT',
                        help='JSON job file to use')

    parser.add_argument('--dev-mode',
                        action='store_true',
                        dest='dev_mode',
                        required=False,
                        default=False,
                        help='Run in developer mode')

    parser.add_argument('--debug',
                        action='store',
                        dest='debug',
                        required=False,
                        type=int,
                        default=0,
                        metavar='DEBUG_LEVEL',
                        help='Log debug messages')

    return parser.parse_args()


def get_env_var(variable, default):
    """Read variable from the environment and provide a default value
    """

    result = os.environ.get(variable, default)
    if not result:
        raise RuntimeError('You must specify {} in the environment'
                           .format(variable))
    return result


def get_jobs(job_filename):
    """Reads jobs from a known job file location
    """

    jobs = list()

    if job_filename and os.path.isfile(job_filename):
        with open(job_filename, 'r') as input_fd:
            data = input_fd.read()

        job_dict = json.loads(data)
        del data

        for job in job_dict['jobs']:
            jobs.append(job)

        os.unlink(job_filename)

    return jobs


def main():
    """Main processing for the application
    """

    global MSG_SERVICE_STRING
    global MSG_WORK_QUEUE
    global MSG_STATUS_QUEUE

    # Example connection string: amqp://<username>:<password>@<host>:<port>
    MSG_SERVICE_STRING = get_env_var('PROTO_MSG_SERVICE_CONNECTION_STRING', None)
    MSG_WORK_QUEUE = get_env_var('PROTO_MSG_WORK_QUEUE', None)
    MSG_STATUS_QUEUE = get_env_var('PROTO_MSG_STATUS_QUEUE', None)

    args = retrieve_command_line()

    # Configure logging
    setup_logging(args)

    # Create the connection parameters
    connection_parms = pika.connection.URLParameters(MSG_SERVICE_STRING)

    queue_properties = pika.BasicProperties(delivery_mode=2)

    logger.info('Beginning Processing')

    try:
        while True:
            # Create the connection
            with pika.BlockingConnection(connection_parms) as connection:
                # Open a channel
                with connection.channel() as channel:

                    # Create/assign the queue to use
                    channel.queue_declare(queue=MSG_WORK_QUEUE, durable=True)

                    jobs = get_jobs(args.job_filename)
                    for job in jobs:
                        message_json = json.dumps(job, ensure_ascii=False)

                        try:
                            channel.basic_publish(exchange='',
                                                  routing_key=MSG_WORK_QUEUE,
                                                  body=message_json,
                                                  properties=queue_properties,
                                                  mandatory=True)

                            # TODO - This prototype doesn't care, but we
                            # TODO -   should probably update the status at
                            # TODO -   the work source.
                            print('Queued Message = {}'.format(message_json))
                        except pika.exceptions.ChannelClosed:
                            # TODO - This prototype doesn't care, but does
                            # TODO -   something need to be done if this
                            # TODO -   happens?
                            print('Returned Message = {}'.format(message_json))

            sleep(60)

    except KeyboardInterrupt:
        pass
    except pika.exceptions.ConnectionClosed:
        pass

    logger.info('Terminated Processing')


if __name__ == '__main__':
    main()
