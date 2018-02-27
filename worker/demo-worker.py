#! /usr/bin/env python3


import os
import sys
import logging
import random
import time


logger = None
SYSTEM = 'MESOS_DEMO'
COMPONENT = 'worker'


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
def setup_logging():

    global logger

    # Setup the logging level
    logging_level = logging.INFO

    handler = logging.StreamHandler(sys.stdout)
    msg_formatter = ExceptionFormatter()
    msg_filter = LoggingFilter(SYSTEM, COMPONENT)

    handler.setFormatter(msg_formatter)
    handler.addFilter(msg_filter)

    logger = logging.getLogger()
    logger.setLevel(logging_level)
    logger.addHandler(handler)


def get_env_var(variable, default):
    result = os.environ.get(variable, default)
    if not result:
        raise RuntimeError('You must specify {} in the environment'
                           .format(variable))
    return result


def main():

    # Configure logging
    setup_logging()

    seed = get_env_var('DEMO_WORKER_SEED', time.gmtime())
    standard_range = int(get_env_var('DEMO_WORKER_STANDARD_RANGE', None))
    variation_range = int(get_env_var('DEMO_WORKER_VARIATION_RANGE', None))
    variation_percentage = int(get_env_var('DEMO_WORKER_VARIATION_PERCENTAGE', None))

    random.seed(seed)
    guess = random.randint(1, 100)

    if guess >= (100 - variation_percentage):
        seconds = standard_range + random.randint(1, variation_range)
    else:
        seconds = random.randint(1, standard_range)

    logger.info('Starting to sleep {} seconds'.format(seconds))
    time.sleep(seconds)
    logger.info('Finished sleeping')


if __name__ == '__main__':
    main()
