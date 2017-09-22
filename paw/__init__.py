import logging
import logging.config
import os

from .models import MainPawWorker
from .utils import task, queue_task, generate_name_from_hostname
from . import exceptions

log_level = os.getenv('DEBUGLEVEL')
if log_level:
    LOGGER_LEVEL = log_level.upper()
    if LOGGER_LEVEL not in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']:
        LOGGER_LEVEL = 'INFO'
else:
    LOGGER_LEVEL = 'INFO'

if LOGGER_LEVEL == 'DEBUG':
    FORMAT = ('%(asctime)s [%(levelname)s] (([%(pathname)s] [%(module)s] '
              '[%(funcName)s] [%(lineno)d ])): %(message)s')
else:
    FORMAT = '%(asctime)s [%(levelname)s] : %(message)s'

LOGGING_DICT = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': FORMAT
        },
    },
    'handlers': {
        'default': {
            'level': LOGGER_LEVEL,
            'formatter': 'standard',
            'class': 'logging.StreamHandler',
            'stream': 'ext://sys.stdout',

        },
    },
    'loggers': {
        '': {
            'handlers': ['default'],
            'level': LOGGER_LEVEL,
            'propagate': True
        },
    }
}

logging.config.dictConfig(LOGGING_DICT)
