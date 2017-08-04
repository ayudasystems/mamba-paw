import json
import logging
import logging.config
import os
import time
import traceback
from inspect import getmembers, isfunction
from multiprocessing import Pool, Process, Queue

from azure.common import AzureException, AzureMissingResourceHttpError
# noinspection PyPackageRequirements
from azure.storage.queue import QueueService
# noinspection PyPackageRequirements
from azure.storage.table import TableService

from .utils import log_to_table, PAW_LOGO

SUCCESS = 'SUCCESS'
FAILED = 'FAILED'
STARTED = 'STARTED'
RETRY = 'RETRY'

log_level = os.getenv('DEBUGLEVEL')
if log_level:
    LOGGER_LEVEL = log_level.upper()
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


class Worker(Process):
    """Process that get sent to the Pool and consumes from the Queue"""
    def __init__(self, local_queue, queue_service, queue_name,
                 table_service, table_name, tasks, logger):
        """
        :param local_queue: multiprocessing.Queue
        :param queue_service: azure.storage.queue.QueueService
        :param queue_name: Name of the Azure queue to use
        :param table_service: azure.storage.table.TableService
        :param table_name: Name of the Azure table to use
        :param tasks: Dict of tasks {"task_name": Function Object}
        :param logger: Instantiated logger object
        """
        super(Worker, self).__init__()

        self.local_queue = local_queue
        self.queue_service = queue_service
        self.table_service = table_service
        self.queue_name = queue_name
        self.table_name = table_name
        self.tasks = tasks
        self.logger = logger

    def run(self):
        """ Loops to pick tasks in the local queue.
            Once a message is picked, it execute the corresponding task.
            Takes care of deleting from the queue and logging the result to
            Azure table.
        """
        self.logger.info('STARTING worker PID: {}'.format(os.getpid()))

        while True:
            content = self.local_queue.get(True)

            if not content:
                self.logger.critical('Picked empty message from local queue')
                self.delete_from_queue(
                    msg_id=content['msg'].id,
                    pop_receipt=content['msg'].pop_receipt,
                    task_name=content['task_name'],
                    job_id=content['job_id']
                )
                continue

            func = self.tasks.get(content['task_name'])
            deleted = self.delete_from_queue(
                    msg_id=content['msg'].id,
                    pop_receipt=content['msg'].pop_receipt,
                    task_name=content['task_name'],
                    job_id=content['job_id']
                )

            if not deleted:
                self.logger.critical('Message and/or entity missing. Aborting.')
                continue

            if not func:
                self.logger.critical('{} is not a registered task.'.format(
                        content['task_name']))
                log_to_table(
                    table_service=self.table_service,
                    table_name=self.table_name,
                    task_name=content['task_name'],
                    status=FAILED,
                    job_id=content['job_id'],
                    result=None,
                    exception='{} is not a registered task.'.format(
                        content['task_name']),
                    create=True
                )
                continue

            # Logging STARTED to table storage.
            log_to_table(
                table_service=self.table_service,
                table_name=self.table_name,
                task_name=content['task_name'],
                status=STARTED,
                job_id=content['job_id'],
                result=None,
                exception=None,
                create=True
            )
            exception = None
            result = None

            # Grabbing any exception caused by the task.
            # noinspection PyBroadException
            try:
                if content['args']:
                    result = func(*content['args'])
                elif content['kwargs']:
                    result = func(**content['kwargs'])
                else:
                    result = func()
            except Exception:
                exception = traceback.format_exc()
            finally:
                if exception:
                    status = FAILED
                else:
                    status = SUCCESS

                log_to_table(
                    table_service=self.table_service,
                    table_name=self.table_name,
                    task_name=content['task_name'],
                    status=status,
                    job_id=content['job_id'],
                    result=result,
                    exception=exception
                )

                self.logger.debug(
                    'Exception {}\n Result: {}'.format(exception, result)
                )

    def delete_from_queue(self, msg_id, pop_receipt, task_name, job_id):
        try:
            self.queue_service.delete_message(
                queue_name=self.queue_name,
                message_id=msg_id,
                pop_receipt=pop_receipt
            )
            return True
        except AzureMissingResourceHttpError:
            # Message doesn't exist. Cleaning table.
            try:
                self.table_service.delete_entity(
                    table_name=self.table_name,
                    partition_key=task_name,
                    row_key=job_id
                )
            except AzureMissingResourceHttpError:
                pass
            self.logger.debug("Deleting from queue failed. {}",
                              traceback.format_exc())
            return False


class MainPawWorker:
    """Main class to use for running a worker. call start_workers() to start.
    """
    def __init__(self, azure_storage_name, azure_storage_private_key,
                 azure_queue_name, azure_table_name, tasks_module, workers):
        """
        :param azure_storage_name: Name of Azure storage account
        :param azure_storage_private_key: Private key of Azure storage account.
        :param azure_queue_name: Name of the Azure queue to use.
        :param azure_table_name: Name of the Azure table to use.
        :param tasks_module: Module containing decorated functions to load from.
        :param workers: Int of workers. Ex: 4
        """
        self.account_name = azure_storage_name
        self.account_key = azure_storage_private_key
        self.queue_name = azure_queue_name
        self.table_name = azure_table_name
        self.tasks_module = tasks_module
        self.workers = workers
        self.queue_service = QueueService(account_name=self.account_name,
                                          account_key=self.account_key)
        self.table_service = TableService(account_name=self.account_name,
                                          account_key=self.account_key)
        self.local_queue = Queue(self.workers)
        self.logger = logging.getLogger()

        self.logger.info(PAW_LOGO)

        self.worker_process = Worker(
            local_queue=self.local_queue,
            queue_service=self.queue_service,
            queue_name=self.queue_name,
            table_service=self.table_service,
            table_name=azure_table_name,
            tasks=self._load_tasks(),
            logger=self.logger
        )
        self.pool = Pool(self.workers, self.worker_process.run, ())

    def _load_tasks(self):
        """Loads and returns decorated functions from a given modules, as a
           dict
        """
        tasks = dict(
            [o for o in getmembers(self.tasks_module)
             if isfunction(o[1]) and hasattr(o[1], 'paw')]
        )

        for t, f in tasks.items():
            self.logger.info("REGISTERED '{}'".format(t))
            if f.description:
                self.logger.info("\tdescription: '{}'".format(f.description))
        if not tasks:
            self.logger.warning("No tasks found...")

        return tasks

    def start_workers(self, sleep_for=5):
        """
           Starts workers and picks message from the Azure queue. On new
           message, when the local queue has room, the message is placed for a
           worker to pick-up
           :param sleep_for: Seconds to sleep for after a loop end.
        """
        self.queue_service.create_queue(self.queue_name)

        while True:
            if not self.local_queue.full():
                try:
                    new_msg = self.queue_service.get_messages(
                        queue_name=self.queue_name,
                        num_messages=1,
                        visibility_timeout=5 * (60*60)
                    )
                except AzureException:
                    self.logger.error("Error while getting message "
                                      "from Azure queue. Trying to create "
                                      "the queue")
                    self.queue_service.create_queue(self.queue_name)
                    time.sleep(sleep_for)
                    continue

                if new_msg:
                    msg = new_msg[0]
                    try:
                        content = json.loads(msg.content)
                    except json.JSONDecodeError:
                        self.logger.critical(
                            'Json error {}'.format(traceback.format_exc()))
                        try:
                            self.queue_service.delete_message(
                                queue_name=self.queue_name,
                                message_id=msg.id,
                                pop_receipt=msg.pop_receipt
                            )
                        except AzureException:
                            self.logger.critical(
                                'Deleting invalid message from queue failed: '
                                '{}'.format(traceback.format_exc()))
                        time.sleep(sleep_for)
                        continue

                    if msg.dequeue_count > 5:
                        log_to_table(
                            table_service=self.table_service,
                            table_name=self.table_name,
                            task_name=content['task_name'],
                            status=FAILED,
                            job_id=content['job_id'],
                            result="PAW MESSAGE: Dequeue count exceeded.",
                        )
                        self.queue_service.delete_message(
                            self.queue_name,
                            msg.id,
                            msg.pop_receipt
                        )
                        continue

                    content['msg'] = msg
                    self.local_queue.put_nowait(content)
                    self.logger.debug('ADDING: {}'.format(content['task_name']))
                    continue

            time.sleep(sleep_for)
