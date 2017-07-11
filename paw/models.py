import json
import logging
import logging.config
import os
import time
import traceback
from inspect import getmembers, isfunction
from multiprocessing import Pool, Process, Queue

# noinspection PyPackageRequirements
from azure.storage.queue import QueueService
# noinspection PyPackageRequirements
from azure.storage.table import TableService

from .utils import log_to_table

SUCCESS = 'SUCCESS'
FAILED = 'FAILED'
STARTED = 'STARTED'
RETRY = 'RETRY'

LOGGING_DICT = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
        },
    },
    'handlers': {
        'default': {
            'level': 'INFO',
            'formatter': 'standard',
            'class': 'logging.StreamHandler',
            'stream': 'ext://sys.stdout',

        },
    },
    'loggers': {
        '': {
            'handlers': ['default'],
            'level': 'INFO',
            'propagate': True
        },
    }
}






# logging_ini = os.path.join(
#     os.path.dirname(os.path.abspath(__file__)), 'logging.ini'
# )
logging.config.dictConfig(LOGGING_DICT)
# logging.config.fileConfig(logging_ini)


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
        self.qs = queue_service
        self.ts = table_service
        self.azure_queue_name = queue_name
        self.azure_table_name = table_name
        self.tasks = tasks
        self.logger = logger

    def run(self):
        """ Loops to pick tasks in the local queue.
            Once a message is picked, it execute the corresponding task.
            Takes care of deleting from the queue and logging the result to
            Azure table.
        """
        self.logger.info(
            'STARTING worker PID: {}'.format(os.getpid())
        )

        while True:
            content = self.local_queue.get(True)
            if not content:
                raise Exception('Picked empty message from local queue')

            func = self.tasks.get(content['task_name'])
            try:
                self.qs.delete_message(
                    self.azure_queue_name,
                    content['msg'].id,
                    content['msg'].pop_receipt
                )
            except Exception:
                # Deleting table entity to try to keep it clean. The job is
                # still in the queue but could not be deleted. Could means it
                # expired, already deleted, or re-queued etc...
                #
                # This should not happen, so we're still re-raising the
                # exception,
                self.ts.delete_entity(self.azure_table_name,
                                      content['task_name'],
                                      content['job_id'])
                raise

            if not func:
                log_to_table(
                    table_service=self.ts,
                    table_name=self.azure_table_name,
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
                table_service=self.ts,
                table_name=self.azure_table_name,
                task_name=content['task_name'],
                status=STARTED,
                job_id=content['job_id'],
                result=None,
                exception=None,
                create=True
            )
            exception = None
            result = None

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
                    table_service=self.ts,
                    table_name=self.azure_table_name,
                    task_name=content['task_name'],
                    status=status,
                    job_id=content['job_id'],
                    result=result,
                    exception=exception
                )
                self.logger.info(
                    'ExceptionP {} | Result: {}'.format(exception, result)
                )


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

        return tasks

    def start_workers(self):
        """
           Starts workers and picks message from the Azure queue. On new
           message, when the local queue has room, the message is placed for a
           worker to pick-up
        """
        self.queue_service.create_queue(self.queue_name)

        while True:
            if not self.local_queue.full():
                # noinspection PyBroadException
                try:
                    new_msg = self.queue_service.get_messages(
                        queue_name=self.queue_name,
                        num_messages=1,
                        visibility_timeout=5 * (60*60)
                    )
                    if new_msg:
                        msg = new_msg[0]
                        content = json.loads(msg.content)

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

                except Exception:
                    self.logger.critical(
                        "Error while getting message from Azure queue",
                        '\nTB:', traceback.format_exc()
                    )

            time.sleep(5)
