import datetime
import json
import logging
import socket
import string
import sys
import time
import traceback
import uuid
from json import JSONDecodeError

# noinspection PyPackageRequirements
from azure.common import AzureException, AzureHttpError
# noinspection PyPackageRequirements
from azure.storage.queue import QueueService
# noinspection PyPackageRequirements
from azure.storage.table import EdmType, Entity, EntityProperty

from .exceptions import PawError

log_format = ('{levelname}: {asctime} PID:{process} {module}.{funcName} '
              '@{lineno}: {message}')

LOGGER = logging.getLogger('paw')

LOGGER.setLevel(logging.INFO)
formatter = logging.Formatter(log_format, style='{')
console = logging.StreamHandler(sys.stdout)
console.setFormatter(formatter)
LOGGER.addHandler(console)
LOGGER.propagate = False


PAW_LOGO = """
=======================
= Python Azure Worker =
=======================
   _  _ 
 _(_)(_)_
(_).--.(_)
  /    \\
  \    /  _  _
   '--' _(_)(_)_
       (_).--.(_)
         /    \\
   _  _  \    /
 _(_)(_)_ '--'
(_).--.(_)
  /    \\
  \    /  _  _
   '--' _(_)(_)_
       (_).--.(_)
         /    \\
         \    /
          '--'
"""

DEFAULT_TABLE_COLUMNS = (
    'PartitionKey',
    'RowKey',
    'Timestamp',
    'dequeue_time',
    'result',
    'status',
    'exception',
)


def create_table_if_missing(table_service, table_name):
    while True:
        try:
            table_service.create_table(table_name, fail_on_exist=True)
        except AzureHttpError:
            break
        LOGGER.info("Waiting for table to be ready")
        time.sleep(2)


def log_to_table(table_service, table_name, message, status, result=None,
                 exception=None, create=False):
    """
    Logs to table service the status/result of a task

    :param table_service: azure.storage.table.TableService
    :param table_name: Name of the Azure table to use.
    :param message: Dict from Azure queue or azure.storage.table.Entity
    :param status: Status of the task. Ex: STARTED, FAILED etc...
    :param result: Result if any.
    :param exception: Exception, if any.
    :param create: Bool. Adds the created date. Used to keep it even after
                   updating an existing row.
    """
    create_table_if_missing(table_service, table_name)
    entity = Entity()
    # To support both an Entity or message from queue
    partition_key = message.get('task_name') or message.get('PartitionKey')
    row_key = message.get('job_id') or message.get('RowKey')

    if not partition_key or not row_key:
        raise PawError('message did not contained all required information. '
                       '"task_name" {}, "job_id" {}'.format(partition_key,
                                                            row_key))

    if message.get('additional_log'):
        entity.update(message['additional_log'])

    entity.PartitionKey = partition_key
    entity.RowKey = row_key
    entity.status = status

    #
    # Added in this manner because Azure SDK's serializer fails
    # when results are repr(list).
    if result:
        # noinspection PyTypeChecker
        entity.result = EntityProperty(type=EdmType.STRING, value=repr(result))
    if exception:
        # noinspection PyTypeChecker
        entity.exception = EntityProperty(type=EdmType.STRING,
                                          value=repr(exception))

    if create:
        entity.dequeue_time = datetime.datetime.utcnow()

    retries = 60

    while retries:
        try:
            table_service.insert_or_merge_entity(table_name, entity)
            break
        except AzureException as e:
            LOGGER.warning("Error from Azure table service: "
                           "{}".format(traceback.format_exc()))
            retries -= 1
            if not retries:
                LOGGER.error("Error from Azure table service: "
                             "{}".format(traceback.format_exc()))
                raise PawError(e)

            time.sleep(2)


def task(description=''):
    """
    Decorator used to identify tasks to load from a module. A description
    can optionally be given.
    """
    def wrapper(func):
        setattr(func, 'description', description)
        setattr(func, 'paw', True)
        return func
    return wrapper


def queue_task(task_name, account_name, account_key, queue_name, args=None,
               kwargs=None, retries=30, additional_log=None):
    """
    Sends messages into the Azure queue.

    :param task_name: Name of the task to queue.
    :param account_name: Name of the Azure account with the queue.
    :param account_key: Private key of the Azure account with the queue
    :param queue_name: Name of the Azure queue
    :param args: List of arguments to pass to the task.
    :param kwargs: Dict of arguments to pass to the task
    :param retries: Int of how many times to retry. 1 second wait per try
    :param additional_log: Dictionary with additional values to have logged
                           in table. Key must not override:
                           PartitionKey, RowKey, Timestamp, dequeue_time,
                           result, exception and status.
                           Also, it must be a dict that can be serialized
                           to json, or an already serialized json string.

    :returns: Job ID for this task.
    """
    if args and kwargs:
        raise PawError("You can't pass both positional and keyword arguments")

    # Testing additional_log if present. Converting to dict if received json
    if additional_log:
        try:
            if isinstance(additional_log, dict):
                json.dumps(additional_log)
            else:
                additional_log = json.loads(additional_log)
        except (TypeError, JSONDecodeError) as e:
            raise PawError(e)

        if any([True for k in DEFAULT_TABLE_COLUMNS if
                k in additional_log.keys()]):
            raise PawError("Overlapping keys with default values. Reserved "
                           "key names are {}".format(DEFAULT_TABLE_COLUMNS))

    queue_service = QueueService(account_name=account_name,
                                 account_key=account_key)

    while retries:
        try:
            queue_service.create_queue(queue_name, fail_on_exist=True)
        except AzureException:
            break
        retries -= 1
        if not retries:
            raise PawError('Too many retries creating the queue.')
        time.sleep(1)

    job_id = str(uuid.uuid4())
    content = json.dumps({
        "task_name": task_name,
        "args": args,
        "kwargs": kwargs,
        "job_id": job_id,
        "additional_log": additional_log
    })
    queue_service.put_message(queue_name, content)

    return job_id


def generate_name_from_hostname():
    """
    Generates a name based on hostname. Validates that it is valid to use as
    queue or table name on Azure Storage.

    It is assumed that your run all your instances of paw.MainWorker under
    hosts that are unique.

    :return: Valid name for Azure Table and Queue based on hostname
    """
    LOGGER.debug('It is assumed that your run all your instances of '
                 'paw.MainWorker under hosts that are unique.')
    hostname = socket.gethostname()
    cleaned_hostname = ''.join(
        [l for l in hostname if l not in string.punctuation]).lower()[:63]

    if len(cleaned_hostname) < 3:
        raise PawError('Cannot generate name from hostname. '
                       '"{}" is not valid'.format(cleaned_hostname))

    if cleaned_hostname.isnumeric():
        raise PawError('Cannot use all number name. '
                       '"{}"'.format(cleaned_hostname))

    return cleaned_hostname
