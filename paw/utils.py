import datetime
import json
import logging
import socket
import string
import time
import traceback
import uuid

# noinspection PyPackageRequirements
from azure.common import AzureException, AzureHttpError
# noinspection PyPackageRequirements
from azure.storage.queue import QueueService
# noinspection PyPackageRequirements
from azure.storage.table import EdmType, Entity, EntityProperty

from .exceptions import PawError

LOGGER = logging.getLogger(__name__)
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


def create_table_if_missing(table_service, table_name):
    while True:
        try:
            table_service.create_table(table_name, fail_on_exist=True)
        except AzureHttpError:
            break
        LOGGER.info("Waiting for table to be ready")
        time.sleep(2)


def log_to_table(table_service, table_name, task_name, status, job_id,
                 result=None, exception=None, create=False):
    """
    Logs to table service the status/result of a task

    :param table_service: azure.storage.table.TableService
    :param table_name: Name of the Azure table to use.
    :param task_name: Name of the task to log result/status for.
    :param status: Status of the task. Ex: STARTED, FAILED etc...
    :param job_id: UUID of the task.
    :param result: Result if any.
    :param exception: Exception, if any.
    :param create: Bool. Adds the created date. Used to keep it even after
                   updating an existing row.
    """
    create_table_if_missing(table_service, table_name)
    entity = Entity()
    entity.PartitionKey = task_name
    entity.RowKey = job_id
    entity.status = status

    if result:
        # Results are added in this manner because Azure SDK's serializer fails
        # when results are repr(list).
        # noinspection PyTypeChecker
        entity.result = EntityProperty(type=EdmType.STRING, value=repr(result))

    entity.exception = exception

    if create:
        entity.dequeue_time = datetime.datetime.utcnow()

    retries = 60

    while retries:
        try:
            table_service.insert_or_merge_entity(table_name, entity)
            break
        except AzureException as e:
            retries -= 1
            if not retries:
                raise PawError(e)
            LOGGER.error("Error from Azure table service: "
                         "{}".format(traceback.format_exc()))
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
               kwargs=None, retries=30):
    """
    Sends messages into the Azure queue.

    :param task_name: Name of the task to queue.
    :param account_name: Name of the Azure account with the queue.
    :param account_key: Private key of the Azure account with the queue
    :param queue_name: Name of the Azure queue
    :param args: List of arguments to pass to the task.
    :param kwargs: Dict of arguments to pass to the task
    :param retries: Int of how many times to retry. 1 second wait per try

    :returns: Job ID for this task.
    """
    if args and kwargs:
        raise PawError("You can't pass both positional and keyword arguments")

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
        "job_id": job_id
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
