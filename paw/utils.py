import datetime
import json
import time
import uuid

from azure.common import AzureException, AzureHttpError
from azure.storage.queue import QueueService
from azure.storage.table import Entity


def log_to_table(table_service, table_name, task_name, status, job_id,
                 result=None, exception=None, create=False):
    """Logs to table service the status/result of a task

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
    while True:
        try:
            table_service.create_table(table_name, fail_on_exist=True)
        except AzureHttpError:
            break
        time.sleep(2)

    entity = Entity()
    entity.PartitionKey = task_name
    entity.RowKey = job_id

    if result:
        result = repr(result)

    entity.status = status
    entity.result = result
    entity.exception = exception

    if create:
        entity.dequeue_time = datetime.datetime.utcnow()

    retries = 60

    while retries:
        try:
            table_service.insert_or_merge_entity(table_name, entity)
        except AzureException:
            retries -= 1
            if not retries:
                raise
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
               kwargs=None):
    """Sends messages into the Azure queue.

    :param task_name: Name of the task to queue.
    :param account_name: Name of the Azure account with the queue.
    :param account_key: Private key of the Azure account with the queue
    :param queue_name: Name of the Azure queue
    :param args: List of arguments to pass to the task.
    :param kwargs: Dict of arguments to pass to the task

    :returns: Job ID for this task.
    """
    queue_service = QueueService(account_name=account_name,
                                 account_key=account_key)
    job_id = str(uuid.uuid4())
    content = json.dumps({
        "task_name": task_name,
        "args": args,
        "kwargs": kwargs,
        "job_id": job_id
    })
    queue_service.put_message(queue_name, content)
    return job_id
