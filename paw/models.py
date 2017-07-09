import json
import time
import uuid
from inspect import getmembers, isfunction
from multiprocessing import Pool, Queue, Process
import os
import datetime
# noinspection PyPackageRequirements
from azure.storage.queue import QueueService
# noinspection PyPackageRequirements
from azure.storage.table import TableService, Entity
import traceback


SUCCESS = 'SUCCESS'
FAILED = 'FAILED'
STARTED = 'STARTED'
RETRY = 'RETRY'


class Worker(Process):
    def __init__(self, local_queue, queue_service, queue_name,
                 table_service, table_name, tasks):
        super(Worker, self).__init__()

        self.local_queue = local_queue
        self.qs = queue_service
        self.ts = table_service
        self.azure_queue_name = queue_name
        self.azure_table_name = table_name
        self.tasks = tasks

    def run(self):
        print('STARTING worker PID: {}'.format(os.getpid()))
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
                if content['msg]'].dequeue_count > 5:
                    # TODO: delete from table if too many dequeue
                    pass
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
                print(os.getpid(), datetime.datetime.now(), 'Exception',
                      exception, 'RESULT:', result)


class Message:
    def __init__(self, task_name, account_name, account_key, queue_name,
                 args=None, kwargs=None):
        self.queue_service = QueueService(account_name=account_name,
                                          account_key=account_key)
        self.queue_name = queue_name
        self.task_name = task_name
        self.args = args
        self.kwargs = kwargs
        self.job_id = str(uuid.uuid4())

    def queue_message(self):
        content = json.dumps({
            "task_name": self.task_name,
            "args": self.args,
            "kwargs": self.kwargs,
            "job_id": self.job_id
        })

        self.queue_service.put_message(self.queue_name, content)
        return self.job_id


class MainPawWorker:
    def __init__(self, azure_storage_name, azure_storage_private_key,
                 azure_queue_name, azure_table_name, tasks_module, workers):
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
        self.worker_process = Worker(
            local_queue=self.local_queue,
            queue_service=self.queue_service,
            queue_name=self.queue_name,
            table_service=self.table_service,
            table_name=azure_table_name,
            tasks=self._load_tasks()
        )
        self.pool = Pool(self.workers, self.worker_process.run, ())

    def _load_tasks(self):
        tasks = dict(
            [o for o in getmembers(self.tasks_module)
             if isfunction(o[1]) and hasattr(o[1], 'paw')]
        )

        for t, f in tasks.items():
            print("REGISTERED '{}'".format(t))
            if f.description:
                print("\tdescription: '{}'".format(f.description))
        print('\n')
        return tasks

    def start_workers(self):
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
                        content['msg'] = msg
                        self.local_queue.put_nowait(content)

                except Exception:
                    # TODO: Due to the chaotic nature of the azure package.
                    # TODO: Replace with proper catching once we figure it out
                    print("Error while getting message from Azure queue",
                          '\nTB:', traceback.format_exc())

            time.sleep(5)


def log_to_table(table_service, table_name, task_name, status, job_id,
                 result=None, exception=None, create=False):
    table_service.create_table(table_name)

    while table_name not in [t.name for t in table_service.list_tables()]:
        time.sleep(2)

    entity = Entity()
    entity.PartitionKey = task_name
    entity.RowKey = job_id

    if result:
        result = repr(result)

    entity.status = status
    entity.result = result
    entity.exception = exception
    # TODO: Grab existin one, and update, mainting dequeue_time intact.
    if create:
        entity.dequeue_time = datetime.datetime.utcnow()
        table_service.insert_entity(table_name, entity)

    table_service.update_entity(table_name, entity)


def task(description=''):
    def wrapper(func):
        setattr(func, 'description', description)
        setattr(func, 'paw', True)
        return func
    return wrapper
