import json
import time
import uuid
from inspect import getmembers, isfunction
from multiprocessing import Pool, Queue
import os

# noinspection PyPackageRequirements
from azure.storage.queue import QueueService
# noinspection PyPackageRequirements
from azure.storage.table import TableService


class Worker:
    def __init__(self, local_queue, queue_service, queue_name, table_service,
                 tasks):
        self.local_queue = local_queue
        self.qs = queue_service
        self.ts = table_service
        self.azure_queue_name = queue_name
        self.tasks = tasks

    def start_working(self):
        print('STARTING worker PID: {}'.format(os.getpid()))

        while True:
            msg = self.local_queue.get(True)
            exception_message = None
            result = None

            try:
                result = self.run(msg)
            except Exception as e:
                exception_message = e
            finally:
                self.delete_message(msg)
                self.log_to_table(msg, result, exception_message)
                print(exception_message, result)
            time.sleep(5)

    def run(self, msg):
        result = None
        exception = None
        message = msg.content

        task = self.tasks.get(message['task_name'])
        if not task:
            raise Exception(
                '{} is not a registered task.'.format(message['task_name']))

        try:
            if message['args']:
                result = task(*message['args'])
            elif message['kwargs']:
                result = task(**message['kwargs'])
            else:
                result = task()
        except Exception as e:
            exception = str(e)
        finally:
            self.delete_message(msg)
            self.log_to_table(msg, result, exception)

    def delete_message(self, msg):
        pass

    def log_to_table(self, msg, result, exception):
        pass


class Message:
    def __init__(self, task_name, args=None, kwargs=None):
        self.task_name = task_name
        self.args = args
        self.kwargs = kwargs

    @property
    def msg(self):
        return json.dumps({
            "task_name": self.task_name,
            "args": self.args,
            "kwargs": self.kwargs,
            "id": str(uuid.uuid4())
        })


class MainPawWorker:
    def __init__(self, azure_storage_name, azure_storage_private_key,
                 azure_storage_queue_name, tasks_module, workers):
        self.account_name = azure_storage_name
        self.account_key = azure_storage_private_key
        self.queue_name = azure_storage_queue_name
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
            tasks=self._load_tasks()
        )
        self.pool = Pool(self.workers, self.worker_process.start_working, ())

    def _load_tasks(self):
        tasks = dict(
            [o for o in getmembers(self.tasks_module)
             if isfunction(o[1]) and hasattr(o[1], 'paw')]
        )

        for task, func in tasks.items():
            print("Registered: '{}'".format(func.name))
            if func.description:
                print('\t{}'.format(func.description))
        return tasks

    def start_workers(self):
        self.queue_service.create_queue(self.queue_name)
        while True:
            if not self.local_queue.full():
                try:
                    msg = self.queue_service.get_messages(self.queue_name, 1)
                    if msg:
                        self.local_queue.put_nowait(msg[0])
                except Exception as e:
                    # TODO: Due to the chaotic nature of the azure package.
                    # TODO: Replace with proper catching once we figure it out
                    print("Error while getting message from Azure queue")
                    print(e)

            time.sleep(5)


def task(name, description=''):
    def wrapper(func):
        setattr(func, 'name', name)
        setattr(func, 'description', description)
        setattr(func, 'paw', True)
        return func
    return wrapper
