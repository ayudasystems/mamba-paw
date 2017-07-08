import json
import time
import uuid
from inspect import getmembers, isfunction
from multiprocessing import Pool, Queue, Process, current_process
import os

# noinspection PyPackageRequirements
from azure.storage.queue import QueueService
# noinspection PyPackageRequirements
from azure.storage.table import TableService
import traceback


class Worker(Process):
    def __init__(self, local_queue, queue_service, queue_name,
                 table_service,
                 tasks):
        super(Worker, self).__init__()

        self.local_queue = local_queue
        self.qs = queue_service
        self.ts = table_service
        self.azure_queue_name = queue_name
        self.tasks = tasks
        self.msg = None

    def start_working(self):
        print('STARTING worker PID: {}'.format(os.getpid()))
        while True:
            self.msg = self.local_queue.get(True)
            exception_message = None
            result = None

            try:
                result = self.run()
            except Exception as e:
                exception_message = e
            finally:
                self.delete_message()
                self.log_to_table(result, exception_message)
                print(exception_message, result,
                      'TB\n:', traceback.format_exc())
            time.sleep(5)

    def run(self):
        result = None
        exception = None
        print(current_process().name, self.msg.get('task_name'))

        func = self.tasks.get(self.msg['task_name'])
        if not func:
            raise Exception(
                '{} is not a registered task.'.format(self.msg['task_name']))

        try:
            if self.msg['args']:
                result = func(*self.msg['args'])
            elif self.msg['kwargs']:
                result = func(**self.msg['kwargs'])
            else:
                result = func()
        except Exception as e:
            exception = str(e)
        finally:
            self.delete_message()
            self.log_to_table(result, exception)

    def delete_message(self):
        self.qs.delete_message(self.azure_queue_name, self.msg['azure_id'],
                               self.msg['pop_receipt'])
        print('DELETED!!!!!!!!!!!!!!!!!!!!')


    def log_to_table(self, result, exception):
        pass


class _Worker:
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
                print(exception_message, result,
                      'TB\n:', traceback.format_exc())
            time.sleep(5)

    def run(self, msg):
        result = None
        exception = None
        # message = msg['content

        func = self.tasks.get(msg['task_name'])
        if not func:
            raise Exception(
                '{} is not a registered task.'.format(msg['task_name']))

        try:
            if msg['args']:
                result = func(*msg['args'])
            elif msg['kwargs']:
                result = func(**msg['kwargs'])
            else:
                result = func()
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
    def __init__(self, task_name, account_name, account_key, queue_name,
                 args=None, kwargs=None):
        self.queue_service = QueueService(account_name=account_name,
                                          account_key=account_key)
        self.queue_name = queue_name
        self.task_name = task_name
        self.args = args
        self.kwargs = kwargs

    def queue_message(self):
        content = json.dumps({
            "task_name": self.task_name,
            "args": self.args,
            "kwargs": self.kwargs,
            "id": str(uuid.uuid4())
        })

        msg = self.queue_service.put_message(self.queue_name, content)
        return msg.id


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

        for t, f in tasks.items():
            print("REGISTERED '{}'".format(t))
            if f.description:
                print("\tdescription: '{}'".format(f.description))
        print('\n')
        return tasks

    def start_workers(self):
        self.queue_service.create_queue(self.queue_name)

        while True:
            # for p in active_children():
            #     print(p, dir(p))

            if not self.local_queue.full():
                try:
                    new_msg = self.queue_service.get_messages(
                        queue_name=self.queue_name,
                        num_messages=1,
                        visibility_timeout=60*60
                    )
                    if new_msg:
                        msg = new_msg[0]
                        new_content = {
                            'pop_receipt': msg.pop_receipt,
                            'azure_id': msg.id
                        }
                        content = {**json.loads(msg.content), **new_content}
                        self.local_queue.put_nowait(content)
                        print('\tadded: {}'.format(content))

                except Exception as e:
                    # TODO: Due to the chaotic nature of the azure package.
                    # TODO: Replace with proper catching once we figure it out
                    print("Error while getting message from Azure queue",
                          '\nTB:', traceback.format_exc())
                    print(e)

            time.sleep(5)


def task(description=''):
    def wrapper(func):
        setattr(func, 'description', description)
        setattr(func, 'paw', True)
        return func
    return wrapper
