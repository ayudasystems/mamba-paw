import importlib
import time
from inspect import getmembers, isfunction


class Worker:
    def __init__(self, queue, azure_queue_service, azure_queue_name,
                 azure_table_service, tasks):
        self.queue = queue
        self.qs = azure_queue_service
        self.ts = azure_table_service
        self.azure_queue_name = azure_queue_name

        self.tasks = dict(
            [o for o in getmembers(
                importlib.import_module(tasks)
            )
             if isfunction(o[1]) and hasattr(o[1], 'paw')]
        )

    def start_working(self):
        while True:
            msg = self.queue.get(True)
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

    # noinspection PyCallingNonCallable
    def run(self, msg):
        task = self.tasks.get(msg['task_name'])
        if not task:
            raise Exception(
                '{} is not a registered task.'.format(msg['task_name']))

        if msg.get('args'):
            return task(*msg['args'])
        if msg.get('kwargs'):
            return task(**msg['kwargs'])
        return task()

    def delete_message(self, msg):
        pass

    def log_to_table(self, msg, result, exception_message):
        pass
