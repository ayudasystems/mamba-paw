mamba-paw (Python Azure Worker)
===============================

Base project for the **paw** package. Very much a work in progress.


To create tasks:
```
# tasks.py

from paw import task

@task(description='Stupid simple example')
def task_one(print_me):
    print(print_me)
    return True

```


To start a worker:
```
# start_workers.py

from paw import MainPawWorker
import tasks  # importing tasks from tasks.py in local project

workers = MainPawWorker(
    azure_storage_name='storage account name',
    azure_storage_private_key='storage account private key',
    azure_queue_name='name of the queue',
    azure_table_name='name of the table',
    tasks_module=tasks,
    workers=4
)

workers.start_workers()
```

To queue a task:
```
# queue_tasks.py

from paw import queue_task

queue_task(
    task_name='task_one',
    account_name='storage account name',
    account_key='storage account private key',
    queue_name='name of the queue',
    args=['List', 'of', 'arguments'],
    kwargs={'Key_word': 'arguments'}
)
```
