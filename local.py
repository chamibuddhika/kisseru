import logging
import os
try:
    import cPickle as pickle
except:
    import pickle

from tasks import Port
from tasks import FusedTask
from backend import Backend
from process import ProcessFactory
from logger import TaskLogger
from logger import MonoChromeLogger

log = logging.getLogger(__name__)


class LocalPort(Port):
    def __init__(self, typ, name, index, task):
        Port.__init__(self, typ, name, index, task)
        self.is_one_sided_receive = True

    def send(self, value, to_port):
        to_port.receive(value, self)

    def receive(self, value=None, from_port=None):
        # This receive was invoked as a callback from the task. Just return
        if value == None:
            return

        log.debug("Received value {} from task {}".format(
            value, from_port.task_ref.name))

        self.task_ref._args[self.name] = value
        # Notify the task that it got a new input
        self.notify_task()
        # Invoke the receive on task since we are doing one sided push data-flow
        # with local ports
        self.task_ref.receive()


class LocalThreadedPort(Port):
    def __init__(self, typ, name, index, task):
        Port.__init__(self, typ, name, index, task)
        self.is_one_sided_receive = True

    def send(self, value, to_port):
        filename = "{}_{}".format(to_port.task_ref.id, to_port.name)
        fp = open(filename, 'wb')
        pickle.dump(value, fp)
        fp.close()

        to_port.receive(filename, self)

    def receive(self, value=None, from_port=None):
        if value == None:
            # This receive was invoked as a callback from the task.
            filename = "{}_{}".format(self.task_ref.id, self.name)
            if os.path.isfile(filename):
                fp = open(filename, 'rb')
                value = pickle.load(fp)
                fp.close()

                self.task_ref._args[self.name] = value
            return

        # Notify the task that it got a new input
        self.notify_task()

        # Invoke the receive on task since we are doing one sided push data-flow
        # with local ports
        self.task_ref.receive()


@Backend.register_backend
class LocalNonThreadedBackend(Backend):
    name = "LOCAL_NON_THREADED"

    def __init__(self, backend_config):
        Backend.__init__(self, backend_config)
        self.logger = TaskLogger('app.log')

    def get_port(self, typ, name, index, task):
        return LocalPort(typ, name, index, task)

    def package(self):
        pass

    def deploy(self):
        pass

    def run_task(self, task):
        task.run()

    def run_flow(self, graph):
        for tid, source in graph.sources.items():
            source.run()


@Backend.register_backend
class LocalThreadedBackend(Backend):
    name = "LOCAL"

    def __init__(self, backend_config):
        Backend.__init__(self, backend_config)
        self.logger = TaskLogger('app.log')

    def get_port(self, typ, name, index, task):
        return LocalThreadedPort(typ, name, index, task)

    def package(self):
        pass

    def deploy(self):
        pass

    def run_task(self, task):
        def threaded_task(task):
            if isinstance(task, FusedTask):
                print("At fused task {}".format(task.name))
            else:
                print("At unfused task {}".format(task.name))
            task.run()

            with task.graph.completed_tasks.get_lock():
                task.graph.completed_tasks.value += 1

            if task.graph.completed_tasks.value == task.graph.get_num_tasks():
                with task.graph.done:
                    print("Notifying the main thread..")
                    task.graph.done.notify()

        if not task.is_fusee:
            print("Running fused {}".format(task.name))
            ProcessFactory.create_process(threaded_task, (task, ))
        else:
            print("Running fusee {}".format(task.name))
            task.run()

    def run_flow(self, graph):
        print("Barrier count {}".format(graph.get_num_tasks() + 1))

        for tid, source in graph.sources.items():
            self.run_task(source)

        while graph.completed_tasks.value != graph.get_num_tasks():
            print("Waiting at main thread..")
            with graph.done:
                graph.done.wait()
