import logging
import os
import platform
try:
    import cPickle as pickle
except:
    import pickle

from tasks import Port
from tasks import FusedTask
from backend import Backend
from process import ProcessFactory
from logger import TaskLogger
from logger import ThreadLocalLogger
from logger import MonoChromeLogger
from logger import LogColor

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
        self.logger = None

    def get_port(self, typ, name, index, task):
        return LocalPort(typ, name, index, task)

    def package(self):
        pass

    def deploy(self):
        pass

    def run_task(self, task):
        task.run()

    def run_flow(self, graph):
        self.logger = TaskLogger("{}.log".format(graph.name))
        for tid, source in graph.sources.items():
            source.run()
        self.logger.flush()

    def cleanup(self, graph):
        pass


@Backend.register_backend
class LocalThreadedBackend(Backend):
    name = "LOCAL"

    def __init__(self, backend_config):
        Backend.__init__(self, backend_config)
        self.logger = None

        # [NOTE] We have to disable proxies to get some libraries working
        # (e.g: urllib, scikitlearn) with multiprocessing
        # due to https://bugs.python.org/issue30385 in OS X
        if platform.system().startswith("Darwin"):
            os.environ["no_proxy"] = "*"

    def get_port(self, typ, name, index, task):
        return LocalThreadedPort(typ, name, index, task)

    def package(self):
        pass

    def deploy(self):
        pass

    def run_task(self, task):
        def threaded_task(task):
            task.run()
            # Flush the thread local log in case task.run() did not result in
            # spawning a new thread (process).
            self.logger.flush()

        if not task.is_fusee:
            # We are about to spawn a new task. Flush the logs that we
            # accumulated for this thread before spawning the new thread
            if self.logger:
                self.logger.flush()
            self.logger = ThreadLocalLogger(task.name + '.log')

            # [NOTE] We have to disable proxies to get some libraries working
            # (e.g: urllib, scikitlearn) with multiprocessing
            # due to https://bugs.python.org/issue30385 in OS X
            if platform.system().startswith("Darwin"):
                os.environ["no_proxy"] = "*"

            ProcessFactory.create_process(threaded_task, (task, ))
        else:
            task.run()

    def run_flow(self, graph):
        for tid, source in graph.sources.items():
            self.run_task(source)

        while graph.completed_tasks.value != graph.get_num_tasks():
            with graph.done:
                graph.done.wait()

    def cleanup(self, graph):
        # Remove temporary files used for transferring data between python
        # threads (processes)
        for tid, task in graph.tasks.items():
            if isinstance(task, FusedTask) or (not task.is_fusee):
                for param, inport in task.inputs.items():
                    filename = "{}_{}".format(task.id, param)
                    if os.path.isfile(filename):
                        os.remove(filename)
