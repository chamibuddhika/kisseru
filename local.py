import logging

from tasks import Port
from backend import Backend
from process import ProcessFactory

log = logging.getLogger(__name__)


class LocalPort(Port):
    def __init__(self, typ, name, index, task):
        Port.__init__(self, typ, name, index, task)
        self.is_one_sided = True

    def send(self, value, to_port):
        to_port.receive(value, self)

    def receive(self, value=None, from_port=None):
        # This receive was invoked as a callback from the task. Just return
        if value == None:
            return self.is_one_sided

        log.debug("Received value {} from task {}".format(
            value, from_port.task_ref.name))

        self.task_ref._args[self.name] = value
        # Notify the task that it got a new input
        self.notify_task()
        # Invoke the receive on task since we are doing one sided push data-flow
        # with local ports
        self.task_ref.receive()
        return self.is_one_sided


class LocalThreadedPort(LocalPort):
    def __init__(self, typ, name, index, task):
        Port.__init__(self, typ, name, index, task)
        self.is_one_sided = True

    def send(self, value, to_port):
        def threaded_receive(from_port, value, to_port):
            to_port.receive(value, to_port)

        ProcessFactory.create_process(threaded_receive, (self, value, to_port))


@Backend.register_backend
class LocalThreadedBackend(Backend):
    name = "LOCAL"

    def __init__(self, backend_config):
        Backend.__init__(self, backend_config)

    def get_port(self, typ, name, index, task):
        return LocalThreadedPort(typ, name, index, task)

    def package(self):
        pass

    def deploy(self):
        pass

    def run(self):
        pass


@Backend.register_backend
class LocalNonThreadedBackend(Backend):
    name = "LOCAL_NON_THREADED"

    def __init__(self, backend_config):
        Backend.__init__(self, backend_config)

    def get_port(self, typ, name, index, task):
        return LocalPort(typ, name, index, task)

    def package(self):
        pass

    def deploy(self):
        pass

    def run(self, graph):
        for tid, source in graph.sources.items():
            source.run()
