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
from logger import ThreadLocalLogger
from logger import MonoChromeLogger
from logger import LogColor

log = logging.getLogger(__name__)

class SlurmPort(Port):
    def __init__(self, typ, name, index, task):
        Port.__init__(self, typ, name, index, task)
        self.is_one_sided_receive = False

    def send(self, value, to_port):
        # Write value to file
        filename = "{}_{}".format(to_port.task_ref.id, to_port.name)
        fp = open(filename, 'wb')
        pickle.dump(value, fp)
        fp.close()


    def receive(self, value=None, from_port=None):
        # Poll value from file
        filename = "{}_{}".format(self.task_ref.id, self.name)
        while not os.path.exists(file_path):
            time.sleep(1) 

        if os.path.isfile(filename):
            fp = open(filename, 'rb')
            value = pickle.load(fp)
            fp.close()
        else:
            raise ValueError("%s isn't a file\n" % filename)

        log.debug("Received value {} from task {}".format(
            value, from_port.task_ref.name))

        self.task_ref._args[self.name] = value
        # Notify the task that it got a new input
        self.notify_task()

@Backend.register_backend
class SlurmBackend(Backend):
    name = "SLURM"

    def __init__(self, backend_config):
        Backend.__init__(self, backend_config)
        self.logger = None

    def get_port(self, typ, name, index, task):
        return SlurmPort(typ, name, index, task)

    def package(self):
        pass

    def deploy(self):
        pass

    def run_task(self, task):
        task.run()

    def run_flow(self, graph):
        pass

    def cleanup(self, graph):
        # Remove temporary files used for transferring data between python
        # threads (processes)
        for tid, task in graph.tasks.items():
            if isinstance(task, FusedTask) or (not task.is_fusee):
                for param, inport in task.inputs.items():
                    filename = "{}_{}".format(task.id, param)
                    if os.path.isfile(filename):
                        os.remove(filename)
