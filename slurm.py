import jsonpickle
import logging
import os
try:
    import cPickle as pickle
except:
    import pickle
import shutil

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
        self.slurm_driver = """

import jsonpickle
import sys

from kisseru import *

if __name__ == "__main__":
    if not len(sys.argv):
        raise ValueError("Task ID unavailable")

    tid = sys.argv[0]
    graph_str = None
    with open("graph", "r") as fp:
       graph_str = fp.read()

    if not graph_str:
        raise ValueError("Graph not present")

    graph = jsonpickle.decode(graph_str)

    task = graph.get_task(tid)
    task.receive()

"""
    def _populate_job_map(self, source, visited, job_map, counter):
        if source in visited:
            return (counter, job_map)

        queue = [source]
        while queue:
            node = queue.pop(0)
            print("job_map for node : %s" % node.id)
            if node in visited:
                continue
            visited.add(node)
            job_id = "jid" + str(counter)
            job_name = "--job-name=" + node.name + "_" + str(counter)
            job_script = "job_" + node.name + "_" + str(node.id)
            job_map[node.id] = job_id + "/" + job_name + "/" + job_script

            children = node.get_children()
            for child in children:
                queue.append(child)
            counter += 1

        return (counter, job_map)


    def _get_slurm_jobs(self, source, visited, job_map):
        if source in visited:
            return ""

        jobs = ""
        queue = [source]
        while queue:
            node = queue.pop(0)
            print("visiting node %s" % node.id)
            if node in visited:
                continue

            visited.add(node)

            dependencies = ""
            parents = node.get_parents()
            if parents:
                dependencies = "--dependency=afterany"
                for parent in parents:
                    parent_job_id = job_map[parent.id]
                    dependencies = ":$" + parent_job_id

            tokens = job_map[node.id].split("/")
            job_id = tokens[0]
            job_name = tokens[1]
            job_script = tokens[2]
            jobs += job_id + "=" + "$(sbatch " + dependencies + " " + job_name +\
                    " " + job_script + ")\n\n"

            children = node.get_children()
            for child in children:
                queue.append(child)
        return jobs 


    def get_port(self, typ, name, index, task):
        return SlurmPort(typ, name, index, task)

    def package(self, graph, app_dir):
        # create temporary directory
        temp_dir = "/tmp/.kisseru_" + graph.name 
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        os.makedirs(temp_dir)

        # copy over the files in application directory
        files = os.listdir(app_dir)
        for f in files:
            if os.path.isfile(os.path.join(app_dir, f)):
                shutil.copy(os.path.join(app_dir, f), temp_dir)

        # serialize the graph as a file in the temporary directory
        serialized = jsonpickle.encode(graph)
        with open(os.path.join(temp_dir, "graph"), "w") as fp:
            fp.write(serialized)

        # serialize the slurm driver as file in the temporary directory
        with open(os.path.join(temp_dir, "slurm_driver.py"), "w") as fp:
            fp.write(self.slurm_driver)

        # create slurm script which submits jobs according to graph dependencies
        visited = set()
        counter = 0
        job_graph = ""
        job_map = {}
        for tid, source in graph.sources.items():
            (counter, job_map)  = \
                    self._populate_job_map(source, visited, job_map, counter)

        visited.clear()
        for tid, source in graph.sources.items():
            (counter, slurm_jobs) = self._get_slurm_jobs(source, visited, job_map)
            job_graph += slurm_jobs 

        batch_script = ""
        header = "#! /bin/bash\n\n"

        batch_script += header
        batch_script += job_graph

        # serialize the batch srcipt as file in the temporary directory
        with open(os.path.join(temp_dir, "run.sh"), "w") as fp:
            fp.write(batch_script)


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
