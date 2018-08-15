import os
import inspect
import urllib.request
import shutil
import time
import socket

from colors import Colors
from tasks import gen_runner
from tasks import Task
from tasks import Edge
from passes import Pass
from passes import PassResult
from utils import get_file_extention
from utils import get_file_name
from utils import get_path_to_file


def staging(infile):

    filename = get_file_name(infile)
    path = get_path_to_file(infile)
    response = None

    try:
        response = urllib.request.urlopen(infile)
    except e:
        print(e.reason)

    content = response.read()
    with open("{}".format(filename), "wb") as data:
        data.write(content)
    return filename


class Stage(Pass):
    def __init__(self, name):
        Pass.__init__(self, name)
        self.description = "Inserting staging operations"

    def run(self, graph, ctx):
        Pass.run(self, graph, ctx)
        new_tasks = []
        new_sources = []
        deleted_sources = []

        # Run edge transformations
        #
        # This depends on the run time placement of the task since staging may
        # or may not be necessary depending on whether the next task is placed
        # at the same node or not.

        # Run source input staging
        for tid, source in graph.sources.items():
            for name, inport in source.inputs.items():
                # Get the actual argument value passed to this source
                arg = source._args[name]

                # Check if it looks like a URL (currently we only support FTP)
                if arg.startswith("ftp:"):
                    ext = get_file_extention(get_file_name(arg))
                    intype = inport.type.id
                    '''
                    if ext:
                        if intype != ext:
                            print(
                                Colors.WARNING +
                                """[Compiler] {} input file extention does not seem to match the declared argument type {} at {}"""
                                .format(ext, intype, source.name) +
                                Colors.ENDC)
                    '''
                    args = [arg]

                    sig = inspect.signature(staging)
                    # Generate a new task for staging the input
                    task = Task(
                        gen_runner(staging, sig), staging, sig, args, {})
                    task.is_staging = True

                    # We know this generated task only has one output
                    outport = task.outputs['0']

                    # Make the configuration of the original task's input to be
                    # non immediate since now it accepts the output from newly
                    # generated staging task at runtime
                    inport.flip_is_immediate()

                    # Connect the out port of the new task to the
                    # in port of the old source
                    task.edges.append(Edge(outport, inport))

                    # Collect the new task as a source
                    new_sources.append(task)
                    # Collect sources which are made not sources anymore
                    deleted_sources.append(source)
                    # Collect newly generated tasks
                    new_tasks.append(task)

        # Add the newly generated tasks to the graph
        for task in new_tasks:
            graph.add_task(task)

        # Mark removed sources as not sources
        for source in deleted_sources:
            graph.unset_source(source)

        # Add newly generated sources
        for source in new_sources:
            graph.set_source(source)

        return PassResult.CONTINUE

    def post_run(self, graph, ctx):
        pass
