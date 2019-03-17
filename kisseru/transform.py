import inspect
import pandas as pd

from utils import get_file_name_without_extention
from utils import get_file_extention
from passes import Pass
from passes import PassResult
from tasks import gen_runner
from tasks import Tasklet
from tasks import Task
from tasks import Edge
from typed import get_type


# [TODO] Make transformations first class and modular. Ideally users should be
# to be able to plug in new file transformations
def csv_to_xls(infile):
    pass


def xls_to_csv(infile):
    file_name = get_file_name_without_extention(infile)
    new_file_name = file_name + '.csv'
    df = pd.read_excel(infile)
    df.to_csv(
        new_file_name,
        index=False)  # index=False prevents pandas to write row index
    return new_file_name


# @args3.id annotation says 'get the actual return type of the function by
# accessing the id field of the third argument runtime value'. In this case
# 'outtype.id'.
#
# Who said we don't have dependent types in Python!! (o_o) :)
def transform(infile, intype, outtype) -> '@args3.id':

    trans_map = {'csv->xls': csv_to_xls, 'xls->csv': xls_to_csv}
    trans_key = '{}->{}'.format(intype.id, outtype.id)
    transform_fn = trans_map.get(trans_key, None)
    return transform_fn(infile)


class Transform(Pass):
    def __init__(self, name):
        Pass.__init__(self, name)
        self.description = "Inserting data transformations"

    def run(self, graph, ctx):
        Pass.run(self, graph, ctx)
        new_tasks = []
        new_sources = []
        deleted_sources = []
        # Run edge transformations
        for tid, task in graph.tasks.items():
            for index, edge in enumerate(task.edges):
                # If we find that we need to do a data type transformation we
                # need to splice in the transformation in between the original
                # tasks
                if edge.needs_transform:
                    intype = edge.source.type
                    outtype = edge.dest.type

                    # [FIXME] Code debt - Currently we have two overloaded ways
                    # of indexing in to the task.outputs dictionary. One with
                    # indices and one with names in the case of named outputs.
                    # But there is currently no way of distinguishing between
                    # these two at the moment. I currently just assume it is
                    # indexed addressing case here since we don't currently
                    # support named outputs. This needs to change once we add
                    # support for named outputs.
                    tasklet = Tasklet(edge.source.task_ref,
                                      str(edge.source.index))
                    args = [tasklet, intype, outtype]

                    sig = inspect.signature(transform)
                    new_task = Task(
                        gen_runner(transform, sig), transform, sig, args, {})
                    new_task.is_transform = True
                    # The input corresponds to the 'infile' parameter of the
                    # transform function
                    inport = new_task.inputs['infile']
                    # We know this generated task only has one output
                    outport = new_task.outputs['0']

                    old_dest_port = edge.dest

                    # Remove the old edge since we should have generated a new
                    # edge from the original source to this newly generated task
                    # during the call to Task constructor
                    del task.edges[index]

                    # Make the original destination port of the edge to be the
                    # the destination port of the outward edge of the new task
                    new_task.edges.append(Edge(outport, old_dest_port))
                    # Collect newly generated tasks
                    new_tasks.append(new_task)

        # Run source input transformations
        for tid, source in graph.sources.items():
            for name, inport in source.inputs.items():
                # Get the actual argument value passed to this source
                arg = source._args[name]

                # Check if it looks like a file
                ext = get_file_extention(arg)
                if ext:
                    if ext == 'csv':
                        intype = inport.type.id
                        if intype != ext:
                            outtype = get_type(ext)
                            args = [arg, intype, outtype]

                            sig = inspect.signature(transform)
                            # Generate a new task for transforming the input to
                            # type the original source was expecting
                            task = Task(
                                gen_runner(transform, sig), transform, sig,
                                args, {})
                            task.is_transform = True

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
