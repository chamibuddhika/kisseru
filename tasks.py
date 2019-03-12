import inspect
import uuid
import threading
import abc
import traceback
import logging

from multiprocessing import Value
from multiprocessing import Condition
from collections import defaultdict

from logger import LogColor
from typed import get_type
from passes import Pass
from passes import PassResult
from process import ProcessFactory
from backend import Backend
from backend import BackendConfig
from backend import BackendType
from colors import Colors
from utils import gen_tuple
from handler import HandlerContext
from handler import HandlerRegistry

log = logging.getLogger(__name__)


class Port(metaclass=abc.ABCMeta):
    """ A communication port.

    This class is responsible for getting data in-to and out-of a task. 
    Each port be an input port or and output port. Each task argument gets an 
    in-port. If the task output is a tuple each tuple element gets an out-port.
    Otherwise the task has a single out-port.

    Attributes:
        type: Type of the value communicated via the port
        name: Name of the port (parameter name for in-ports, positional index 
               for out-ports)
        index: Positional index of the port (argument positional index for 
               in-ports, tuple positional index for out-ports)
        task_ref: Task which this port is bound to
        inport_edge: If the port is an in-port and associated with an edge 
                      the reference to the associated edge
        is_inport: True if this is an in-port. False if this an out-port
        is_one_sided: True if this port is an in-port which  models a one sided
                       receive
        is_immediate: True if this port is an in-port which has an immediately
                       available value. (i.e.: Not an output from another task)
    """

    def __init__(self, typ, name, index, task):
        self.type = typ
        self.name = name
        self.index = index
        self.task_ref = task
        self.inport_edge = None
        self.is_inport = False
        self.is_one_sided = False
        self.is_immediate = True

    @abc.abstractmethod
    def send(self, value, to_port):
        """ Sends a value to target port

        Used at out-ports to push a task result value to another task in-port
        down stream.

        Args:
            value: Value to be sent
            to_port: Downstream task in-port
        """

        pass

    @abc.abstractmethod
    def receive(self, value=None, from_port=None):
        """ Receive a value from an upstream task out-port 

        Used at in-ports to accept a task argument from an upstream task 
        out-port. Value and from_port are optional. If optional then it is 
        assumed that input is received via other mechanism other than a direct
        function call (i.e: from filesystem or from network)

        For a local backend receive is called twice, once with the value and
        another time without the value. Second invocation happens from 
        task.receive() which calls receive() on all in-ports. This design 
        maintains symmetry at task.receive() for all types of backends - 
        backends featuring onesided in-ports or otherwise. 

        Args:
            value: Value to be received
            from_port: Upstream task out-port
        """

        pass

    def notify_task(self):
        with self.task_ref._latch.get_lock():
            self.task_ref._latch.value -= 1
        with self.task_ref.triggered:
            self.task_ref.triggered.notify()

    def flip_is_immediate(self):
        """ Flips the is_immediate state of this in-port

        This method is used in graph optimization passes where immediate inputs
        may be made non-immediate (e.g: for staging etc.)
        """

        if self.is_immediate:
            self.is_immediate = False
            # Unsynchronized access since we know that this method will be
            # called within a single threaded environment at task graph
            # generation
            self.task_ref._latch.value += 1
        else:
            self.is_immediate = True
            # Unsynchronized access since we know that this method will be
            # called within a single threaded environment at task graph
            # generation
            self.task_ref._latch.value -= 1

    def dump(self):
        print("Port : {} {} {} {}".format(self.type, self.name, self.index,
                                          self.task_ref.name))


class Sink(Port):
    """ An out-port which terminates data flow """

    def __init__(self, port):
        Port.__init__(self, port.type, port.name, port.index, port.task_ref)

    def send(self, value, to_port):
        pass

    def receive(self, value, from_port):
        """ Receives the result and logs it """
        logger = Backend.get_current_backend().logger
        log_str = logger.fmt(
            "[KISSERU] Pipeline output : {}".format(str(value)), LogColor.BLUE)
        logger.log(log_str)

    def dump(self):
        Port.dump(self)


class Edge(object):
    """ An edge connects an up-stream task out-port with a down-stream task 
    in-port 
    
    Attributes:
        source: Upstream task out-port 
        dest: Downstream task in-port
        needs_transform: This edge requires a data transformation due to a
            type mismatch between the source and the dest

    """

    def __init__(self, source, dest):
        self.source = source
        self.dest = dest
        self.needs_transform = False
        self.dest.inport_edge = self

    def send(self, value):
        """ Transfers a value from the source to the dest port

        Args:
            value: The value to be transferred
        """
        log.debug("Sending value {} to task {}".format(
            value, self.dest.task_ref.name))
        self.source.send(value, self.dest)

    def dump(self):
        self.source.dump()
        self.dest.dump()


class Tasklet(object):
    """ Tasklet holds info about a single output from a multi-output task 
    (i.e: a task which returns tuple)

    Tasklet is a temporary data structure which gets generated during task 
    graph generation. Tasklet holds upstream task output positional information
    so that the downstream task can wire its in-port to the correct out-port of
    the upstream task

    Attributes:
        parent: Task associated with the output
        out_slot_in_parent: Positional index in the 'parent' task outputs
    """

    def __init__(self, parent, index):
        self.parent = parent
        self.out_slot_in_parent = index


class Task(object):
    """ Task is an unit of execution contained within a workflow.

    A task accepts zero or more inputs and outputs zero or more results. 
    Multiple results are returned in the form of a python tuple.

    Attributes:
        name: Task name. Defaults to the function name which corresponds to the
            task
        id: Task UUID
        graph: Task graph which the task is bound to
        _runner: Executable function associated with the task. This gets 
            executed at runtime. May include additional code than user provided
            task logic (i.e: pre and post task handlers) 
        _fn: User given function for the task (this is a python code object)
        _sig: Original task (function) signature
        _args: Task (function) arguments

        _latch: Task trigger latch. Gets triggers once all non immediate inputs
            have been received 
        triggered: Task input monitor. Gets notified and task woken up once all
            non immediate inputs have been received. Used in conjunction with 
            _latch

        inputs: in-ports of the task. A dictionary with input argument name as
            key and an in-port object as value
        outputs: out-ports of the task. A dictionary with output name as key
            and an out-port object as value
        edges: Output edges of the task

        is_fusee: True if this task is contained within a FusedTask
        is_source: True if this task is a source of the associated task graph
        is_sink: True if this task is a sink of the associated task graph
        is_staging: True if this task is a staging task generated by the task 
            graph compiler
        is_transform: True if this task is a data transformation task generated
            by the task graph compiler
    """

    def __init__(self, runner, fn, sig, args, kwargs):
        self.name = fn.__name__
        self.id = None
        self.graph = None
        self._runner = runner
        self._fn = fn
        self._sig = sig
        self._args = {}

        # Runtime control
        self._latch = Value('i', 0)
        self.triggered = Condition()

        # I/O
        self.inputs = {}
        self.outputs = {}
        self.edges = []

        # Flags
        self.is_fusee = False
        self.is_fused  = False
        self.is_source = False
        self.is_sink = False
        self.is_staging = False
        self.is_transform = False

        self._set_inputs(fn, args, kwargs)
        self._set_outputs(fn, args)

    def __repr__(self):
        return self.name

    def __str__(self):
        return self.name

    def _set_inputs(self, fn, args, kwargs):
        sig = self._sig
        params = sig.parameters
        # print(params)

        ba = sig.bind(*args, **kwargs)
        ba.apply_defaults()
        arguments = ba.arguments
        # print(arguments)

        if len(params) != len(arguments):
            raise Exception(
                "{} accepts {} arguments. But {} were given".format(
                    self.name, len(params), len(arguments)))

        for pname, param in params.items():
            value = arguments[pname]
            py_type = param.annotation

            param_type = None
            if py_type == param.empty:
                if isinstance(value, Task):
                    parent = value
                    # Get the only out-port of the parent task
                    outport = next(iter(parent.outputs.values()))
                    param_type = outport.type
                elif isinstance(value, Tasklet):
                    parent = value.parent
                    outport = parent.outputs[value.out_slot_in_parent]
                    param_type = outport.type
                else:
                    py_type = type(value)
                    param_type = get_type(py_type)
            else:
                param_type = get_type(py_type)

            self._args[pname] = value
            inport = Backend.get_current_backend().get_port(
                param_type, pname, -1,
                self)  #LocalPort(param_type, pname, -1, self)
            self.inputs[pname] = inport

            if isinstance(value, Task):
                inport.is_immediate = False
                parent = value
                # Get the only out-port of the parent task
                outport = next(iter(parent.outputs.values()))

                edge = Edge(outport, inport)
                parent.edges.append(edge)
                # Unsynchronized access here since we know graph generation is
                # single threaded
                self._latch.value += 1
            elif isinstance(value, Tasklet):
                inport.is_immediate = False
                parent = value.parent
                outport = parent.outputs[value.out_slot_in_parent]

                edge = Edge(outport, inport)
                parent.edges.append(edge)
                # Unsynchronized access here since we know graph generation is
                # single threaded
                self._latch.value += 1

    def _set_outputs(self, fn, args):
        sig = self._sig
        if type(sig.return_annotation) == tuple:
            for index, ret_type in enumerate(sig.return_annotation):
                self.outputs[str(
                    index)] = Backend.get_current_backend().get_port(
                        get_type(ret_type), str(index), index, self)
        else:
            ret_type = sig.return_annotation
            type_obj = None
            if ret_type == sig.empty:
                # [FIXME] Code debt - Currently we have two dynamic types. One
                # for builtins and one for files. Here I just assume if we
                # an untyped return it is a file type. This needs fixing if we
                # want to return any untyped builtins as well.
                ret_type = 'anyfile'
                type_obj = get_type(ret_type)
            elif type(ret_type) == str and ret_type.startswith('@args'):
                # Get the actual type from the task args input
                # arg index follows '@args' prefix. Need to make it zero indexed
                arg_index = int(ret_type[5]) - 1
                # arg accessor follows the arg_index
                arg_accessor = ret_type[7:]

                arg = args[arg_index]
                ret_type = getattr(arg, arg_accessor)
                type_obj = get_type(ret_type)
            else:
                type_obj = get_type(ret_type)

            self.outputs[str(0)] = Backend.get_current_backend().get_port(
                type_obj, str(0), 0, self)

    def get_parents(self):
        parents = set()
        for name, inport in self.inputs.items():
            if not inport.is_immediate and inport.inport_edge != None:
                if inport.inport_edge.source.task_ref == None:
                    raise Exception("Null parent for out-port")

                parents.add(inport.inport_edge.source.task_ref)
        return list(parents)

    def get_children(self):
        if self.is_sink:
            return []

        children = set()

        for edge in self.edges:
            children.add(edge.dest.task_ref)
        return list(children)

    def send(self, ret):
        log.debug("Sending value {} from {}".format(ret, self.name))
        # We have multiple out-ports and we need to route return values to the
        # corresponding out-ports
        if type(ret) == tuple and type(self._sig.return_annotation) == tuple:
            for edge in self.edges:
                edge.send(ret[edge.source.index])
        else:
            log.debug("Number of edges: {}".format(len(self.edges)))
            for edge in self.edges:
                edge.send(ret)

    def receive(self):
        is_one_sided_receive = False
        for name, inport in self.inputs.items():
            if not inport.is_immediate:
                # result = inport.receive()
                # print("{} : {}".format(inport.name, result))
                is_one_sided_receive |= inport.is_one_sided_receive
                inport.receive()

        # If communication is one sided i.e: task is not actively waiting for
        # inputs we shouldn't block this thread since other in-ports needs to
        # run on this thread and push the rest of the inputs to the task
        if self._latch.value and is_one_sided_receive:
            return

        # Latch is triggered and task run when we get all the inputs
        # That will be the case if the in-ports are blocking at receive()
        # or communication is one-sided (in which case we make sure we get to
        # here only after getting all the inputs as per the conditional above).
        # If the in-ports are non blocking we wait on the `triggered` monitor
        while self._latch.value:
            with self.triggered:
                self.triggered.wait()

        Backend.get_current_backend().run_task(self)

    def run(self):
        self.send(self._runner(**self._args))
        self.graph.mark_completed(self.id)

    def dump(self):
        print("Task : {}".format(self.name))
        print("Inputs :")
        for name, inport in self.inputs.items():
            print("{}".format(name))
        print("Output :")
        for name, outport in self.outputs.items():
            print("{}".format(name))
        for edge in self.edges:
            edge.dump()
        print("------------")


# Notes : ports take care of inter task communication
class FusedTask(Task):
    """ A container task for multiple tasks fused together"""

    def __init__(self, tasks):
        if tasks == None or len(tasks) == 0:
            raise Exception(
                "Internal compiler error. Tried fusing an empty task list")

        # If there is only one task nothing to fuse
        if len(tasks) == 1:
            return tasks

        self.name = '__'.join(map(lambda task: task.name, tasks))
        self.tasks = tasks
        self.head = tasks[0]
        self.tail = tasks[-1]

        # Default initializing other task flags
        self.is_fusee = False
        self.is_fused = True
        self.is_source = False
        self.is_sink = False
        self.is_staging = False
        self.is_transform = False

        for task in self.tasks:
            task.is_fusee = True

        # Now transplant edges of the fused tasks with local ports
        def transplant(edge):
            source = edge.source
            current_backend = Backend.get_current_backend()
            if not current_backend.name == 'LOCAL_NON_THREADED':
                source = Backend.get_backend(
                    BackendConfig(BackendType.LOCAL_NON_THREADED,
                                  'Local Non Threaded')).get_port(
                                      source.type, source.name, source.index,
                                      source.task_ref)
                # Update the task out-port to be a local port
                source.task_ref.outputs[source.name] = source
                # Update the edge
                edge.source = source

            dest = edge.dest
            if not current_backend.name == 'LOCAL_NON_THREADED':
                dest = Backend.get_backend(
                    BackendConfig(BackendType.LOCAL_NON_THREADED,
                                  'Local Non Threaded')).get_port(
                                      dest.type, dest.name, dest.index,
                                      dest.task_ref)
                # Update the task in-port to be a local port
                dest.task_ref.inputs[source.name] = dest
                # Update the edge
                edge.dest = dest
            return edge

        # Make all edges of intermediate tasks to contain local ports
        for task in tasks[:len(tasks) - 1]:
            task.edges = list(map(lambda edge: transplant(edge), task.edges))

        # Now assume head task's in-ports
        self._args = self.head._args
        self._latch = self.head._latch
        self.triggered = self.head.triggered
        self.inputs = self.head.inputs

        for name, inport in self.head.inputs.items():
            inport.task_ref = self

        # Also keep a reference to tail task's edges
        self.edges = self.tail.edges

        # Make this task inputs to be that of the head task
        self.inputs = self.head.inputs

    def run(self):
        # Push the inputs that we accepted on behalf of the head task through
        # the head task
        self.head.send(self.head._runner(**self._args))


class TaskGraph(object):
    """ TaskGraph is the intermediate representation (IR) of the workflow

    This is the IR which is fed through the graph compiler and then optimized
    and then code generated against

    Attributes:
        name: Graph name. Defaults the @app annotated function name
        tasks: A dictionary of tasks belonging to this graph. Key is task UUID
        sources: A dictionary of tasks which are sources of graph. Key is task
            UUID
        num_tasks: Number of executable units in the graph. A fused task is 
            considered as one executable unit. So any tasks contained within a
            fused task is not counted towards num_tasks
        
        completed_tasks: Runtime control value for the number of tasks 
            completed so far in the task graph
        done: Runtime monitor which will be notified once the graph execution 
            is completed
    """

    name = None
    tasks = {}
    fusee_map = defaultdict()
    sources = {}
    num_tasks = 0

    # Runtime controls for task graph execution
    completed_tasks = Value('i', 0)
    done = Condition()

    def mark_completed(self, tid):
        with self.completed_tasks.get_lock():
            self.completed_tasks.value += 1

        if self.completed_tasks.value == self.get_num_tasks():
            with self.done:
                self.done.notify()

    def add_task(self, task):
        task.id = uuid.uuid1()
        self.tasks[task.id] = task
        task.graph = self

    def get_task(self, tid):
        return self.tasks[tid]

    def set_source(self, task):
        task.is_source = True
        self.sources[task.id] = task

    def unset_source(self, task):
        task.is_source = False
        self.sources.pop(task.id, None)

    def dump(self):
        for task in tasks:
            task.dump()

    def get_num_tasks(self):
        return self.num_tasks
        # count = 0
        # tasks = set()
        # for tid, task in self.tasks.items():
        # tasks.add(task)

        # # Now remove any tasks within fused tasks
        # for tid, task in self.tasks.items():
        # if isinstance(task, FusedTask):
        # for t in task.tasks:
        # tasks.remove(t)
        # return len(tasks)

    def print_fused_graph(self):
        for tid, task in self.tasks.items():
            if task.is_fused:
                print("Task : %s" % task.name)
                print("Edges :")
                for edge in task.edges:
                    print("     Source : %s Destination : %s" % (edge.source.task_ref, edge.dest.task_ref))
                print("Head : %s" % task.head)
                print("Tail : %s\n" % task.tail)

    def __str__(self):
        print("Sources")
        print("-------")
        print(", ".join(list(map(lambda x : str(self.sources[x]), self.sources.keys()))))

        print("\n")
        print("Vertices")
        print("--------")
        for tid, task in self.tasks.items():
            print("task %s" % task)
            print("parents : %s" % (", ".join(list(map(lambda x: str(x), task.get_parents())))))
            print("children : %s" % (", ".join(list(map(lambda x: str(x), task.get_children())))))
            print("\n")
        return ""

class PreProcess(Pass):
    def __init__(self, name):
        Pass.__init__(self, name)
        self.description = "Preprocessing the graph"

    def run(self, graph, ctx):
        Pass.run(self, graph, ctx)
        for tid, task in graph.tasks.items():
            # Infer if the task is a source
            is_source = True
            for name, inport in task.inputs.items():
                if not inport.is_immediate:
                    is_source = False
                    break
            if is_source:
                graph.set_source(task)

            # Infer if the task is a sink and generate sink out-ports
            if not task.edges:
                task.is_sink = True
                # If there no out edges it means this is a sink and all outputs
                # are sinks. Make them so...
                current_backend = Backend.get_current_backend()
                for name, outport in task.outputs.items():
                    # If current out port is not a local port make it so
                    if not current_backend.name == 'LOCAL_NON_THREADED':
                        outport = Backend.get_backend(
                            BackendConfig(BackendType.LOCAL_NON_THREADED,
                                          'Local Non Threaded')).get_port(
                                              outport.type, outport.name,
                                              outport.index, outport.task_ref)
                        task.edges.append(Edge(outport, Sink(outport)))
        return PassResult.CONTINUE

    def post_run(self, graph, ctx):
        pass


class PostProcess(Pass):
    def __init__(self, name):
        Pass.__init__(self, name)
        self.description = "Post processing the graph"

    def run(self, graph, ctx):
        Pass.run(self, graph, ctx)

        count = 0
        tasks = set()
        for tid, task in graph.tasks.items():
            tasks.add(task)

        # Now remove any tasks within fused tasks
        for tid, task in graph.tasks.items():
            if isinstance(task, FusedTask):
                for t in task.tasks:
                    tasks.remove(t)

        graph.num_tasks = len(tasks)

    def post_run(self, graph, ctx):
        pass


def gen_runner(fn, sig):
    def run_task(**kwargs):
        ctx = HandlerContext(fn)
        ctx.args = kwargs
        ctx.sig = sig

        log.debug("Running pre handlers")
        log.debug(HandlerRegistry().pre_handlers)
        for pre in HandlerRegistry.pre_handlers:
            pre.run(ctx)

        ret = None
        try:
            ret = ctx.fn(**kwargs)
        except:
            # [TODO] Ideally we want to match the original line info in the
            # printed trace back for better script debuggability. We should
            # probably be able to do that by playing with the exception stack
            # trace.
            traceback.print_exc()
        ctx.ret = ret

        for post in HandlerRegistry.post_handlers:
            post.run(ctx)
        return ret

    return run_task


def gen_task(fn, sig, args, kwargs):
    task = Task(gen_runner(fn, sig), fn, sig, args, kwargs)
    # Check if the task returns multiple values.
    rets = sig.return_annotation
    tasklets = []
    if type(rets) == tuple:
        # [TODO] Support named out-ports. One way to do that would be to
        # specify the return annotation as
        #  ... -> 'return_1:typ_1, ..., return_N:typ_N'
        for index, ret in enumerate(rets):
            tasklets.append(Tasklet(task, index))
    tup = gen_tuple(tasklets)
    if tup == None:
        log.error("Task returning more than 10 outputs.")
        raise Exception("Task returning more than 10 outputs")
    return (task, tup)
