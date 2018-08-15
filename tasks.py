import inspect
import uuid
import threading
import abc
import traceback
import logging

from multiprocessing import Value
from multiprocessing import Condition

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
        pass

    @abc.abstractmethod
    def receive(self, value=None, from_port=None):
        pass

    def notify_task(self):
        with self.task_ref._latch.get_lock():
            self.task_ref._latch.value -= 1
        with self.task_ref.triggered:
            self.task_ref.triggered.notify()

    def flip_is_immediate(self):
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
    def __init__(self, port):
        Port.__init__(self, port.type, port.name, port.index, port.task_ref)

    def send(self, value, to_port):
        pass

    def receive(self, value, from_port):
        logger = Backend.get_current_backend().logger
        log_str = logger.fmt(
            "[KISSERU] Pipeline output : {}".format(str(value)), LogColor.BLUE)
        logger.log(log_str)

    def dump(self):
        Port.dump(self)


class Edge(object):
    def __init__(self, source, dest):
        self.source = source
        self.dest = dest
        self.send_value = None
        self.needs_transform = False
        self.dest.inport_edge = self

    def send(self, value):
        log.debug("Sending value {} to task {}".format(
            value, self.dest.task_ref.name))
        self.source.send(value, self.dest)

    def dump(self):
        self.source.dump()
        self.dest.dump()


class Tasklet(object):
    def __init__(self, parent, index):
        self.parent = parent
        self.out_slot_in_parent = index


class Task(object):
    def __init__(self, runner, fn, sig, args, kwargs):
        self._runner = runner
        self._fn = fn
        self._sig = sig
        self._args = {}
        self._latch = Value('i', 0)
        self.triggered = Condition()

        self.inputs = {}
        self.outputs = {}
        self.edges = []
        self.name = fn.__name__
        self.id = None
        self.graph = None

        self.is_fusee = False
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

    def run(self):
        # Push the inputs that we accepted on behalf of the head task through
        # the head task
        self.head.send(self.head._runner(**self._args))


class TaskGraph(object):
    name = None
    tasks = {}
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
