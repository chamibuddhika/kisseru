import inspect
import uuid
import threading
import abc
import traceback
import logging

from typed import get_type
from passes import Pass
from passes import PassResult
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
        self.task_ref._latch -= 1
        with self.task_ref.triggered:
            self.task_ref.triggered.notify()

    def flip_is_immediate(self):
        if self.is_immediate:
            self.is_immediate = False
            self.task_ref._latch += 1
        else:
            self.is_immediate = True
            self.task_ref._latch -= 1

    def dump(self):
        print("Port : {} {} {} {}".format(self.type, self.name, self.index,
                                          self.task_ref.name))


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


class Sink(Port):
    def __init__(self, port):
        Port.__init__(self, port.type, port.name, port.index, port.task_ref)

    def send(self, value, to_port):
        pass

    def receive(self, value, from_port):
        print(Colors.OKBLUE +
              "[KISSERU] Pipeline output : {}".format(str(value)) +
              Colors.ENDC)

    def dump(self):
        Port.dump(self)


class Edge(object):
    def __init__(self, source, dest):
        self.source = source
        self.dest = dest
        self.send_value = None
        self.needs_transform = False

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
        self._latch = 0
        self.triggered = threading.Condition()

        self.inputs = {}
        self.outputs = {}
        self.edges = []
        self.name = fn.__name__
        self.id = None
        self.is_source = False
        self.is_sink = False
        self.is_staging = False
        self.is_transform = False

        self._set_inputs(fn, args, kwargs)
        self._set_outputs(fn, args)

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
            inport = LocalPort(param_type, pname, -1, self)
            self.inputs[pname] = inport

            if isinstance(value, Task):
                inport.is_immediate = False
                parent = value
                # Get the only out-port of the parent task
                outport = next(iter(parent.outputs.values()))

                edge = Edge(outport, inport)
                parent.edges.append(edge)
                inport.inport_edge = edge
                self._latch += 1
            elif isinstance(value, Tasklet):
                inport.is_immediate = False
                parent = value.parent
                outport = parent.outputs[value.out_slot_in_parent]

                edge = Edge(outport, inport)
                parent.edges.append(edge)
                inport.inport_edge = edge
                self._latch += 1

    def _set_outputs(self, fn, args):
        sig = self._sig
        if type(sig.return_annotation) == tuple:
            for index, ret_type in enumerate(sig.return_annotation):
                self.outputs[str(index)] = LocalPort(
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

            self.outputs[str(0)] = LocalPort(type_obj, str(0), 0, self)

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
                is_one_sided_receive |= inport.receive()

        # If communication is one sided i.e: task is not actively waiting for
        # inputs we shouldn't block this thread since other in-ports needs to
        # run on this thread and push the rest of the inputs to the task
        if self._latch and is_one_sided_receive:
            return

        # Latch is triggered and task run when we get all the inputs
        # That will be the case if the in-ports are blocking at receive()
        # or communication is one-sided (in which case we make sure we get to
        # here only after getting all the inputs as per the conditional above).
        # If the in-ports are non blocking we wait on the `triggered` monitor
        while self._latch:
            with self.triggered:
                self.triggered.wait()
        self.run()

    def run(self):
        self.send(self._runner(**self._args))

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


class FusedTask(Task):
    def __init__(self, tasks):
        if tasks == None or len(tasks) == 0:
            raise Exception(
                "Internal compiler error. Tried fusing an empty task list")

        # If there is only one task nothing to fuse
        if len(tasks) == 1:
            return tasks

        self.head = tasks[0]
        self.tail = tasks[-1]

        # Now transplant edges of the fused tasks with local ports
        def transplant(edge):
            source = edge.source
            if not isinstance(source, LocalPort):
                source = LocalPort(source.type, source.name, source.index,
                                   source.task_ref)
                # Update the task out-port to be a local port
                source.task_ref.outport[source.name] = source
                # Update the edge
                edge.source = source

            dest = edge.dest
            if not isinstance(dest, LocalPort):
                dest = LocalPort(dest.type, dest.name, dest.index,
                                 dest.task_ref)
                # Update the task in-port to be a local port
                dest.task_ref.inport[source.name] = dest
                # Update the edge
                edge.dest = dest
            return edge

        # Make all edges of intermediate tasks to contain local ports
        for task in tasks[:len(tasks - 1)]:
            task.edges = map(lambda edge: transplant(edge), task.edges)

        # Now assume head task's in-ports
        self.args = head.args
        self._latch = head._latch
        self.triggered = head.triggered
        self.inputs = head.inputs

        for inport in head.inputs:
            inport.task_ref = self

        # Also keep a reference to tail task's edges
        self.edges = tail.edges

    def run(self):
        # Push the inputs that we accepted on behalf of the head task through
        # the head task
        self.head.send(self.head._runner(**self._args))


class TaskGraph(object):
    name = None
    tasks = {}
    sources = {}

    def add_task(self, task):
        task.id = uuid.uuid1()
        self.tasks[task.id] = task

    def set_source(self, task):
        task.is_source = True
        self.sources[task.id] = task

    def unset_source(self, task):
        task.is_source = False
        self.sources.pop(task.id, None)

    def dump(self):
        for task in tasks:
            task.dump()


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
                for name, outport in task.outputs.items():
                    task.edges.append(Edge(outport, Sink(outport)))
        return PassResult.CONTINUE

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
