import inspect
import uuid
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


class Port(object):
    def __init__(self, typ, name, index, task):
        self.type = typ
        self.name = name
        self.index = index
        self.task_ref = task
        self.is_immediate = True

    def send(self, value):
        self.task_ref.retrive(self, value)

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


class Sink(Port):
    def __init__(self, port):
        Port.__init__(self, port.type, port.name, port.index, port.task_ref)

    def send(self, value):
        print(Colors.OKBLUE +
              "[KISSERU] Pipeline output : {}".format(str(value)) +
              Colors.ENDC)

    def dump(self):
        Port.dump(self)


class Edge(object):
    def __init__(self, source, dest):
        self.source = source
        self.dest = dest
        self.needs_transform = False

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
            inport = Port(param_type, pname, -1, self)
            self.inputs[pname] = inport

            if isinstance(value, Task):
                inport.is_immediate = False
                parent = value
                # Get the only out-port of the parent task
                outport = next(iter(parent.outputs.values()))

                parent.edges.append(Edge(outport, inport))
                self._latch += 1
            elif isinstance(value, Tasklet):
                inport.is_immediate = False
                parent = value.parent
                outport = parent.outputs[value.out_slot_in_parent]

                parent.edges.append(Edge(outport, inport))
                self._latch += 1

    def _set_outputs(self, fn, args):
        sig = self._sig
        if type(sig.return_annotation) == tuple:
            for index, ret_type in enumerate(sig.return_annotation):
                self.outputs[str(index)] = Port(
                    get_type(ret_type), str(index), index, self)
        else:
            ret_type = sig.return_annotation
            type_obj = None
            if ret_type == sig.empty:
                # [FIXME] Code debt - Currently we have two dyanmic types. One
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

            self.outputs[str(0)] = Port(type_obj, str(0), 0, self)

    def _send(self, ret):
        log.debug("Sending value {} from {}".format(ret, self.name))
        if type(ret) == tuple:
            for edge in self.edges:
                send_val = ret[edge.source.index]
                edge.dest.send(send_val)
        else:
            log.debug("Number of edges: {}".format(len(self.edges)))
            for edge in self.edges:
                edge.dest.send(ret)

    def run(self):
        self._send(self._runner(**self._args))

    def retrive(self, inport, value):
        self._args[inport.name] = value
        self._latch -= 1

        # Latch is triggered when we get all the inputs
        if not self._latch:
            self.run()

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
