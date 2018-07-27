import inspect
import uuid
import logging

from typed import get_type
from passes import Pass
from passes import PassResult
from colors import Colors

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

    def dump(self):
        self.source.dump()
        self.dest.dump()


class Tasklet(object):
    def __init__(self, parent):
        self.parent = parent
        self.out_slot_in_parent = -1


class Task(object):
    def __init__(self, runner, ctx, args, kwargs):
        self._runner = runner
        self._ctx = ctx
        self._fn = ctx.fn
        self._args = {}
        self._latch = 0

        self.inputs = {}
        self.outputs = {}
        self.edges = []
        self.name = ctx.fn.__name__
        self.id = None
        self.is_source = False
        self.is_sink = False
        self.is_staging = False
        self.is_transform = False

        self._set_inputs(ctx.fn, args, kwargs)
        self._set_outputs(ctx.fn)

    def _set_inputs(self, fn, args, kwargs):
        sig = inspect.signature(fn)
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
            if py_type == inspect.Parameter.empty:
                py_type = type(value)

            param_type = None
            if isinstance(value, Task):
                parent = value
                # Get the only out-port of the parent task
                outport = next(iter(parent.outputs.values()))
                param_type = outport.type
            elif isinstance(value, Task):
                parent = value.parent
                outport = parent.outputs[value.out_slot_in_parent]
                param_type = outport.type
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
            '''
            if not isinstance(value, param_type):
                if is_coerceable(value, param_type):
                    pass
                else:
                    raise Exception(
                        "Type Error: {} does not match type {}".format(
                            str(value), str(param_type)))
                # print("%s : %s" % (str(value), str(param_type)))
            '''

    def _set_outputs(self, fn):
        sig = inspect.signature(fn)
        if type(sig.return_annotation) == tuple:
            for index, ret_type in enumerate(sig.return_annotation):
                self.outputs[str(index)] = Port(
                    get_type(ret_type), str(index), index, self)
        else:
            self.outputs[str(0)] = Port(
                get_type(sig.return_annotation), str(0), 0, self)

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
        self.sources[task.id] = task

    def dump(self):
        for task in tasks:
            task.dump()


class PreProcess(Pass):
    def __init__(self, name):
        Pass.__init__(self, name)

    def run(self, graph, ctx):
        for tid, task in graph.tasks.items():
            # Infer if the task is a source
            is_source = True
            for name, inport in task.inputs.items():
                if not inport.is_immediate:
                    is_source = False
                    break
            if is_source:
                task.is_source = True
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
