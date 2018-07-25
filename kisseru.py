import ast
import uuid
import inspect
import traceback
import types
import re
import functools
import logging
import time

from enum import Enum

from func import parse_fn
from func import recompile
from bash import handle_scripts
from handler import Context
from handler import HandlerRegistry
from logger import LoggerEntry
from logger import LoggerExit
from func import ASTOps
from profiler import ProfilerEntry
from profiler import ProfilerExit
from typed import get_type
from typed import Type
from utils import gen_tuple

_CSV = 'CSV'

log = logging.getLogger(__name__)

# Setup kiseru logging
logging.basicConfig(level=logging.INFO)

# Setup kiseru handlers
prof_entry = ProfilerEntry("ProfilerEntry")
prof_exit = ProfilerExit("ProfilerExit")
logger_entry = LoggerEntry("LoggerEntry")
logger_exit = LoggerExit("LoggerExit")
ast_ops = ASTOps("ASTOps")

HandlerRegistry.register_prehandler(logger_entry)
HandlerRegistry.register_prehandler(ast_ops)
HandlerRegistry.register_prehandler(prof_entry)
HandlerRegistry.register_posthandler(prof_exit)
HandlerRegistry.register_posthandler(logger_exit)


class Port(object):
    def __init__(self, typ, index, task):
        self.type = typ
        self.index = index
        self.task_ref = task


class Edge(object):
    def __init__(self, source, dest):
        self.source = None
        self.dest = None


class Tasklet(object):
    def __init__(self, parent):
        self.parent = parent
        self.out_slot_in_parent = -1


class Task(object):
    def __init__(self, runner, fn, args, kwargs):
        self._runner = runner
        self._fn = fn
        self._args = {}

        self.inputs = {}
        self.outputs = {}
        self.edges = []
        self.name = fn.__name__
        self.id = None
        self.latch = 0

        self._set_inputs(fn, args, kwargs)
        self._set_outputs(fn)

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
            param_type = get_type(py_type)

            self._args[pname] = value
            inport = Port(param_type, pname, self)
            self.inputs[pname] = inport

            if isinstance(value, Task):
                parent = value
                # Get the only out-port of the parent task
                outport = next(iter(parent.outputs.values()))
                parent.edges.append(Edge(outport, inport))
                self.latch += 1
            elif isinstance(value, Tasklet):
                parent = value.parent
                outport = parent.outputs[value.out_slot_in_parent]
                parent.edges.append(Edge(outport, inport))
                self.latch += 1
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
                    get_type(ret_type), str(index), self)
        else:
            self.outputs[str(0)] = Port(
                get_type(sig.return_annotation), str(0), self)

    def run(self):
        return self._runner(**self._args)


class TaskGraph(object):
    _tasks = {}

    def add_task(self, task):
        task.id = uuid.uuid1()
        self._tasks[task.id] = task


_graph = TaskGraph()

params = {'split': None}


def gen_runner(fn):
    def run_task(**kwargs):
        ctx = Context(fn)

        log.info("Running pre handlers")
        log.info(HandlerRegistry().pre_handlers)
        for pre in HandlerRegistry.pre_handlers:
            pre.run(ctx)

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


def gen_task(func, args, kwargs):
    task = Task(gen_runner(func), func, args, kwargs)
    # Check if the task returns multiple values.
    rets = inspect.signature(func).return_annotation
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


def task(**params):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            global _graph
            task, tasklets = gen_task(func, args, kwargs)
            _graph.add_task(task)
            if tasklets == ():
                return task.run()
            else:
                return tasklets

        log.info("Return wrapper")
        return wrapper

    return decorator
