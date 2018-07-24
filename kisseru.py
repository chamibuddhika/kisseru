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


class Types(Enum):
    CSV = 1
    ESRI = 2
    GRB = 3
    NETCDF4 = 4


class Stage(object):
    def __init__(self, task, args, kwargs):
        self.task = task
        self.args = args
        self.kwargs = kwargs
        self.uuid = uuid.uuid1()


def process_fn(func):
    fnIR = parse_fn(func)
    handle_scripts(fnIR)
    return recompile(fnIR, func)


class Port(object):
    def __init__(self, typ, index, task):
        self.type = typ
        self.index = index
        self.task_ref = task


class Edge(object):
    def __init__(self):
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
        self.args = {}
        self.inputs = {}
        self.outputs = {}
        self.edges = []
        self.name = fn.__name__
        self.id = None

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

            if isinstance(value, Task):
                pass
            self.args[pname] = value
            self.inputs[pname] = Port(param_type, pname, self)
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

    def set_id(self, tid):
        self.id = tid

    def run(self):
        return self._runner(**self.args)


class TaskGraph(object):
    _tasks = {}

    def add_task(self, task):
        task.set_id(uuid.uuid1())
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
        print(ctx.ret)

        for post in HandlerRegistry.post_handlers:
            post.run(ctx)
        return ret

    return run_task


def task(**params):
    def decorator(func):
        # new_sig = sig.replace(return_annotation=Signature.empty)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            global _graph
            task = Task(gen_runner(func), func, args, kwargs)
            _graph.add_task(task)
            return task.run()

        # wrapper.__signature__ = new_sig
        log.info("Return wrapper")
        return wrapper

    return decorator
