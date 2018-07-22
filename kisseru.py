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
from inspect import Signature
from inspect import signature

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

_CSV = 'CSV'

logger = logging.getLogger(__name__)

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
    _CSV = 1
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


class Task(object):
    def __init__(self, fn, args, kwargs):
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.id = None

    def set_id(self, tid):
        self.id = tid


class TaskGraph(object):
    _tasks = {}

    def add_task(self, task):
        task.set_id(uuid.uuid1())
        self._tasks[task.id] = task


_graph = TaskGraph()

params = {'split': None}


def gen_runner(fn, *args, **kwargs):
    def run_task():
        ctx = Context(fn)

        print("Running pre handlers")
        print(HandlerRegistry().pre_handlers)
        for pre in HandlerRegistry.pre_handlers:
            pre.run(ctx)

        print(args)
        print(kwargs)
        # [FIXME] Doesn't handle default arguments at the moment. Fix it.
        ret = None
        try:
            ret = ctx.fn(*args, **kwargs)
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
        sig = signature(func)
        # new_sig = sig.replace(return_annotation=Signature.empty)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            global _graph
            task = Task(gen_runner(func, *args, **kwargs), args, kwargs)
            _graph.add_task(task)
            return task.fn()

        # wrapper.__signature__ = new_sig
        logger.info("Return wrapper")
        return wrapper

    return decorator
