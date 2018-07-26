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
from handler import HandlerContext
from handler import HandlerRegistry
from logger import LoggerEntry
from logger import LoggerExit
from func import ASTOps
from profiler import ProfilerEntry
from profiler import ProfilerExit
from typed import get_type
from typed import Type
from utils import gen_tuple
from passes import Pass
from passes import PassManager
from passes import PassContext
from passes import PassResult
from tasks import Task
from tasks import PreProcess
from tasks import TaskGraph
from dot import DotGraphGenerator
from colors import Colors

_CSV = 'CSV'

log = logging.getLogger(__name__)

# Setup kisseru logging
logging.basicConfig(level=logging.INFO)

# Setup kisseru handlers
prof_entry = ProfilerEntry("ProfilerEntry")
prof_exit = ProfilerExit("ProfilerExit")
logger_entry = LoggerEntry("LoggerEntry")
logger_exit = LoggerExit("LoggerExit")
ast_ops = ASTOps("ASTOps")

HandlerRegistry.register_init_handler(ast_ops)

HandlerRegistry.register_pre_handler(logger_entry)
HandlerRegistry.register_pre_handler(prof_entry)
HandlerRegistry.register_post_handler(prof_exit)
HandlerRegistry.register_post_handler(logger_exit)

# Setup graph IR passes
preprocess = PreProcess("Graph Preprocess")
dot = DotGraphGenerator("Dot Graph Generation")

PassManager.register_pass(preprocess)
PassManager.register_pass(dot)

params = {'split': None}


def gen_runner(fn):
    def run_task(**kwargs):
        ctx = HandlerContext(fn)
        ctx.args = kwargs

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


def gen_task(ctx, args, kwargs):
    task = Task(gen_runner(ctx.fn), ctx, args, kwargs)
    # Check if the task returns multiple values.
    rets = inspect.signature(ctx.fn).return_annotation
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


_graph = TaskGraph()


def task(**configs):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Run task init handlers
            ctx = HandlerContext(func)
            for init in HandlerRegistry.init_handlers:
                init.run(ctx)

            global _graph
            task, tasklets = gen_task(ctx, args, kwargs)
            _graph.add_task(task)
            if tasklets == ():
                return task
            else:
                return tasklets

        return wrapper

    return decorator


def app(**configs):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            _graph.name = func.__name__
            print(Colors.OKBLUE +
                  "[KISSERU] Running pipeline {}".format(_graph.name) +
                  Colors.ENDC)
            print("========================================")
            print("")
            func(*args, **kwargs)
            return _graph

        log.info("Return app")
        return wrapper

    return decorator


class AppRunner(object):
    def __init__(self, app):
        self.app = app

    def run(self):
        # Get the task graph by running the app specification
        graph = self.app()

        # Now run the passes on the graph IR. PassContext holds any errors
        # encountered during the graph processing. We fail fast if we encounter
        # any errors during a pass.
        ctx = PassContext()
        for p in PassManager.passes:
            res = p.run(graph, ctx)
            if res == PassResult.ERROR:
                # [TODO] Print user friendly error message using the ctx
                # information here
                raise Exception("Aborting pipeline compilation due to errors")

        # for tid, task in graph.tasks.items():
        # print("Dumping task {}".format(str(tid)))
        # task.dump()

        # Finally push the validated (and hopefully optimized) graph IR to
        # specified code generation backend or runner given we didn't encounter
        # any errors during the graph processing passes
        for tid, source in graph.sources.items():
            source.run()

        # Run any post code generation tasks which passes may run for
        # tearing down or saving computed results
        for p in PassManager.passes:
            res = p.post_run(graph, ctx)
