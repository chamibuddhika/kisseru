import ast
import inspect
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


params = {'split': None}


def task(**params):
    def decorator(func):
        sig = signature(func)
        # new_sig = sig.replace(return_annotation=Signature.empty)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Stage(func.__name__, args, kwargs)
            ctx = Context(func)

            print("Running pre handlers")
            print(HandlerRegistry().pre_handlers)
            for pre in HandlerRegistry.pre_handlers:
                pre.run(ctx)

            ret = ctx.fn(*args, **kwargs)
            ctx.ret = ret

            for post in HandlerRegistry.post_handlers:
                post.run(ctx)

            return ret

        # wrapper.__signature__ = new_sig
        logger.info("Return wrapper")
        return wrapper

    return decorator
