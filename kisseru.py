import ast
import inspect
import types
import re
import functools

from enum import Enum

from inspect import Signature
from inspect import signature

from func import parse_fn
from func import recompile
from bash import handle_scripts

# Need these to be in global environment when recompile is run
from bash import run_script
from bash import set_assignments

_CSV = 'CSV'


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
            fn = process_fn(func)
            print("Invoking the modified function...")
            ret = fn(*args, **kwargs)
            return ret

        # wrapper.__signature__ = new_sig
        print("Return wrapper...")
        return wrapper

    return decorator


@task(split='dfd')
def add(a: int, b: int, c: int) -> _CSV:
    b = a + c \
            + a
    '''bash ls -al > %{b}.txt'''
    '''bash %{d} =`cat %{b}.txt`'''
    return d


if __name__ == "__main__":
    print(add(1, 2, 3))
