import abc

from enum import Enum


class PassResult(Enum):
    CONTINUE = 1,
    WARN = 2,
    ERROR = 3


class PassContext(object):
    def __init__(self):
        self.errors = []
        self.warnings = []
        self.properties = {}


class Pass(metaclass=abc.ABCMeta):
    def __init__(self, name):
        self.name = name

    @abc.abstractmethod
    def run(self, graph, ctx):
        pass

    @abc.abstractmethod
    def post_run(self, graph, ctx):
        pass


class PassManager(object):
    passes = []

    @staticmethod
    def register_pass(p):
        PassManager.passes.append(p)
