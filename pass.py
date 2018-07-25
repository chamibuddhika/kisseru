import abc

from enum import Enum


class Result(Enum):
    CONTINUE,
    WARN,
    ERROR


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
