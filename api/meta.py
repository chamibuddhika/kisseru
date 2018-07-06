
import abc
import time
import types

class MetaClassDecorator(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def decorate(cls, attr):
        pass

class MetaClassManager(abc.ABCMeta):

    decorators = []

    @classmethod
    def register_decorator(cls, decorator):
        cls.decorators.append(decorator)

    def __new__(cls, clsname, bases, dct):
        for decorator in cls.decorators:
            dct = decorator.decorate(cls, dct)
        return super(MetaClassManager, cls).__new__(cls, clsname, bases, dct)
