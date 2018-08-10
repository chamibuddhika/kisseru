import abc

from enum import Enum


class BackendType(Enum):
    LOCAL_NON_THREADED = 1
    LOCAL = 2
    SLURM = 3


class BackendConfig(object):
    def __init__(self, backend_type):
        self.backend_type = backend_type


class Backend(metaclass=abc.ABCMeta):
    backends = {}
    cur_backend = None

    def __init__(self, backend_config):
        self.backend = backend_config

    @abc.abstractmethod
    def get_port(self, typ, name, index, task):
        pass

    @abc.abstractmethod
    def package(self):
        pass

    @abc.abstractmethod
    def deploy(self):
        pass

    @abc.abstractmethod
    def run(self):
        pass

    @classmethod
    def register_backend(cls, backend):
        cls.backends[backend.name] = backend

    @classmethod
    def get_backend(cls, backend_config):
        backend_type = backend_config.backend_type
        if backend_type == BackendType.LOCAL_NON_THREADED:
            cls.cur_backend = cls.backends['LOCAL_NON_THREADED'](
                backend_config)
        elif backend_type == BackendType.LOCAL:
            cls.cur_backend = cls.backends['LOCAL'](backend_config)
        elif backend_type == BackendType.SLURM:
            raise Exception("Slurm backend not implemented yet.")
        return cls.cur_backend

    @classmethod
    def get_current_backend(cls):
        return cls.cur_backend
