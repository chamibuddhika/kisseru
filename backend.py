import abc

from enum import Enum


class BackendType(Enum):
    LOCAL_NON_THREADED = 1
    LOCAL = 2
    SLURM = 3


class BackendConfig(object):
    def __init__(self, backend_type, name):
        self.backend_type = backend_type
        self.name = name


class Backend(metaclass=abc.ABCMeta):
    backends = {}
    cur_backend = None

    def __init__(self, backend_config):
        self.config = backend_config
        self.name = backend_config.name
        self.logger = None

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
    def run_task(self, task):
        pass

    @abc.abstractmethod
    def run_flow(self, graph):
        pass

    @abc.abstractmethod
    def cleanup(self, graph):
        pass

    @classmethod
    def register_backend(cls, backend):
        cls.backends[backend.name] = backend

    @classmethod
    def get_backend(cls, backend_config):
        backend_type = backend_config.backend_type
        backend = None
        if backend_type == BackendType.LOCAL_NON_THREADED:
            backend = cls.backends['LOCAL_NON_THREADED'](backend_config)
        elif backend_type == BackendType.LOCAL:
            backend = cls.backends['LOCAL'](backend_config)
        elif backend_type == BackendType.SLURM:
            backend = cls.backends['SLURM'](backend_config)
        return backend

    @classmethod
    def set_current_backend(cls, backend_config):
        cls.cur_backend = cls.get_backend(backend_config)

    @classmethod
    def get_current_backend(cls):
        return cls.cur_backend
