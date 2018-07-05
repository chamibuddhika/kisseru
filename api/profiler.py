
import time
import types
import functools

from meta import MetaClassDecorator

class Timer:
    def __init__(self, func=time.perf_counter):
        self.elapsed = 0.0
        self._func = func
        self._start = None

    def start(self):
        if self._start is not None:
            raise RuntimeError('Already started')
        self._start = self._func()

    def stop(self):
        if self._start is None:
            raise RuntimeError('Not started')
        end = self._func()
        self.elapsed += end - self._start
        self._start = None

    def reset(self):
        self.elapsed = 0.0

    @property
    def running(self):
        return self._start is not None

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args):
        self.stop()


class Profiler(MetaClassDecorator):

    def timefunc(self, fn, *args, **kwargs):
        @functools.wraps(fn)
        def fncomposite(*args, **kwargs):
            timer = Timer()
            timer.start()
            rt = fn(*args, **kwargs)
            timer.stop()
            print("Executing %s took %s seconds." % (fn.__name__, timer.elapsed))
            return rt
        # return the composite function
        return fncomposite

    def decorate(self, cls, attr):
        for name, value in attr.items():
            if type(value) is types.FunctionType or type(value) is types.MethodType:
                if name == "run":
                    attr[name] = self.timefunc(value) 
        return attr
