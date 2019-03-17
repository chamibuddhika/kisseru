import time

from colors import Colors
from handler import Handler
from logger import LogColor
from backend import Backend


class Timer:
    def __init__(self, func=time.perf_counter):
        self._elapsed = 0.0
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
        self._elapsed += (end - self._start)
        self._start = None

    def reset(self):
        self.elapsed = 0

    def elapsed(self):
        m, s = divmod(int(self._elapsed), 60)
        h, m = divmod(m, 60)
        return "{}h:{}m:{}s".format(h, m, s)

    @property
    def running(self):
        return self._start is not None

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args):
        self.stop()


class ProfilerEntry(Handler):
    def __init__(self, name):
        Handler.__init__(self, name)

    def run(self, ctx):
        timer = Timer()
        ctx.set('__timer__', timer)
        timer.start()


class ProfilerExit(Handler):
    def __init__(self, name):
        Handler.__init__(self, name)

    def run(self, ctx):
        timer = ctx.get('__timer__')
        timer.stop()

        logger = Backend.get_current_backend().logger

        log_str = logger.fmt(
            "[Runner] {} took {}".format(ctx.get('__name__'), timer.elapsed()),
            LogColor.GREEN)
        logger.log(log_str)
        '''
        print(Colors.OKGREEN + "[Runner] {} took {}".format(
            ctx.get('__name__'), timer.elapsed()) + Colors.ENDC)
        '''
