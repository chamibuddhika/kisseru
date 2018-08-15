from enum import Enum

from multiprocessing import Lock

from colors import Colors
from handler import Handler


class LogColor(Enum):
    GREEN = 0
    YELLOW = 1
    RED = 2
    BLUE = 3


class TaskLogger(object):
    def __init__(self, logfile):
        self.fp = open(logfile, "a")

    def _strip_ansi_colors(self, log_str):
        new_log_str = ''
        skip = False
        for c in log_str:
            if c == '\033':
                skip = True
                continue
            elif c == 'm' and skip:
                skip = False
                continue

            if not skip:
                new_log_str += c
        return new_log_str

    def log(self, log_str):
        # Write to stdout
        print(log_str)
        # Write to log file stripping out ANSI color codes since they look
        # wierd in text editors
        self.fp.write(self._strip_ansi_colors(log_str) + '\n')

    def flush(self):
        self.fp.close()

    def fmt(self, log_str, color):
        if color == LogColor.GREEN:
            return Colors.OKGREEN + log_str + Colors.ENDC
        elif color == LogColor.YELLOW:
            return Colors.OKYELLOW + log_str + Colors.ENDC
        elif color == LogColor.RED:
            return Colors.OKRED + log_str + Colors.ENDC
        elif color == LogColor.BLUE:
            return Colors.OKBLUE + log_str + Colors.ENDC
        return log_str


class ThreadLocalLogger(TaskLogger):
    def __init__(self, logfile):
        self.fp = open(logfile, "a")
        self.flush_lock = Lock()
        self.thread_log = []

    def log(self, log_str):
        # Accumulate thread local log
        self.thread_log.append(log_str)
        # Write to log file stripping out ANSI color codes since they look
        # wierd in text editors
        self.fp.write(self._strip_ansi_colors(log_str) + '\n')

    def flush(self):
        with self.flush_lock:
            for log_line in self.thread_log:
                print(log_line)
        self.thread_log = []
        self.fp.close()


class MonoChromeLogger(TaskLogger):
    def __init__(self, logfile):
        TaskLogger.__init__(self, logfile)

    def fmt(self, log_str, color):
        return log_str
