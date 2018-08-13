from enum import Enum

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

    def log(self, log_str):
        # Write to stdout
        print(log_str)
        # Write to log file
        self.fp.write(log_str)

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


class MonoChromeLogger(TaskLogger):
    def __init__(self, logfile):
        TaskLogger.__init__(self, logfile)

    def fmt(self, log_str, color):
        return log_str
