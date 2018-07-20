from colors import Colors
from handler import Handler


class LoggerEntry(Handler):
    def __init__(self, name):
        Handler.__init__(self, name)

    def run(self, ctx):
        print(Colors.OKGREEN +
              "[KISERU] INFO - Running {}".format(ctx.get('__name__')) +
              Colors.ENDC)


class LoggerExit(Handler):
    def __init__(self, name):
        Handler.__init__(self, name)

    def run(self, ctx):
        print(Colors.OKGREEN + "[KISERU] INFO - {} output : \n{}".format(
            ctx.get('__name__'), ctx.ret) + Colors.ENDC)
