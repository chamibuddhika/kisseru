from colors import Colors
from handler import Handler


class LoggerEntry(Handler):
    def __init__(self, name):
        Handler.__init__(self, name)

    def run(self, ctx):
        print(Colors.OKGREEN +
              "[KISSERU] Running {}".format(ctx.get('__name__')) + Colors.ENDC)
        print(Colors.OKGREEN +
              "[KISSERU] {} inputs : ".format(ctx.get('__name__')) +
              Colors.ENDC + Colors.OKBLUE + Colors.BOLD +
              "{}".format(ctx.args) + Colors.ENDC)
        print(Colors.OKGREEN + "            .             " + Colors.ENDC)
        print(Colors.OKGREEN + "            .             " + Colors.ENDC)


class LoggerExit(Handler):
    def __init__(self, name):
        Handler.__init__(self, name)

    def run(self, ctx):
        print(Colors.OKGREEN +
              "[KISSERU] {} output : ".format(ctx.get('__name__')) +
              Colors.ENDC + Colors.OKBLUE + Colors.BOLD +
              "{}".format(ctx.ret) + Colors.ENDC)
        print("========================================")
        print("                  \/                    \n")
