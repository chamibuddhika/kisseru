from colors import Colors
from handler import Handler


class LoggerEntry(Handler):
    def __init__(self, name):
        Handler.__init__(self, name)

    def run(self, ctx):
        print(Colors.OKGREEN +
              "[Runner] Running {}".format(ctx.get('__name__')) + Colors.ENDC)
        print(Colors.OKGREEN +
              "[Runner] {} inputs : ".format(ctx.get('__name__')) +
              Colors.ENDC + Colors.OKBLUE + Colors.BOLD +
              "{}".format(ctx.args) + Colors.ENDC)
        print(Colors.OKGREEN + "            .             " + Colors.ENDC)
        print(Colors.OKGREEN + "            .             " + Colors.ENDC)


class LoggerExit(Handler):
    def __init__(self, name):
        Handler.__init__(self, name)

    def run(self, ctx):
        print(Colors.OKGREEN +
              "[Runner] {} output : ".format(ctx.get('__name__')) +
              Colors.ENDC + Colors.OKBLUE + Colors.BOLD +
              "{}".format(ctx.ret) + Colors.ENDC)
        print("========================================")
        print("                  \/                    \n")
