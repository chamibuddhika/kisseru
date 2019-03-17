from colors import Colors
from handler import Handler
from logger import LogColor
from backend import Backend


class TraceEntry(Handler):
    def __init__(self, name):
        Handler.__init__(self, name)

    def run(self, ctx):
        logger = Backend.get_current_backend().logger

        log_str = logger.fmt("[Runner] Running {}".format(ctx.get('__name__')),
                             LogColor.GREEN)
        logger.log(log_str)

        log_str = logger.fmt(
            "[Runner] {} inputs : ".format(ctx.get('__name__')),
            LogColor.GREEN)
        log_str += logger.fmt("{}".format(ctx.args), LogColor.BLUE)
        logger.log(log_str)

        log_str = logger.fmt("            .             ", LogColor.GREEN)
        logger.log(log_str)
        logger.log(log_str)
        '''
        print(Colors.OKGREEN +
              "[Runner] Running {}".format(ctx.get('__name__')) + Colors.ENDC)
        print(Colors.OKGREEN +
              "[Runner] {} inputs : ".format(ctx.get('__name__')) +
              Colors.ENDC + Colors.OKBLUE + Colors.BOLD +
              "{}".format(ctx.args) + Colors.ENDC)
        print(Colors.OKGREEN + "            .             " + Colors.ENDC)
        print(Colors.OKGREEN + "            .             " + Colors.ENDC)
        '''


class TraceExit(Handler):
    def __init__(self, name):
        Handler.__init__(self, name)

    def run(self, ctx):
        logger = Backend.get_current_backend().logger

        log_str = logger.fmt(
            "[Runner] {} output : ".format(ctx.get('__name__')),
            LogColor.GREEN)
        log_str += logger.fmt("{}".format(ctx.ret), LogColor.BLUE)
        logger.log(log_str)
        logger.log("========================================")
        logger.log("                  \/                    \n")
        '''
        print(Colors.OKGREEN +
              "[Runner] {} output : ".format(ctx.get('__name__')) +
              Colors.ENDC + Colors.OKBLUE + Colors.BOLD +
              "{}".format(ctx.ret) + Colors.ENDC)
        print("========================================")
        print("                  \/                    \n")
        '''
