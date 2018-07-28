class HandlerContext(object):
    def __init__(self, fn):
        self.fn = fn
        self.properties = {'__name__': fn.__name__}
        self.ret = None
        self.args = None
        self.sig = None

    def get(self, prop):
        if prop in self.properties:
            return self.properties[prop]
        return None

    def set(self, prop, value):
        self.properties[prop] = value


class Handler(object):
    def __init__(self, name):
        self.name = name

    # Context -> Void
    def run(self, ctx):
        if not isinstance(ctx, Context):
            raise Exception("Handler {} got a invalid context!!".format(name))


class HandlerRegistry(object):
    pre_handlers = []
    post_handlers = []
    init_handlers = []

    @staticmethod
    def register_init_handler(handler):
        HandlerRegistry.init_handlers.append(handler)

    @staticmethod
    def register_pre_handler(handler):
        HandlerRegistry.pre_handlers.append(handler)

    @staticmethod
    def register_post_handler(handler):
        HandlerRegistry.post_handlers.append(handler)
