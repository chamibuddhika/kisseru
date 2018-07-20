class Context(object):
    def __init__(self, fn):
        self.fn = fn
        self.properties = {'__name__': fn.__name__}
        self.ret = None

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

    @staticmethod
    def register_prehandler(handler):
        HandlerRegistry.pre_handlers.append(handler)

    @staticmethod
    def register_posthandler(handler):
        HandlerRegistry.post_handlers.append(handler)
