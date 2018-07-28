from enum import Enum
from passes import Pass
from passes import PassResult


class TypeRegistry(object):
    types = {}

    @staticmethod
    def register_type(type_obj):
        TypeRegistry.types[type_obj.tid] = type_obj

    @staticmethod
    def get_type(type_id):
        return TypeRegistry.types.get(type_id, None)


class MetaType(Enum):
    BUILT_IN = 0
    FILE = 1
    USER_DEF = 2
    NONE = 3


class Type(object):
    def __init__(self, type_id, typ):
        self.id = type_id
        self.type = typ

    def __repr__(self):
        return "{}".format(self.id)

    def __str__(self):
        return "{}".format(self.id)


class BuiltinType(Type):
    def __init__(self, type_id, typ):
        Type.__init__(self, type_id, typ)
        self.meta = MetaType.BUILT_IN


class FileType(Type):
    def __init__(self, type_id, extension, typ=str, domain=None):
        Type.__init__(self, type_id, typ)
        self.meta = MetaType.FILE
        self.ext = extension
        self.domain = domain


def UserDefinedType(Type):
    def __init__(self, type_id, typ):
        Type.__init__(self, type_id, typ)
        self.meta = MetaType.USER_DEF


def get_type(typ):
    if typ == int:
        return BuiltinType('int', typ)
    elif typ == str:
        return BuiltinType('str', typ)
    elif typ == bool:
        return BuiltinType('bool', typ)
    elif typ == dict:
        return BuiltinType('dict', typ)
    elif typ == float:
        return BuiltinType('float', typ)
    elif typ == 'csv':
        return FileType('csv', None)
    elif typ == 'xls':
        return FileType('xls', None)
    elif typ == 'png':
        return FileType('png', None)
    elif type == 'any':
        return BuiltinType('void', typ)
    elif type == 'anyfile':
        return FileType('unknown', typ)


def is_castable(type1, type2):

    cast_map = {
        'int': ['float', 'any'],
        'csv': ['xls', 'anyfile'],
        'xls': ['csv', 'anyfile']
    }

    castables = cast_map.get(type1.id, None)
    can_cast = False
    if castables:
        for castable in castables:
            if type2.id == castable:
                can_cast = True
                break
    return can_cast


class TypeCheck(Pass):
    def __init__(self, name):
        Pass.__init__(self, name)
        self.description = "Running the type checker"

    # [TODO] Type checker should be able to catch argument mismatch errors.
    # We should also be able to do some level of type inferencing at this
    # level. This transformation is also important for implicit data type
    # transformation pass to work.
    def run(self, graph, ctx):
        Pass.run(self, graph, ctx)
        type_errors_found = False
        for tid, task in graph.tasks.items():
            for edge in task.edges:
                intype = edge.source.type
                outtype = edge.dest.type
                if is_castable(intype, outtype):
                    # Handle file related types separately
                    if isinstance(intype, FileType) and isinstance(
                            outtype, FileType):
                        # Check if one end of the edge is untype but the other
                        # is not. If that's the case then we cast the untyped
                        # end to be the type of the other end. Otherwise we
                        # mark it as needing a data transformation
                        if intype.id == 'anyfile' and outtype.id != 'anyfile':
                            intype.id = outtype.id
                        elif intype.id != 'anyfile' and outtype.id == 'anyfile':
                            outtype.id = intype.id
                        else:
                            edge.needs_transform = True
                else:
                    ctx.errors.append(
                        "{} expected a {} got a {} from {}".format(
                            edge.dest.task_ref.name, intype.id, outtype.id,
                            edge.source.task_ref.name))
        if type_errors_found:
            return PassResult.ERROR

    def post_run(self, graph, ctx):
        pass
