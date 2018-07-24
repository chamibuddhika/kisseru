from enum import Enum


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
    def __init__(self, type_id):
        self.id = None
        self.type = None


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


def UserDefineType(Type):
    def __init__(self, type_id, typ):
        Type.__init__(self, type_id, typ)
        self.meta = MetaType.USER_DEF


def get_type(typ):
    pass


def is_coerceable(val, typ):
    pass
