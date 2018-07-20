class Function(object):
    def __init__(self):
        self.lines = []
        self.scripts = []
        self.line_map = set()
        self.name = None
        self.indent = -1
        self.body_indent = -1
        self.deco_start = -1
        self.deco_end = -1
        self.proto_start = -1
        self.proto_end = -1
        self.body_start = -1
        self.body_end = -1

        self._populate_line_info()

    def is_inlined_bash(self, lineno):
        if lineno in self.line_map:
            return True
        return False

    def _populate_line_info(self):
        for script in self.scripts:
            self.line_map.update(range(script.start, script.end + 1))


class Script(object):
    def __init__(self, lines, start, end, indent):
        self.lines = lines
        self.start = start
        self.end = end
        self.indent = indent
