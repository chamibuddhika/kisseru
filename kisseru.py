import uuid
import ast
import subprocess
import inspect
import re
import functools
import types

from enum import Enum

from inspect import Signature
from inspect import signature
from collections import namedtuple
from itertools import islice

_CSV = 'CSV'


class Types(Enum):
    _CSV = 1
    ESRI = 2
    GRB = 3
    NETCDF4 = 4


class Stage(object):
    def __init__(self, task, args, kwargs):
        self.task = task
        self.args = args
        self.kwargs = kwargs
        self.uuid = uuid.uuid1()


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


def remove_prefix(text, prefix):
    if text.startswith(prefix):
        return text[len(prefix):]
    return text


def remove_suffix(text, suffix):
    if not text.endswith(suffix):
        return text
    return text[:len(text) - len(suffix)]


def substitute_rvalue(locls, globls, script_env, match):
    matched_str = match.group(0)
    if matched_str.startswith("%{") and matched_str.endswith("}"):
        # Extract the python variable name stripping the delimiters and
        # whitespaces
        py_var = matched_str[2:len(match.group(1)) - 1]
        py_var = py_var.strip()
        py_var_val = None

        # First check script environment
        py_var_val = script_env.get(py_var, None)

        # Then check local environment
        if not py_var_val:
            py_var_val = locls.get(py_var, None)

        # Finally check global scope
        if not py_var_val:
            py_var_val = globls.get(py_var, None)

        if py_var_val:
            return str(py_var_val)
    return matched_str


def annotate_lvalue(match):
    matched_str = match.group(0)
    if matched_str.startswith("%{") and matched_str.endswith("="):
        # Annotate as a lvalue for so that rewrite step recognize it
        lvalue_annotated = matched_str[:1] + "=" + matched_str[1:]
        return lvalue_annotated
    return matched_str


KISERU_TAG = "<<kiseru>>"


def rewrite_lvalue_assign(match):
    matched_str = match.group(0)
    if matched_str.startswith("%={") and (matched_str.endswith(";")
                                          or matched_str.endswith("\n")):
        # Extract the python variable name from %={varname} = value
        py_var = matched_str[3:matched_str.index("}")].strip()

        # Extract the assigned value from %={varname} = value
        value = matched_str[matched_str.index("}") + 1:]
        value = value.strip()[1:]  # Skips the '=' after removing the padding
        # value = value.strip()  # Remove any padding between '=' and value

        # Echo the tagged variable assignment to stdout
        echo_str = '{}={}\necho "{}{}=\'${}\n\'"\n'.format(
            py_var, value, KISERU_TAG, py_var, py_var)
        return echo_str
    return matched_str


def modify_script(script_lines, regex, subst):
    for lineno, line in enumerate(script_lines):
        script_lines[lineno] = re.sub(r'{}'.format(regex), subst, line)


def interpolate(script_str, locls, globls, script_env):
    lines = script_str.splitlines()
    lines = list(map(lambda x: x + '\n', lines))  # Reintroduce the newlines

    # varname is a python variable
    # Match '%{varname} ='
    lvalue_regex = '(%{\s*[a-zA-Z_]\w*\s*})\s*='
    # Match '%{varname} = value\n' or '%{varname} = value;'
    lvalue_assign_regex = '(%={\s*[a-zA-Z_]\w*\s*}\s*=.*[\n,;])'
    # Match '%{varname}'
    var_regex = '(%{\s*[a-zA-Z_]\w*\s*})'

    # Substitute python variables appearing as lvalues and rvlaues
    # Order of method invocation is important. We first annotate lvalues.
    # Then we run variable substitution which skips any annotated lvalues and
    # only operating on rvalues. Finally we rewrite the lvalue assign.
    modify_script(lines, lvalue_regex, annotate_lvalue)
    modify_script(
        lines, var_regex,
        functools.partial(substitute_rvalue, locls, globls, script_env))
    modify_script(lines, lvalue_assign_regex, rewrite_lvalue_assign)

    InterpolationResult = namedtuple('InterpolationResult',
                                     'script lvalues rvalues')
    result = InterpolationResult(''.join(lines), None, None)
    return result


class ScriptOutput(object):
    def __init__(self, stdout, stderr):
        self.stdout = stdout
        self.stderr = stderr


def run_script(script_str, locls, globls, script_env):
    result = interpolate(script_str, locls, globls, script_env)
    print(result.script)
    p = subprocess.Popen(
        result.script,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=True,
        universal_newlines=True)
    stdout, stderr = p.communicate()

    output = ScriptOutput(stdout, stderr)
    return output


def sanitize(script):
    lines = script.lines

    # Remove the '''bash delimiter of the inlined script
    stripped = remove_prefix(lines[0].strip(), '\'\'\'bash')
    if not stripped:
        del (lines[0])
    else:
        lines[0] = stripped + '\n'

    # Remove the ''' delimiter of the inlined script
    lines[len(lines) - 1] = \
            remove_suffix(lines[len(lines) - 1].strip(), '\'\'\'') + '\n'

    # Now strip all lines of extra whitespaces while preserving
    # line breaks
    for lineno, line in enumerate(lines):
        if line[-1] == '\n':
            stripped = line.strip() + '\n'
        else:
            stripped = line.strip()
        lines[lineno] = stripped

    # Remove the last line if it is empty
    last = lines[len(lines) - 1]
    if not last:
        del (lines[len(lines) - 1])

    # print(''.join(script.lines))


def get_indentation(line):
    whitespace_regex = re.compile('\s+')
    match = whitespace_regex.search(line)
    if match:
        return len(match.group(0))
    return 0


def extract_scripts(fn):
    scripts = []
    cur_script = []
    cur_script_start = 0
    script_indent = 0
    in_script = False
    for i, line in enumerate(fn.lines):
        stripped = line.strip()

        if stripped.startswith('\'\'\'bash'):
            if in_script:
                raise Exception("Invalid inlined bash script")
            in_script = True
            cur_script_start = i
            script_indent = get_indentation(line)
        elif stripped.startswith('\'\'\''):
            in_script = False
            scripts.append(
                Script(fn.lines[cur_script_start:i + 1], cur_script_start, i,
                       script_indent))
            cur_script.append(line)

        if stripped.endswith('\'\'\''):
            in_script = False
            if i != cur_script_start:
                cur_script.append(line)
            scripts.append(
                Script(fn.lines[cur_script_start:i + 1], cur_script_start, i,
                       script_indent))
    fn.scripts = scripts


class StaticAnalysis(object):
    def __init__(self):
        self.deps = set()
        self.resources = set()
        self.system_deps = set()
        self.lvalues = set()
        self.rvalues = set()
        self.vars = set()


def set_vars(script_lines, regex, variables):
    for lineno, line in enumerate(script_lines):
        matches = re.findall(r'{}'.format(regex), line)
        variables.update(matches)


def static_analyze_script(lines):

    analysis = StaticAnalysis()

    # Analyze used depedencies, resources
    # TODO...

    # Run syntax check
    # TODO...

    # Extract rvalues and lvalues
    # Match '%{varname} =' and capture just the 'varname'
    lvalue_regex = '%{\s*([a-zA-Z_]\w*)\s*}\s*='
    # Match '%{varname}' and capture just the 'varname'
    var_regex = '%{\s*([a-zA-Z_]\w*)\s*}'

    # Set variable references
    set_vars(lines, var_regex, analysis.vars)
    # Set lvalues
    set_vars(lines, lvalue_regex, analysis.lvalues)
    # Set rvalues
    analysis.rvalues = analysis.vars - analysis.lvalues

    return analysis


def set_assignments(stdout, env):
    assigns = {}
    lines = stdout.splitlines()

    # Match '[kiseru]var = value\n' and capture just the varname and value
    extract_assigns_str = '\s*{}([a-zA-Z_]\w*)\s*=\s*\'(.*)\s*'.format(
        KISERU_TAG)
    extract_assigns_regex = re.compile(extract_assigns_str)

    for line in lines:
        match = re.search(extract_assigns_regex, line)
        if match:
            varname = match.group(1)
            value = match.group(2)
            env[varname] = value


def do_alpha_rename(match):
    prefix = match.group(1)
    variable = match.group(2)
    suffix = match.group(3)
    return '{}__kiseru_assigns[\'{}\']{}'.format(prefix, variable, suffix)


# Alpha rename the python variables modified within inlined bash scripts
#
# [TODO]
# This captures a reasonable set of places where a variable reference can occur
# But it might also capture non variable references. For example
# the same in scope variable name may appear as a parameter of a lambda without
# causing any collision. So this method is not infallible. The problem is that
# currently we are doing a source to source transformation for both bash script
# interpolation and dynamic python code injection. We need to move the dynamic
# python code injection part to be operating on the ast that we can obtain from
# invoking python ast module methods on the function
def alpha_rename(fn, script, start_from, variables):

    # op delimiters can occur either left or right to the variable reference
    op_delimiters = ['+', '-', '/', '&', '^', '|', '*', '[', '>', '<', '%']
    lr_delimiters = ['(', ':', '\,',
                     '=']  # Variable can occur left or right to these
    r_only_delimiters = ['.', ')']  # Variable can only occur left to these
    l_only_delimiters = ['~']  # Variable can only occur right to these

    r_delimiters = ['\s+'] + op_delimiters + lr_delimiters + r_only_delimiters
    l_delimiters = ['\s+'] + op_delimiters + lr_delimiters + l_only_delimiters

    regex_tokens = '([' + ','.join(l_delimiters) + '])' + '({})' + '([' \
            + ','.join(r_delimiters) + '])'
    regex_base = ''.join(regex_tokens)

    var_match_regexes = []
    # Generate regexes to match each variable assigned within the inlined
    # script
    for variable in variables:
        var_match_regexes.append(regex_base.format(variable))

    for lineno, line in islice(enumerate(fn.lines), start_from, None):
        # If this line belongs to an inlined bash script we skip
        if fn.is_inlined_bash(lineno):
            continue

        # Save the original line indentation.
        indent = get_indentation(line)

        # Pad with whitespaces. This allows us to match any variables appearing
        # at the start or at the end of a line by our regex
        line = ' ' + line.strip() + '\n'

        # Add back the original indentation
        line = gen_spaces(indent) + line.lstrip()

        # Now match and alpha-rename any variable appearing on this line
        for regex in var_match_regexes:
            fn.lines[lineno] = re.sub(regex, do_alpha_rename, line)


def gen_spaces(n_spaces):
    spaces = ''
    return ''.join(map(lambda _: ' ', range(n_spaces)))


def process_scripts(fn):

    _set_script_env = "__kiseru_assigns = {}\n"

    for i, script in enumerate(fn.scripts):
        sanitize(script)
        if len(script.lines) == 0:
            continue

        analysis = static_analyze_script(script.lines)

        script_str = ''.join(script.lines)

        # run_script(script_str, {"param1": ".", "param2": ".."}, {}, {})
        '''
        with open("script_{}.sh".format(i), "w") as script_file:
            script_file.write(script_str)
        '''

        # Replace the inlined bash script with a runtime call to
        # run_script
        _run_script = gen_spaces(script.indent) + "__kiseru_output = " \
            + 'run_script("""{}""", locals(), globals(), __kiseru_assigns)\n'\
            .format(script_str)

        _set_assignments = gen_spaces(script.indent) \
                + "set_assignments(__kiseru_output.stdout, __kiseru_assigns)\n"

        _run_n_get_output = _run_script + _set_assignments

        # Rest of the function body after this inlined script
        rest_of_the_fn = script.end + 1

        # Now alpha-rename the python variables assigned inside our script and
        # occurring in the rest of the function body following the script, to be
        # a lookup of the dictionary (__kiseru_assigns) returned by
        # 'extract_assignments'. This hack is due to the fact that python
        # does not allow us to directly modify the local function environment by
        # using locals()
        alpha_rename(fn, script, rest_of_the_fn, analysis.lvalues)

        # Replace the script header with actual python function calls to run
        # and get the output
        fn.lines[script.start] = _run_n_get_output

        # Mark rest of the inlined script for deletion
        start, end = script.start + 1, script.end + 1
        fn.lines[start:end] = [None] * (end - start)

    fn.lines = [line for line in fn.lines if line != None]

    # Set the local enviornment modified by inlined scripts
    fn.lines.insert(fn.body_start,
                    gen_spaces(fn.body_indent) + _set_script_env)


def normalize_fn_body(fn):
    # Match the tail end of prototype line. Prototype is of the form
    # 'def fn(..) [-> type]: <body | \n>' which may be
    # spread across multiple lines with '\' continuation
    prototype_regex = re.compile("(.*\).*:)(.*)", re.DOTALL)
    proto_end = -1
    first_line_of_body = []

    is_in_proto = False
    for lineno, line in enumerate(fn.lines):
        # This is the start of the function prototype
        if line != None and line.lstrip().startswith("def "):
            is_in_proto = True

        # We set 'is_in_proto' in above conditional and do the prototype
        # termination logic given below separately since the prototype may span
        # multiple lines. So it may be several lines later than when we found
        # the 'def ' in the conditional above
        if is_in_proto:
            match = re.search(prototype_regex, line)
            if match:
                proto_end = lineno
                prototype = match.group(1)
                maybe_body = match.group(2)

                if maybe_body.isspace():
                    # Just a plain old function with a new line after ':'. We
                    # are good. Nothing to normalize.
                    return

                # We have function body tacked on to the end of function
                # prototype!!! Separate it and make it a line of its own
                if maybe_body.strip().endswith('\\'):
                    # Replace the current prototype
                    fn.lines[lineno] = prototype + '\n'

                    # Follow until the end of the line and add the accumulated
                    # buffer as the first line of the body
                    first_line_of_body = [maybe_body]
                else:
                    # Replace the current prototype
                    fn.lines[lineno] = prototype + '\n'
                    # Add the newly separated body
                    fn_indent = get_indentation(fn.lines[0])
                    fn.lines.insert(lineno + 1,
                                    gen_spaces(fn_indent + 4) + maybe_body)
                    return
            else:
                # Prototype is split in to multiple lines after ':'
                # Whatever after ':' belongs to the first line of the body.
                # Keep accumulating the first line of the body
                first_line_of_body.append(line)
                # Mark the line for deletion
                fn.lines[lineno] = None

                if not line.strip().endswith('\\'):
                    # We have arrived at the end of the first and last line
                    # of the function. Add the newly separated body after the
                    # prototype
                    fn_indent = get_indentation(fn.lines[0])
                    fn.lines.insert(
                        proto_end + 1,
                        gen_spaces(fn_indent + 4) +
                        ''.join(first_line_of_body))
                    is_in_proto = False

    fn.lines = [line for line in fn.lines if line != None]


def set_function_meta(fn):
    is_in_proto = False
    fn.indent = get_indentation(fn.lines[0])
    for lineno, line in enumerate(fn.lines):
        if line.lstrip().startswith("def "):
            if not is_in_proto:
                is_in_proto = True
            else:
                raise Exception(
                    """Invalid function definition. Multiple def key words in the
                    prototype for function {}""".format(fn.name))
            fn.proto_start = lineno

        if is_in_proto:
            if line.rstrip().endswith("\\"):
                continue
            elif line.rstrip().endswith(":"):
                fn.proto_end = lineno
                fn.body_start = lineno + 1
                is_in_proto = False
                fn.body_indent = get_indentation(fn.lines[fn.body_start])
                return
    if fn.proto_start == -1 or fn.proto_end == -1:
        raise Exception("Invalid function prototype for function {}".format(
            fn.name))


def recompile(fn, old_func):
    source = ''.join(fn.lines)

    print(source)
    print("\n")

    module = ast.parse(source)
    func = module.body[0]
    module_code = compile(source, '<string>', 'exec')

    func_code = None
    for const in module_code.co_consts:
        if isinstance(const, types.CodeType):
            func_code = const

    new_fn = types.FunctionType(
        func_code, old_func.__globals__.copy(), name=fn.name)
    return new_fn


def process_fn(func):
    fn = Function()
    fn.name = func.__name__
    fn.lines = inspect.getsourcelines(func)[0]
    del fn.lines[0]

    if len(fn.lines) <= 0:
        raise Exception("Empty function {}".format(fn.name))

    normalize_fn_body(fn)

    set_function_meta(fn)
    extract_scripts(fn)
    process_scripts(fn)
    return recompile(fn, func)


params = {'split': None}


def task(**params):
    def decorator(func):
        sig = signature(func)
        # new_sig = sig.replace(return_annotation=Signature.empty)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Stage(func.__name__, args, kwargs)
            fn = process_fn(func)
            print("Invoking the modified function...")
            ret = fn(*args, **kwargs)
            return ret

        # wrapper.__signature__ = new_sig
        print("Return wrapper...")
        return wrapper

    return decorator


@task(split='dfd')
def add(a: int, b: int, c: int) -> _CSV:
    b = a + c \
            + a
    '''bash ls -al > %{b}.txt'''
    '''bash %{d} =`cat %{b}.txt`'''
    return d


if __name__ == "__main__":
    print(add(1, 2, 3))
