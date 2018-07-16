import uuid
import ast
import inspect
import re
import functools

from enum import Enum

from inspect import Signature
from inspect import signature
from functools import wraps

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


class Script(object):
    def __init__(self, lines, linenos):
        self.lines = lines
        self.linenos = linenos


def remove_prefix(text, prefix):
    if text.startswith(prefix):
        return text[len(prefix):]
    return text


def remove_suffix(text, suffix):
    if not text.endswith(suffix):
        return text
    return text[:len(text) - len(suffix)]


def substitute_rvalue(locls, globls, match):
    matched_str = match.group(0)
    if matched_str.startswith("%{") and matched_str.endswith("}"):
        # Extract the python variable name stripping the delimiters and
        # whitespaces
        py_var = matched_str[2:len(match.group(1)) - 1]
        py_var = py_var.strip()

        # First check local scope
        py_var_val = locls[py_var]

        # Check global scope
        if not py_var_val:
            py_var_val = globls[py_var]

        if py_var_val:
            return py_var_val
    return matched_str


def annotate_lvalue(match):
    matched_str = match.group(0)
    if matched_str.startswith("%{") and matched_str.endswith("="):
        # Annotate as a lvalue for so that rewrite step recognize it
        lvalue_annotated = matched_str[:1] + "=" + matched_str[1:]
        return lvalue_annotated
    return matched_str


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
        echo_str = 'echo -e "[kiseru]{}={}"\n'.format(py_var, value)
        return echo_str
    return matched_str


def modify_script(script_lines, regex, subst):
    for lineno, line in enumerate(script_lines):
        script_lines[lineno] = re.sub(r'{}'.format(regex), subst, line)


def interpolate(script_str, locls, globls):
    lines = script_str.splitlines()
    lines = list(map(lambda x: x + '\n', lines))  # Reintroduce the newlines

    # varname is a python variable
    # Match '%{varname} ='
    lvalue_regex = '(%{\s*[a-zA-Z_]\w*\s*})\s*='
    # Match '%{varname} = value\n' or '%{varname} = value;'
    lvalue_assign_regex = '(%={\s*[a-zA-Z_]\w*\s*}\s*=.*[\n,;])'
    # Match '%{varname}'
    rvalue_regex = '(%{\s*[a-zA-Z_]\w*\s*})'

    # Substitute python variables appearing as lvalues and rvlaues
    # Order of method invocation is important. We first annotate lvalues.
    # Then we run rvalue substitution which skips any annotated lvalues and
    # only operating on rvalues. Finally we rewrite the lvalue assign.
    modify_script(lines, lvalue_regex, annotate_lvalue)
    modify_script(lines, rvalue_regex,
                  functools.partial(substitute_rvalue, locls, globls))
    modify_script(lines, lvalue_assign_regex, rewrite_lvalue_assign)
    return ''.join(lines)


def run_script(script_str, locls, globls):
    script_str = interpolate(script_str, locls, globls)


def sanitize(script):
    lines = script.lines

    # Remove the '''bash delimiter of the inlined script
    stripped = remove_prefix(lines[0].strip(), '\'\'\'bash')
    if stripped:
        # Add new line if the command spans over to a new line
        if stripped.endswith('\\'):
            stripped += '\n'
        lines[0] = stripped
    else:
        del (lines[0])

    # Remove the ''' delimiter of the inlined script
    stripped = remove_suffix(lines[len(lines) - 1].strip(), '\'\'\'')

    if stripped:
        stripped = stripped.strip()
        stripped += '\n'

        lines[len(lines) - 1] = stripped
    else:
        del (lines[len(lines) - 1])

    # Now strip all other lines of extra whitespaces while preserving
    # line breaks
    for lineno, line in enumerate(lines):
        if line[-1] == '\n':
            stripped = line.strip() + '\n'
        else:
            stripped = line.strip()
        lines[lineno] = stripped

    # print(''.join(script.lines))


params = {'split': None}


def task(**params):
    def decorator(func):
        sig = signature(func)
        new_sig = sig.replace(return_annotation=Signature.empty)

        @wraps(func)
        def wrapper(*args, **kwargs):
            # Stage(func.__name__, args, kwargs)
            lines = inspect.getsourcelines(func)[0]
            # print(lines)
            scripts = []
            cur_script = []
            cur_script_start = 0
            in_script = False
            for i, line in enumerate(lines):
                stripped = line.strip()
                if stripped.startswith('\'\'\'bash'):
                    if in_script:
                        raise Exception("Invalid inlined bash script")
                    in_script = True
                    cur_script_start = i
                elif stripped.startswith('\'\'\''):
                    in_script = False
                    scripts.append(
                        Script(lines[cur_script_start:i + 1],
                               (cur_script_start, i)))
                    cur_script.append(line)

                if stripped.endswith('\'\'\''):
                    in_script = False
                    if i != cur_script_start:
                        cur_script.append(line)
                    scripts.append(
                        Script(lines[cur_script_start:i + 1],
                               (cur_script_start, i)))

            for script in scripts:
                print(script.linenos)
                sanitize(script)
                if len(script.lines) == 0:
                    continue
                script_str = ''.join(script.lines)

                # Replace the inlined bash script with a runtime call to
                # run_script
                run_script_str = \
                    'run_script(\'{}\', locals(), globals())'.format(script_str)
                lines[script.linenos[0]] = run_script_str

                start, end = script.linenos
                start += 1
                end += 1
                lines[start:end] = [None] * (end - start)
                print(lines)

            lines = [line for line in lines if line != None]
            print(lines)

            ret = func(*args, **kwargs)
            return ret

        wrapper.__signature__ = new_sig
        return wrapper

    return decorator


@task(split='dfd')
def add(a: int, b: int, c: int) -> _CSV:
    b = a + c \
            + a
    '''bash ./ls -al %{sfdb} %{sdf} \
            > ls.out
       %{bar} = sdfs'''
    '''bash grep -rl x
        %{sdf} =df'''
    return a + b + c


if __name__ == "__main__":
    print(add(1, 2, 3))
