import subprocess
import re
import logging
import functools

from collections import namedtuple
from itertools import islice

from ir import Script
from utils import *

log = logging.getLogger(__name__)

KISERU_TAG = "<<kiseru>>"
KISERU_END_TAG = "<<kiseru_end>>"


def rewrite_lvalue_assign(match):
    matched_str = match.group(0)
    # Check if this is a previously annotated lvalue assignment
    if matched_str.startswith("%={") and (matched_str.endswith(";")
                                          or matched_str.endswith("\n")):
        # Extract the python variable name from %={varname} = value
        py_var = matched_str[3:matched_str.index("}")].strip()

        # Extract the assigned value from %={varname} = value
        value = matched_str[matched_str.index("}") + 1:]
        value = value.strip()[1:]  # Skips the '=' after removing the padding
        # value = value.strip()  # Remove any padding between '=' and value

        # Echo the tagged variable assignment to stdout. Three things involved.
        # 1. Set the value to a bash variable with the same name
        # 2. Output the value of the bash variable with KISERU_TAG so that we
        #    can later extract it out.
        # 3. Write the KISERU_END_TAG at the end of the output so that we can
        #    correctly extract out multi line outputs
        set_var = '{}={}\n'.format(py_var.strip(), value.strip())
        echo = 'echo "{}{}=${}"\n'.format(KISERU_TAG, py_var, py_var)
        end_output = 'echo "{}"'.format(KISERU_END_TAG)
        return set_var + echo + end_output
    return matched_str


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

    # Following wierd formatting is necessary for the logger to correctly print
    # this string as intended
    info_str = """
---------------------
Expanded bash script:
---------------------
{}
---------------------
""".format(result.script)
    logp_debug(log, info_str)

    p = subprocess.Popen(
        result.script,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=True,
        universal_newlines=True)
    stdout, stderr = p.communicate()

    output = ScriptOutput(stdout, stderr)
    return output


def set_assignments(stdout, env):
    assigns = {}
    lines = stdout.splitlines()

    # Match '[kiseru]varname = value\n' and capture just the varname and value
    extract_assigns_str = '\s*{}([a-zA-Z_]\w*)\s*=\s*(.*)\s*'.format(
        KISERU_TAG)
    extract_assigns_regex = re.compile(extract_assigns_str)

    is_in_output = False
    output = []
    varname = None
    for line in lines:
        if is_in_output:
            if line.startswith(KISERU_END_TAG):
                is_in_output = False
                env[varname] = '\n'.join(output)
                varname = None
                output = []
                continue
            else:
                output.append(line.rstrip())
                continue

        match = re.search(extract_assigns_regex, line)
        if match:
            is_in_output = True
            varname = match.group(1)
            output = [match.group(2).rstrip()]


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


def set_vars(script_lines, regex, variables):
    for lineno, line in enumerate(script_lines):
        matches = re.findall(r'{}'.format(regex), line)
        variables.update(matches)


class StaticAnalysis(object):
    def __init__(self):
        self.deps = set()
        self.resources = set()
        self.system_deps = set()
        self.lvalues = set()
        self.rvalues = set()
        self.vars = set()


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


def process_scripts(fnIR):

    _set_script_env = "__kiseru_assigns = {}\n"

    for i, script in enumerate(fnIR.scripts):
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
        alpha_rename(fnIR, script, rest_of_the_fn, analysis.lvalues)

        # Replace the script header with actual python function calls to run
        # and get the output
        fnIR.lines[script.start] = _run_n_get_output

        # Mark rest of the inlined script for deletion
        start, end = script.start + 1, script.end + 1
        fnIR.lines[start:end] = [None] * (end - start)

    fnIR.lines = [line for line in fnIR.lines if line != None]

    # Set the local enviornment modified by inlined scripts
    fnIR.lines.insert(fnIR.body_start,
                      gen_spaces(fnIR.body_indent) + _set_script_env)


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


def handle_scripts(fn_ir):
    extract_scripts(fn_ir)
    process_scripts(fn_ir)
