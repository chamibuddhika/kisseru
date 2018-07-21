import types
import ast
import inspect

from ir import Function


def tabs_to_spaces(string):
    new_str = []
    for c in string:
        if c == '\t':
            new_str.append('    ')  # Make a tab 4 spaces
        else:
            new_str.append(c)
    return ''.join(new_str)


# Read the test case file and returns a [(input, ouput)]
# where input: ir.Function and output: ir.Function
#
# Test case file format is
#
#   ---------------------->
#   test input  1
#   <----------------------
#   test output 1
#   ---------------------->
#   ...
#   ---------------------->
#   test input  N
#   <----------------------
#   test output N
#   ---------------------->
#
# '#' Starts a comment line
#
def get_test_cases(infile):
    with open(infile, "r") as fp:
        lines = fp.readlines()

    # Filter out comments
    lines = list(
        filter(
            lambda ln: False if ln.startswith("#") or ln.isspace() else True,
            lines))

    tests = []

    is_in_input = False
    input_buf = []
    output_buf = []
    for line in lines:
        if line.startswith("--"):
            if is_in_input:
                raise Exception(
                    "{} test input file is in wrong format".format(infile))
            is_in_input = True

            if input_buf and output_buf:
                input_fn_ir = Function()
                input_fn_ir.lines = list(
                    map(lambda ln: tabs_to_spaces(ln), input_buf))

                output_fn_ir = Function()
                output_fn_ir.lines = list(
                    map(lambda ln: tabs_to_spaces(ln), output_buf))

                tests.append((input_fn_ir, output_fn_ir))

            input_buf = []
            output_buf = []

            continue
        elif line.startswith("<-"):
            if not is_in_input:
                raise Exception(
                    "{} test input file is in wrong format".format(infile))
            is_in_input = False
            continue

        if is_in_input:
            input_buf.append(line)
        else:
            output_buf.append(line)
    return tests


def compile_fn(source):
    module = ast.parse(source)
    func = module.body[0]
    module_code = compile(source, '<string>', 'exec')

    func_code = None
    for const in module_code.co_consts:
        if isinstance(const, types.CodeType):
            func_code = const

    new_fn = types.FunctionType(func_code, {}, name="test_fn")
    return new_fn
