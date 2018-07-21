import unittest

# append parent directory to import path
import env
import func

from common import get_test_cases

from utils import gen_spaces
from func import normalize_fn_body
from func import set_function_meta
from bash import handle_scripts


class ASTOpsTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.normalize_tests = get_test_cases("ast_ops_normalize.in")
        cls.script_tests = get_test_cases("ast_ops_script.in")

    def test_normalize_fn_body(self):
        for test in self.normalize_tests:
            input_fn_ir = test[0]
            output_fn_ir = test[1]

            normalize_fn_body(input_fn_ir)
            self.assertEqual(input_fn_ir.lines, output_fn_ir.lines)

    def test_handle_scripts(self):
        for test in self.script_tests:
            input_fn_ir = test[0]
            output_fn_ir = test[1]

            set_function_meta(input_fn_ir)

            handle_scripts(input_fn_ir)
            self.assertEqual(''.join(input_fn_ir.lines),
                             ''.join(output_fn_ir.lines))


if __name__ == "__main__":
    unittest.main()  # run all tests
