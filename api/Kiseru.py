
from enum import Enum
from utils import parmap

try:
   import cPickle as pickle
except:
   import pickle

import abc
 
class RunnerType(Enum):
    SMP     = 1
    CLUSTER = 2
    SPARK   = 3

class Runner(metaclass=abc.ABCMeta):

    def __init__(self, runner_type = RunnerType.SMP):
        self.type = runner_type
 
class Backend(metaclass=abc.ABCMeta):

    def __init__(self):
        self.code = []
        self.descriptor = {}
        self.passes = [self.gen, self.gen_descriptor, self.flush]

    @abc.abstractmethod
    def gen(self, ir): 
        """ Generates the code to run at the backend runtime.

        Args:
            ir: IR representation corresponding to the pipeline

        Returns:
            The potentially modified ir representation.

        Raises:
            Exception: Invalid IR
        """
        return

    @abc.abstractmethod
    def flush(self, ir):
        """ Prepares and writes out the deployment artifact.

        Args:
            ir: IR representation corresponding to the pipeline

        Returns:
            The potentially modified ir representation.

        Raises:
            Exception: Invalid IR
        """
        return

    def gen_descriptor(self, ir):
        """ Generates a deployment descriptor for the pipeline.

        Args:
            ir: IR representation corresponding to the pipeline

        Returns:
            The potentially modified ir representation.

        Raises:
            Exception: Invalid IR
        """
        return

    def run_passes(self, ir):
        """ Runs registere code generation related passes one by one..

        Args:
            ir: IR representation corresponding to the pipeline

        Returns:
            The potentially modified ir representation.

        Raises:
            Exception: Invalid IR
        """
        for p in self.passes:
            ir = p(ir)
        return ir

class SMPBackend(Backend):

    # default indentation size
    INDENT = 4

    def __init__(self):
        Backend.__init__(self)
        self._cur_level = 0

    def gen_descriptor(self, ir):
        return ir

    def flush(self, ir):
        print("Running flush")
        fp = open('run.py', 'w')
        for line in self.code:
          fp.write("%s\n" % line)
        fp.close()
        return ir

    def gen(self, ir):
        if isinstance(ir, Pipeline):
            if not ir.is_nested:
                print("Running codegen")
                # pickle the pipeline
                self._gen_runnable(ir)

                # now to generate the code to run the pickled pipeline
                # ...

                # print import headers using obj.classes
                self._gen_imports(ir.classes)
        
                # generate main function header 
                self._gen_main_header()

                # unpickle and run the pipeline 
                self._gen_main_body()
            else:
                raise Exception("Invalid Pipeline definition")
        else:
            raise Exception("Invalid Pipeline definition >")
        return ir

    def _gen_imports(self, classes):
        # system imports
        self._add("try:")
        self._indent_plus()
        self._add("import cPickle as pickle")
        self._indent_minus()

        self._add("except:")
        self._indent_plus()
        self._add("import pickle")
        self._indent_minus()

        self._addln("import sys")

        # API imports
        self._add("from Kiseru import Operator")
        self._add("from Kiseru import Pipeline")
        self._add("from Kiseru import Conditional")
        self._addln("from Kiseru import RunnerType")

        for cls in classes:
            self._add("from Kiseru import {0}".format(cls))

        self._addln("")

    def _gen_main_header(self):
        self._add("if __name__ == '__main__':")

    def _gen_main_body(self):
        self._indent_plus()
        self._add("fp = open('test.dat', 'rb')")
        self._add("p = pickle.load(fp)")
        self._add("fp.close()")
        self._add("p.run()")
        self._indent_minus()

    def _gen_runnable(self, pipeline):
        fp = open("test.dat", 'wb')
        pickle.dump(pipeline, fp)
        fp.close()

    def _add(self, line):
        self.code.append(self._indent_line(line, self._cur_level * self.INDENT))

    def _addln(self, line):
        self._add(line)
        self._add("")

    def _indent_plus(self):
        self._cur_level += 1

    def _indent_minus(self):
        self._cur_level -= 1

    def _indent_line(self, line, nspaces):
        indent = " " * nspaces 
        return indent + line

class Operator(metaclass=abc.ABCMeta):
  
    def __init__(self):
        self.is_pipeline  = False
        self.is_parallel  = False
        self.is_iterative = False
        self.until  = None

    def _delegate(self, other):
        if type(self) != Pipeline:
            # Bit of a hack by breaking the abstraction down the inheritance
            # hierarchy. But makes pipeline definition simpler by making it
            # possible to construct a new pipeline without explicitly instantiating
            # Pipeline object at the beginning
            return Pipeline().__or__(self).__or__(other) 
        else:
            return self.__or__(other)

    def __or__(self, other):
        return self._delegate(other)

    def __floordiv__(self, other):
        return self._delegate(other)

    @abc.abstractmethod
    def run(self, inputs):
        return

    def _run_wrapper(self, inputs):
        return inputs

    def codegen(self, backend):
        backend.gen(self)

class FusedOperator(Operator):
    
    def __init__(self, operators):
        Operator.__init__(self)
        self.operators = operators

class ParallelOp(Operator):

    def __init__(self, operator, parallelism):
        Operator.__init__(self)
        self.is_parallel = True
        self.operator    = operator
        self.parallelism = parallelism

    def run(self, inputs = None):
        # If the parallelism is not given, it is set to the number of processors by 
        # parmap_dict
        res = inputs
        if parallelism == -1:
            parmap_dict(self.operator.run, res, "__data__")
        else:
            parmap_dict(self.operator.run, res, "__data__", parallelism)

class IterativeOp(Operator):

    def __init__(self, iters, until):
        Operator.__init__(self)
        self.is_iterative = True
        self.iters = iters
        self.until = until

        for it in iters:
            if isinstance(it, Pipeline):
                operator.is_pipeline = True
                operator.is_nested   = True

    def run(self, inputs = None):
        res = inputs
        for it in iters:
            res = it.run(res)
            if until.check(res) == False:
                break
        return res

class Conditional(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def check(res):
        return

class Pipeline(Operator):

    def __init__(self):
        Operator.__init__(self)
        self.operators = []
        self.runner    = None
        self.classes   = set() 
        self.is_nested = False

    def __or__(self, other):
        return self.do(other) 

    def __floordiv__(self, other):
        return self.do_par(self,other)

    def do(self, operator):
        if not isinstance(operator, Operator):
            raise Exception("Invalid operator")

        if isinstance(operator, Pipeline):
            operator.is_pipeline = True
        self.operators.append(operator)
        self.classes.add(operator.__class__.__name__)
        return self

    def do_par(self, operator, parallelism = -1):
        if isinstance(operator, Pipeline):
            operator.is_pipeline = True
        operator.is_nested   = True
        self.operators.append(ParallelOp(operator, parallelism))
        self.classes.add(operator.__class__.__name__)
        return self

    def do_iter(self, iters, until):
        # Check if we got a list of operators
        if isinstance(iters, list) and not isinstance(iters, str): 
            is_valid = reduce(
                (lambda op, res: 
                (res and True) if isinstance(op, Operator) else False), iters) 
            if not is_valid:
                raise Exceptions("Not a valid operator pipeline")

        # Check if we got a valid conditional
        if not isinstance(until, Conditional):
            raise Exception("Invalid conditional provided")

        self.operators.append(IterativeOp(iters, until))
        return self

    def run(self, inputs = None):
        res = inputs
        for operator in self.operators:
            res = operator.run(res)

    def submit(self):
        # default to LOCAL runner
        if self.runner == None:
            self.runner = RunnerType.SMP
            backend = SMPBackend()
            backend.run_passes(self)

    def set_runner(self, runner):
        self.runner = runner 

    def _get_classes(self, operator):
        classes = set() 
        if isinstance(operator, Pipeline):
            for op in operator.operators:
                classes.union(self._get_classes(op))
        elif isinstance(operator, ParallelOp):
            classes.union(self._get_classes(operator.operator))
        elif isinstance(operator, IterativeOp):
            for it in operator.iters:
                classes.union(self._get_classes(it))
            classes.union(operator.until.__class__.__name__)
        else:
            classes.union(operator.__class__.__name__)
        return classes

class FooOperator(Operator):

    def run(self, obj):
        print("Running Foo")
        return obj

class BarOperator(Operator):

    def run(self, obj):
        print("Running Bar")
        return obj

if __name__ == "__main__":
    p = FooOperator() | BarOperator() // BarOperator()

    p.run()
    p.submit()


'''
Operator Fusion for cluster task executer.
Create a fused operator which accepts a array of operators
to be called together.
Use eval to gen the fused operator contaning operators to be fused
and then pickle it
Codegen to run the picked fused operator as an application

arr = [1, 3, 5]

class Foo:
    def __init__(self, arr):
        self.arr = arr

    def foo(self):
        for v in arr:
            print(v)

val = eval('Foo(arr)')
val.foo()

arr = [1, 3, 5]

class Foo:
    def __init__(self, arr):
        self.arr = arr
        self.foos = [self.foo, self.bar]

    def foo(self):
        for v in arr:
            print(v)

    def bar(self):
        print(14)

val = eval('Foo(arr)')
val.foos[1]()
'''
