
from abc import ABCMeta
from enum import Enum

from utils import parmap

class RunnerType(Enum):
  SMP     = 1
  CLUSTER = 2
  SPARK   = 3

class Runner(object):

  def __init__(self, runner_type = RunnerType.SMP):
    self.type = runner_type
 
class Backend(object):
  __metaclass__ = ABCMeta

  def __init__(self):
    self.code = []
    self.descriptor = {}
    self.passes = [self.gen, self.gen_descriptor, self.flush]

  @abstractmethod
  def gen_descriptor(self, obj):

  @abstractmethod
  def gen(self, obj): 

  @abstractmethod
  def flush(self, obj):

  @abstractmethod
  def run_passes():
    ir = None
    for p in passes:
      ir = p(ir)

class SMPBackEnd(Backend):

  # default indentation size
  INDENT = 4

  def __init__(self):
    self.cur_level = 0

  def gen(self, obj):
    if isinstance(obj, Pipeline):
      if not obj.is_nested:
        # print import headers using obj.classes
        self._gen_imports(obj.classes)
        
	# generate main function header 
	self._gen_main_header()

        # unpickle and run the pipeline 
	self._gen_main_body(4)
    else:
      raise Exception("Invalid Pipeline definition")
    return obj

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
    self._add("from kiseru import Operator")
    self._add("from kiseru import Pipeline")
    self._addln("from kiseru import Conditional")

    for cls in classes:
      self._add("import {0}".format(cls))

    self._addln("")

  def _gen_main_header(self, nspaes = 0):
    self._add("if __name__ == '__main__':")

  def _gen_main_body(self, nspaces = 0):
    self._indent_plus()
    self._add("fp = open("test.dat", 'rb')")
    self._add("p = pickle.load(fp)")
    self._add("fp.close()")
    self._add("p.run()")
    self._indent_minus()

  def _add(self, line):
    self.code.append(self._indent_line(line, self._cur_level * self.INDENT)))

  def _addln(self, line)
    self._add(line)
    self._add("")

  def _indent_plus(self)
    self._cur_level += 1

  def _indent_minus(self)
    self._cur_level -= 1

  def _indent_line(self, line, nspaces):
    indent = " " * nspaces 
    return indent + line

class Operator(object):
  __metaclass__ = ABCMeta
  
  def __init__(self):
    self.is_pipeline  = False
    self.is_parallel  = False
    self.is_iterative = False
    self.until  = None
    self.inputs  = {}
    self.outputs = {}

  @abstractmethod
  def run(self, inputs):

  @abstractmethod
  def codegen(self, backend):
    backend.gen(self)

  @abstractmethod
  def _run_wrapper(self, inputs):

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
class FusedOperator(Operator):

  def __init__(self, operators):
    self.operators = operators

class ParallelOp(Operator):

  def __init__(self, operator, parallelism):
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

class Conditional:
  __metaclass__ = ABCMeta

  @abstractmethod
  def check(res):

class Pipeline(Operator):

  def __init__(self):
    self.operators = []
    self.runner    = None
    self.classes   = set() 
    self.is_nested = False
  
  def do(self, operator):
    if isinstance(operator, Pipeline):
      operator.is_pipeline = True
    self.operators.append(operator)
    self.classes.add(operator.__class__.__name__)

  def do_par(self, operator, parallelism = -1):
    if isinstance(operator, Pipeline):
      operator.is_pipeline = True
      operator.is_nested   = True
    self.operators.append(ParallelOp(operator, parallelism))
    self.classes.add(operator.__class__.__name__)

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

  def run(self, inputs = None):
    res = inputs
    for operator in self.operators:
      res = operator.run(res)

  def submit(self):
    # default to LOCAL runner
    if self.runner == None:
      self.runner = RunnerType.SMP
      backend = SMPBackend()

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

  '''
  res = None
  for op in ops:
    if res != None:
      if op instance of ParallelOp:
        spwan a thread for each _data output in the previous result
	combine results
      if op instance of IterativeOp:
	for op1 in op.ops:
	  res = run op
	  if until.check(res):
	    break
  '''
