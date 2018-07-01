
from abc import ABCMeta
from enum import Enum

from utils import parmap

class Runner(Enum):
  SMP     = 1
  CLUSTER = 2
  SPARK   = 3
 
class Backend(object):
  __metaclass__ = ABCMeta

  def __init__(self):
    self.code = []

  @abstractmethod
  def gen(self, obj): 

class SMPBackEnd(Backend):

  def __init__(self):

  def gen(self, obj):
    if isinstance(obj, Pipeline):
      if not obj.is_nested:
        # Print import headers using obj.classes
        # Unpickle the pipeline 
        # Run it
    else:
      raise Exception("Invalid Pipeline definition")

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

class ParallelOp(Operator):

  def __init__(self, operator, parallelism):
    self.is_parallel = True
    self.operator    = operator
    self.parallelism = parallelism

  def run(self, inputs):
    # If the parallelism is not given, it is set to the number of processors by 
    # parmap_dict
    if parallelism == -1:
      parmap_dict(self.operator.run, dataset, "__data__")
    else:
      parmap_dict(self.operator.run, dataset, "__data__", parallelism)

class IterativeOp(Operator):

  def __init__(self, iters, until):
    self.is_iterative = True
    self.iters = iters
    self.until = until

    for it in iters:
      if isinstance(it, Pipeline):
	operator.is_pipeline = True
	operator.is_nested   = True

  def run(self, inputs):
    res = None
    for it in iters:
      res = it.run(res)
    return res

class Conditional:
  __metaclass__ = ABCMeta

  @abstractmethod
  def check():

class Pipeline(Operator):

  def __init__(self):
    self.operators = []
    self.runner    = None
    self.classes   = {}
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
    self.classes.add(operator.__class__.__name__)

  def run(self, inputs):
    res = None
    for operator in self.operators:
      res = operator.run(res)

  def submit(self):
    # default to LOCAL runner
    if self.runner == None:
      self.runner = Runner.LOCAL
      backend = LocalBackend()

  def set_runner(self, runner):
    self.runner = runner 

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
