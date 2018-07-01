
class Visitor:
  def visit(self, element):
    attrs = dir(element)
    for attr in attrs:
      if not attr.startswith('_'):
        print("# %s -> %s" % (attr, getattr(element, attr)))

class CodeGen:

  def codegen(self, visitor):
    visitor.visit(self)

class Foo(CodeGen):

  def __init__(self):
    self.foo = "foo-val"
    self.child = Bar()

  def codegen(self, visitor):
    self.child.codegen(visitor)
    visitor.visit(self) 

class Bar(CodeGen):

  def __init__(self):
    self.bar = "bar-val"

if __name__ == "__main__":
  foo = Foo()
  v   = Visitor()

  foo.codegen(v)

'''
ScriptOp
CrossValidation
  - Split
  - kfolds
  - 

  sess = Session("rest-endpoint")

  p = Pipeline();
  p.source(DataNode("file://text.csv")
  p.runner("local") # "cluster | spark etc.."  

  modelSelection = Pipeline()
  modelSelection
   .do(CrossValidate().setLearners("logistic-regression",
     "knn").setParamGrid());

  iters = []
  for gridSize in [(3, 3), (5, 5)]
    for splitSize in [(5, 5), (15, 15), (25, 25)]
      iters.append(
        Pipeline()
          .do(GridSummarize().setGridSize(gridSize))
	  .do(PartitionGrid().splitSize(splitSize))
          .doPar(v))

  p.do(LoadNetCDF())
   .do(FeatureSelect())
   .doIter(iters, break = CheckThresholds())
   .do(SaveModel())

  sess.submit(p)
'''
