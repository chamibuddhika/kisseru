
try:
    import cPickle as pickle
except:
    import pickle
import sys

class Foo(object):

  def __init__(self):
    self.val = 42

  def foo(self):
    print("In foo")

class FooBar(Foo):

  def bar(self):
    print("In bar")

if __name__ == "__main__":

  fb = FooBar()

  fp = open("test.dat", 'wb')
  pickle.dump(fb, fp)
  fp.close()

  fp = open("test.dat", 'rb')
  fb1 = pickle.load(fp)
  fp.close()

  fb1.bar()
  fb1.foo()

  
  print(fb1.val)
  print(fb1.__class__.__name__)
