
from contextlib import closing

import utils
import threading

class Foo(object):

  def __init__(self):
    self.val = 42

  def run(self, x, partition):
    print("Running run at thread %s" % threading.current_thread())
    x.append(self.val)
    return x

def run(x):
  return x + 42
  
if __name__ == "__main__":
    # Define the dataset
    dataset = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]

    # Output the dataset
    print ('Dataset: ' + str(dataset))

    # Run this with a pool of 5 agents having a chunksize of 3 until finished
    f = Foo()
    agents = 5
    chunksize = 3
    '''
    with closing(Pool(processes=agents)) as pool:
        result = pool.map(f.run, dataset, chunksize)
	pool.terminate()
    '''
    dataset = utils.parmap(f.run, dataset)

    # Output the result
    print ('Result:  ' + str(dataset))
