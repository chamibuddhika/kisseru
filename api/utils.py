
import multiprocessing
import numpy as np
import pandas as pd

def parmap(f, data, nprocs=multiprocessing.cpu_count()):
    q_in = multiprocessing.Queue(1)
    q_out = multiprocessing.Queue()

    if isinstance(data, pd.DataFrame):
      length = data.shape[0] 
    else:
      length = len(data)
    partitions = _chunk(length, nprocs)

    proc = [multiprocessing.Process(target=_run, 
                                    args=(q_in, q_out, f, partition, data, _array_split)) 
              for partition in range(nprocs)]
    for p in proc:
        p.daemon = True
        p.start()

    sent = [q_in.put((i, x)) for i, x in enumerate(partitions)]
    [q_in.put((None, None)) for _ in range(nprocs)]
    res = [q_out.get() for _ in range(len(sent))]

    [p.join() for p in proc]

    return [x for i, x in sorted(res)]

def parmap_dict(fn, data, par_key, nprocs=multiprocessing.cpu_count()):
    q_in = multiprocessing.Queue(1)
    q_out = multiprocessing.Queue()

    if isinstance(data[par_key], pd.DataFrame):
      length = data[par_key].shape[0] 
    else:
      length = len(data[par_key])

    nprocs = nprocs if nprocs < length else length - 1
    print("Parallelism %d" % (nprocs + 1))
    partitions = _chunk(length, nprocs)

    proc = [multiprocessing.Process(
             target=_run, 
             args=(q_in, q_out, fn, partition, data, _dict_split, par_key)) 
            for partition in range(nprocs)]

    for p in proc:
        p.daemon = True
        p.start()

    sent = [q_in.put((i, x)) for i, x in enumerate(partitions)]
    [q_in.put((None, None)) for _ in range(nprocs)]
    res = [q_out.get() for _ in range(len(sent))]

    [p.join() for p in proc]

    return [x for i, x in sorted(res)]

def _run(q_in, q_out, fn, partition, data,
         split_partition = lambda data, partition, indices, par_key = None : data,
	 par_key = None):
  while True:
    partition, indices = q_in.get() # Partitioned data
    if partition is None:
      break
    q_out.put((partition, fn(split_partition(data, partition, indices, par_key), partition)))

def _chunk(length, n):
  chunks = []
  idx    = 0
  chunk_size = int(length / n)

  while idx < length:
    if idx + chunk_size <= length:
      chunks.append((idx, idx + chunk_size))
    else:
      chunks.append((idx, length))
    idx += chunk_size
  return chunks

def _array_split(data, partition, indices, par_key):
  if isinstance(data, pd.DataFrame):
    # This should return a view of the DataFrame instead of a copy 
    return data.iloc[indices[0]:indices[1]]
  else:
    # This only returns a view if the array is a numpy array not a python list
    return data[indices[0]:indices[1]]

def _dict_split(data, partition, indices, par_key):
  # Makes a shallow copy
  copy = data.copy()
  if isinstance(copy[par_key], pd.DataFrame):
    # This should return a view of the DataFrame instead of a copy 
    copy[par_key] = data[par_key].iloc[indices[0]:indices[1]]
  else:
    # This only returns a view if the array is a numpy array not a python list
    copy[par_key] = data[par_key][indices[0]:indices[1]]
  return copy
