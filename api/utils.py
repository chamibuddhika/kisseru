
import multiprocessing
import numpy as np
import pandas as pd

def parmap(f, data, nprocs=multiprocessing.cpu_count()):
    q_in = multiprocessing.Queue(1)
    q_out = multiprocessing.Queue()

    proc = [multiprocessing.Process(target=_run, 
                                    args=(f, partition, data, q_in, q_out)) 
              for partition in range(nprocs)]
    for p in proc:
        p.daemon = True
        p.start()

    sent = [q_in.put((i, x)) for i, x in enumerate(data)]
    [q_in.put((None, None)) for _ in range(nprocs)]
    res = [q_out.get() for _ in range(len(sent))]

    [p.join() for p in proc]

    return [x for i, x in sorted(res)]

def parmap_dict(fn, data, par_key, nprocs=multiprocessing.cpu_count()):
    q_in = multiprocessing.Queue(1)
    q_out = multiprocessing.Queue()

    if isinstance(data[par_key], pd.DataFrame):
      length = X[par_key].shape[0] 
    else:
      length = len(data[par_key])
    partitions = _chunk(length, nprocs)

    proc = [multiprocessing.Process(
             target=_run, 
             args=(fn, partition, data, q_in, q_out, _dict_merge)) 
            for partition in range(nprocs)]

    for p in proc:
        p.daemon = True
        p.start()

    sent = [q_in.put((i, x)) for i, x in enumerate(partitions)]
    [q_in.put((None, None)) for _ in range(nprocs)]
    res = [q_out.get() for _ in range(len(sent))]

    [p.join() for p in proc]

    return [x for i, x in sorted(res)]

def _run(fn, partition, data, q_in, q_out, merge_partition = lambda x, y : y):
  while True:
    i, x = q_in.get() # Partitioned data
    if i is None:
      break
    q_out.put((i, fn(merge_partition(data, x))))

def _chunk(length, n):
  chunks = []
  idx    = 0
  chunk_size = length / n

  while idx < length:
    if idx + chunk_size <= length:
      chunks.append((idx, idx + chunk_size))
    else:
      chunks.append((idx, length))
    idx += chunk_size
  return chunks

def _dict_merge(data, par_key, indexes):
  if isinstance(data[par_key], pd.DataFrame):
    data[par_key] = data[par_key].iloc[indexes[0]:indexes[1]]
  else:
    data[par_key] = data[par_key][indexes[0]:indexes[1]]
  return data
