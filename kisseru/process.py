from multiprocessing import Process
from multiprocessing import Barrier


class ProcessFactory(object):
    processes = []
    barrier = None

    @staticmethod
    def set_barrier(n_tasks):
        ProcessFactory.barrier = Barrier(n_tasks)

    @staticmethod
    def wait():
        ProcessFactory.barrier.wait()

    @staticmethod
    def create_process(fn, args):
        p = Process(target=fn, args=args)
        p.start()
        ProcessFactory.processes.append(p)

    @staticmethod
    def join_all():
        print("Joining all threads")
        for process in ProcessFactory.processes:
            process.join()
