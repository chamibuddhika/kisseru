from multiprocessing import Process


class ProcessFactory(object):
    processes = []

    @staticmethod
    def create_process(fn, args):
        p = Process(target=threaded_receive, args=args)
        p.start()
        ProcessFactory.processes.append(p)

    @staticmethod
    def join_all():
        for process in processes:
            p.join()
