from enum import Enum
from tasks import FusedTask
from passes import Pass
from passes import PassResult


class Fusion(Pass):
    def __init__(self, name):
        Pass.__init__(self, name)
        self.description = "Running the task fusion optimizer"

    def _dfs(self, node, cur_fusables, all_fusables, visited):
        if node in visited:
            return

        visited.add(node)
        children = node.get_children()

        if len(children) == 1:
            child = children[0]

            if len(child.get_parents()) == 1:
                cur_fusables.append(child)
                self._dfs(child, cur_fusables, all_fusables, visited)
                return

        all_fusables.append(cur_fusables)

        # Current node ends a fusable region. Try forming new fusable regions
        # starting with its children
        for child in children:
            cur_fusables = [child]
            self._dfs(child, cur_fusables, all_fusables, visited)

    def run(self, graph, ctx):
        Pass.run(self, graph, ctx)
        all_fusables = []
        visited = set()
        for name, source in graph.sources.items():
            cur_fusables = [source]
            self._dfs(source, cur_fusables, all_fusables, visited)

        # Filter out single node fusable regions which are redundant
        fusables = [fusable for fusable in all_fusables if len(fusable) > 1]

        fused_tasks = list(map(lambda fusable: FusedTask(fusable), fusables))

        for fused_task in fused_tasks:
            print("Before")
            for t in fused_task.tasks:
                print("Task : {}".format(t.name))
                for e in t.edges:
                    s = e.source
                    d = e.dest
                    print("Edge : source ({}) -> dest ({})".format(
                        type(s).__name__,
                        type(d).__name__))

            graph.add_task(fused_task)

            # If the head of the fused task sequence is a source remove it and
            # make the fused container task the source
            if fused_task.head.id in graph.sources:
                graph.unset_source(fused_task.head)
                graph.set_source(fused_task)

            # If the tail of the fused task sequence is a sink make the fused
            # container task a sink as well
            if fused_task.tail.is_sink:
                fused_task.is_sink = True

            print("After")
            for t in graph.get_task(fused_task.id).tasks:
                print("Task : {}".format(t.name))
                for e in t.edges:
                    s = e.source
                    d = e.dest
                    print("Edge : source ({}) -> dest ({})".format(
                        type(s).__name__,
                        type(d).__name__))

        print("\n\n\n")

        for tid, task in graph.tasks.items():
            if isinstance(task, FusedTask):
                print("Fused task : {} ID - {}".format(task.name, task.id))
                print("**************")
                for t in task.tasks:
                    print("Task : {}".format(t.name))
                    for e in t.edges:
                        s = e.source
                        d = e.dest
                        print("Edge : source ({}) -> dest ({})".format(
                            type(s).__name__,
                            type(d).__name__))

        print("")

        for fused_task in fused_tasks:
            print("Fused task : {} ID - {}".format(fused_task.name,
                                                   fused_task.id))
            print("------------")
            for task in fused_task.tasks:
                print("Task : {}".format(task.name))
                for e in task.edges:
                    s = e.source
                    d = e.dest
                    print("Edge : source ({}) -> dest ({})".format(
                        type(s).__name__,
                        type(d).__name__))
        print("")

    def post_run(self, graph, ctx):
        pass
