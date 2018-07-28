from itertools import islice
from passes import Pass
from passes import PassResult
from tasks import Sink


class DotGraphGenerator(Pass):
    def __init__(self, name, tag=""):
        Pass.__init__(self, name, tag)
        self.pre_graph = None
        self.post_graph = None
        self.description = "Generating the dot graph"

    def _gen_dot_graph(self, graph, paths, labels):
        header = "digraph {} {{\n".format(graph.name)
        body = ""
        nodes = ""
        for label in labels:
            tokens = label.split(":")
            node = tokens[0]
            attr_open = "["
            attrs = "fillcolor=lightcyan "  # Default fill color is green
            is_generated = False
            for attr in islice(tokens, 1, None):
                if attr == "source":
                    # If it is a source we change the border to be double lined
                    attrs += "peripheries=2 "
                elif attr == "sink":
                    # If it is a sink we override the file color with orange
                    attrs += "fillcolor=orange "
                elif attr == "generated":
                    is_generated = True
                    # If it is a generated node we change the border to be a
                    # a dotted  red line
                    attrs += "shape=box fillcolor=red "

            # attrs += 'style="filled, dotted"' if is_generated else 'style=filled'
            attrs += 'style=filled'
            attr_close = "]"
            node = node + " " + attr_open + attrs + attr_close + "\n"
            nodes += node

        # Add node configurations as the first part of the body
        body += nodes

        # Next we add the paths in the graph
        for path in paths:
            body += '->'.join(path)
            body += '\n'
        footer = "}"
        return header + body + footer

    def _dfs(self, node, cur, paths, labels):
        if cur == None:
            cur = []

        label = node.name

        if node.is_source:
            # Update the node's label to mark it as source
            label = node.name + ":source"
        if node.is_sink:
            # Update the node's label to mark it as sink
            label = label + ":sink"
        if node.is_staging or node.is_transform:
            # Update the node's label to mark it as generated
            label = label + ":generated"

        cur.append(node.name)
        labels.add(label)

        # Handle the first edge separately since we continue the top down path
        # at left most child
        edge_zero = node.edges[0]
        if not node.is_sink:
            self._dfs(edge_zero.dest.task_ref, cur, paths, labels)
        else:
            paths.append(cur)
            return

        # Generate new paths for non left most children
        if len(node.edges) > 1:
            for edge in islice(node.edges, 1, None):
                self._dfs(edge.dest.task_ref, [node.name], paths, labels)

    def _generate_dot_graph(self, graph):
        paths = []
        labels = set()
        for tid, source in graph.sources.items():
            self._dfs(source, None, paths, labels)
        return self._gen_dot_graph(graph, paths, labels)

    def run(self, graph, ctx):
        Pass.run(self, graph, ctx)
        graph_map = ctx.properties.get('__dot_graph__', None)
        if not graph_map:
            graph_map = {}
        graph_map[self.tag] = self._generate_dot_graph(graph)
        ctx.properties['__dot_graph__'] = graph_map

        return PassResult.CONTINUE

    def post_run(self, graph, ctx):
        graph_map = ctx.properties['__dot_graph__']
        if graph_map:
            for tag, dot_graph in graph_map.items():
                filename = graph.name
                if not tag or tag != "":
                    filename = "{}-{}".format(graph.name, tag)

                with open("{}.dot".format(filename), "w") as f:
                    f.write(dot_graph)
