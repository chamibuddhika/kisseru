from itertools import islice
from passes import Pass
from passes import PassResult
from tasks import FusedTask
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

        # BEGIN -- Path segment related classes and utils
        class PathSegment:
            def __init__(self, typ):
                self.type = typ
                self.items = []

            def extend(self, item):
                self.items.append(item)

            def __repr__(self):
                return self.items

            def __str__(self):
                return str(self.items)

        def get_next_segment(path, index):
            lookahead = None
            ptr = len(path)
            if index + 1 < len(path):
                lookahead = path[index + 1]
                ptr = index + 1

            segment = None
            if lookahead:
                if lookahead == "(":
                    segment = PathSegment("GROUPED")
                    ptr += 1
                else:
                    segment = PathSegment("RAW")
            return (segment, ptr)

        def get_next_segment_head(path, index):
            head = None
            ptr = len(path)
            if index + 1 < len(path):
                head = path[index + 1]
                ptr = index + 1

            if head:
                if head == "(":
                    ptr += 1
                    head = path[ptr]
            return head

        # END --

        # Next we add the paths in the graph
        segments = []
        segment_edges = []
        for path in paths:
            i = 0
            queue = []
            cur_segment, i = get_next_segment(path, -1)
            while i < len(path):
                if path[i] == "(":
                    while queue:
                        cur_segment.extend(queue.pop(0))
                    next_head = get_next_segment_head(path, i)
                    if next_head:
                        segment_edges.append((cur_segment.items[-1],
                                              next_head))
                    segments.append(cur_segment)
                    cur_segment = PathSegment("GROUPED")
                    i += 1
                    continue

                if path[i] == ")":
                    while queue:
                        cur_segment.extend(queue.pop(0))
                    next_head = get_next_segment_head(path, i)
                    if next_head:
                        segment_edges.append((cur_segment.items[-1],
                                              next_head))
                    segments.append(cur_segment)
                    cur_segment, i = get_next_segment(path, i)
                    continue

                queue.append(path[i])
                i += 1
            while queue:
                cur_segment.extend(queue.pop(0))

            if cur_segment:
                segments.append(cur_segment)

        # Draw subgraph regions which corresponds to fused tasks
        subgraph_cntr = 0
        for segment in segments:
            if segment.type == "GROUPED":
                body += "subgraph cluster{} {{\n".format(subgraph_cntr)
                body += "style=filled\n"
                body += "color=lightgrey\n"
                body += '->'.join(segment.items)
                body += "\n"
                body += "}\n"
                subgraph_cntr += 1
            else:
                body += '->'.join(segment.items)
                body += '\n'

        # Connect fused and unfused regions together
        for segment_edge in segment_edges:
            body += '{}->{}\n'.format(segment_edge[0], segment_edge[1])

        footer = "}"
        return header + body + footer

    def _gen_label(self, node):
        label = node.name
        if node.is_source:
            # Update the node's label to mark it as source
            label = label + ":source"
        if node.is_sink:
            # Update the node's label to mark it as sink
            label = label + ":sink"
        if node.is_staging or node.is_transform:
            # Update the node's label to mark it as generated
            label = label + ":generated"
        return label

    def _traverse(self, node, cur, paths, visited, labels):
        # Handle the first edge separately since we continue the top down path
        # at left most child
        if not node.is_sink:
            edge_zero = node.edges[0]
            self._dfs(edge_zero.dest.task_ref, cur, paths, visited, labels)
        else:
            paths.append(cur)
            return

        # Generate new paths for non left most children
        if len(node.edges) > 1:
            for edge in islice(node.edges, 1, None):
                self._dfs(edge.dest.task_ref, [node.name], paths, visited,
                          labels)

    def _dfs(self, node, cur, paths, visited, labels):
        if node == None:
            return

        if cur == None:
            cur = []

        if isinstance(node, FusedTask):
            label = node.head.name
            if node.id in visited:
                cur.append(label)
                paths.append(cur)
                return
            else:
                cur.append("(")
                for task in node.tasks:
                    # Generate the node label with attributes
                    label = self._gen_label(task)
                    # Extend the current path with the node name
                    cur.append(task.name)
                    # Accumulate this node's label
                    labels.add(label)
                cur.append(")")
                visited.add(node.id)
                # Traverse the children starting from the tail of this fused
                # task
                self._traverse(node.tail, cur, paths, visited, labels)
        else:
            label = node.name
            if node.id in visited:
                cur.append(label)
                paths.append(cur)
                return

            # Generate the node label with attributes
            label = self._gen_label(node)
            visited.add(node.id)
            # Extend the current path with the node name
            cur.append(node.name)
            # Accumulate this node's label
            labels.add(label)
            # Traverse the node's children with dfs.
            self._traverse(node, cur, paths, visited, labels)

    def _generate_dot_graph(self, graph):
        paths = []
        visited = set()
        labels = set()
        for tid, source in graph.sources.items():
            self._dfs(source, None, paths, visited, labels)
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
