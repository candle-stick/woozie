from dataclasses import dataclass
from typing import Any, List, Tuple, Iterator

from networkx import (
    DiGraph,
    compose,
    is_directed_acyclic_graph,
    number_weakly_connected_components,
    relabel_nodes,
    topological_sort,
    weakly_connected_components,
    compose_all,
    descendants,
    number_of_nodes,
    dfs_successors,
    topological_sort,
)
from networkx.drawing.nx_pydot import to_pydot

from woozie.domain.action import Action
from woozie.domain.workflow import Workflow


class WorkflowGraphError(Exception):
    """Exception raised for errors during workflow graph construction.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message


@dataclass
class WorkflowGraphBuilder:
    """Defines how to construct a Workflow object from YAML."""

    workflow: Workflow = None
    # Every fork/join pair needs a unique name
    # To keep the names short, we number them sequentially
    fork_count: int = 0

    def build_workflow_graph(
        self,
        generate_graphviz: bool = True,
    ) -> DiGraph:

        # Construct Input Graph from workflow definition
        input_graph = self.build_input_graph(self.workflow)

        # Visualize Input Graph
        if generate_graphviz:
            self.draw(input_graph, "assets/workflow-definition-dag.png")

        # Make sure Input Graph is a Directed Acyclic Graph
        if not is_directed_acyclic_graph(input_graph):
            raise WorkflowGraphError(
                "DAG incorrectly defined. Cyclic dependencies found."
            )

        # Derive Workflow DAG from Input DAG
        workflow_dag = self.build_workflow_dag(input_graph)

        # Visualize Workflow DAG
        if generate_graphviz:
            self.draw(workflow_dag, "assets/oozie-workflow-dag.png")

        return workflow_dag

    def build_input_graph(self, workflow: Workflow) -> DiGraph:
        # Add all actions to a map of string -> action
        action_dict = {action.name: action for action in workflow.actions}
        action_names = action_dict.keys()

        # DAG of the workflow in its raw/un-optimized state
        input_graph = DiGraph()

        # Add all dependencies as edges
        dependencies = [
            (action_dict[dependency], action)
            for action in workflow.actions
            for dependency in action.dependencies
        ]
        input_graph.add_edges_from(dependencies)

        # Add all the actions as vertices.
        input_graph.add_nodes_from(workflow.actions)

        # Make sure all dependencies have an associated action
        dep_actions = set(
            [
                dependency
                for action in workflow.actions
                for dependency in action.dependencies
            ]
        )
        if not dep_actions.issubset(action_names):
            dep = dep_actions - action_names
            raise WorkflowGraphError(f"Missing action for dependencies: {dep}")

        return input_graph

    def build_workflow_dag(self, G: DiGraph) -> DiGraph:
        components = self.get_components(G)
        subcomponents = [self.process_subcomponent(c) for c in components]
        composed = compose_all(subcomponents)
        processed = self.process_components(composed)
        return processed

    def get_components(self, G: DiGraph) -> List[DiGraph]:
        """List of weakly connected components as Directed Graphs."""
        components = weakly_connected_components(G)
        return [G.subgraph(c).copy() for c in components]

    def process_subcomponent(self, G: DiGraph) -> DiGraph:
        """Parallelize each subcomponent."""
        return self.parrallelize_subcomponent(G)

    def process_components(self, G: DiGraph) -> DiGraph:
        """Introduce concurrency between component DAGs.

        Returns:
            A new graph with fork/join pairs, and the
            "first" and "last" node.
        """
        # Parrellelize components
        if number_weakly_connected_components(G) > 1:
            edges, count = self.add_fork_join(G, self.fork_count)
            self.fork_count = count
            G.add_edges_from(edges)

        # Add error handler node
        if self.workflow.error_handler:
            error_edge = self.add_error_node(G)
            G.add_edges_from(error_edge)

        # Add start and end control flow nodes
        control_edges = self.add_start_end(G)
        G.add_edges_from(control_edges)
        return G

    def parrallelize_subcomponent(self, G: DiGraph) -> DiGraph:
        """Introduce concurrency within component DAG.

        Returns:
            A new graph with fork/join pairs.
        """
        sub_component = DiGraph()

        while number_of_nodes(G) > 0:
            # traverse subgraph to extract forkless paths
            roots = [node for node in iter(G) if G.in_degree(node) == 0]
            paths = [self.single_edge_path(G, start) for start in roots]
            paths_flatten = [node for path in paths for node in path]

            # build result graph with fork/joins
            if len(paths) > 1:
                result = G.subgraph(paths_flatten).copy()
                edges, count = self.add_fork_join(result, self.fork_count)
                self.fork_count = count
                result.add_edges_from(edges)
                # append resulting graph to previous
                if sub_component:
                    sub_component = compose(sub_component, result)
                    entry = self.entry_nodes(result)[0]
                    exit = self.exit_nodes(sub_component)[0]
                    sub_component.add_edge(exit, entry)
                else:
                    sub_component = compose(sub_component, result)

            # append single-node decomposition
            elif len(paths) == 1:
                if sub_component:
                    n = self.exit_nodes(sub_component)[0]
                    m = paths_flatten[0]
                    sub_component.add_edge(n, m)
                else:
                    sub_component = compose(
                        sub_component, G.subgraph(paths_flatten).copy()
                    )

            # drop processed nodes
            G.remove_nodes_from(paths_flatten)

        return sub_component

    def add_start_end(self, G: DiGraph) -> List[Any]:
        """Create head and tail nodes."""
        # Add standard Oozie control flow nodes that must be present in every workflow
        start = Action(name="start", action_type="start")
        end = Action(name="end", action_type="end")

        start = [(start, node) for node in self.entry_nodes(G)]
        end = [(node, end) for node in self.exit_nodes(G)]

        # Ensure start and end transtion to/from one node
        if len(start) != 1 or len(end) != 1:
            raise WorkflowGraphError("Multiple transitions found for start/end nodes")

        edges = start + end
        return edges

    def add_fork_join(self, G: DiGraph, count: int) -> Tuple[List[Any], int]:
        """Create a fork/join pair and add it to a graph"""
        fork = Action(name=f"fork-{count}", action_type="fork")
        join = Action(name=f"join-{count}", action_type="join")

        fork_edges = [(fork, node) for node in self.entry_nodes(G)]
        join_edges = [(node, join) for node in self.exit_nodes(G)]
        edges = fork_edges + join_edges
        count += 1
        return (edges, count)

    def add_error_node(self, G: DiGraph) -> Any:
        error_node = self.workflow.error_handler
        edge = [(node, error_node) for node in self.exit_nodes(G)]

        # Ensure single transition into error node
        if len(edge) != 1:
            raise WorkflowGraphError("Multiple transitions found for error node")

        return edge

    def single_edge_path(self, G: DiGraph, source) -> List[Any]:
        """ Contiguous nodes with one single edge in and out."""
        stack = [source]
        visited = []
        while stack:
            current = stack[-1]
            visited.append(current)
            # visit only single-child successors
            children = dfs_successors(G, source=current, depth_limit=1)
            if children and len(children[current]) == 1:
                s = children[current][0]
                stack.pop()
                # keep children nodes with one path in and out
                if G.in_degree(s) == 1 and G.out_degree(s) == 1:
                    stack.append(s)
            else:
                stack.pop()
        return visited

    @staticmethod
    def entry_nodes(G: DiGraph) -> List[Any]:
        return [node for node in G.nodes() if G.in_degree(node) == 0]

    @staticmethod
    def exit_nodes(G: DiGraph) -> List[Any]:
        return [node for node in G.nodes() if G.out_degree(node) == 0]

    @staticmethod
    def sort(G: DiGraph):
        return topological_sort(G)

    @staticmethod
    def get_transitions(G: DiGraph, node: Action) -> Iterator[Action]:
        return G.neighbors(node)

    @staticmethod
    def draw(G: DiGraph, output_dir: str):
        # re-label node with action names
        mapping = {action: action.name for action in G.nodes()}
        g = relabel_nodes(G, mapping)
        # export image
        pdot = to_pydot(g)
        pdot.write_png(output_dir)
