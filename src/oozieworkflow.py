import logging
from dataclasses import dataclass
from functools import partial

# Type hinting
from typing import IO, Dict, Iterator, List, Set

from lxml.etree import Element, ElementTree, SubElement
from networkx import DiGraph

from .action import Action
from .exceptions.exception import WorkflowGraphError
from .workflow import Workflow
from .workflowgraph import WorkflowGraphBuilder


@dataclass
class OozieWorkflow:
    """Generates Oozie workflows from worflow."""

    workflow: Workflow

    def generate(self):

        try:
            workflow_graph = WorkflowGraphBuilder(self.workflow).build_workflow_graph()
        except WorkflowGraphError as e:
            logging.exception(f"Unable to generate workflow\n{e}")

        try:
            tree = self.build_xml_tree(workflow_graph)
            self.write_xml(tree)
        except Exception as e:
            logging.exception(f"Failed to build XML\n{e}")

    def build_xml_tree(self, G: DiGraph) -> ElementTree:
        # visit every node in topological order
        top_order = [action for action in WorkflowGraphBuilder.sort(G)]

        error_node = top_order[-2]

        root = Element(
            "workflow-app", name=self.workflow.name, xmlns="uri:oozie:workflow:1.0"
        )
        sub_element = partial(SubElement, root)

        for a in top_order:
            transition = WorkflowGraphBuilder.get_transitions(G, a)

            if a.action_type == "start":
                sub_element("start", to=next(transition).name)

            elif a.action_type == "end":
                sub_element("end", name=a.name)

            elif a.action_type == "fork":
                fork = sub_element("fork", name=a.name)
                # add fork paths
                for path in transition:
                    SubElement(fork, "path", name=path.name)

            elif a.action_type == "join":
                sub_element("join", name=a.name, to=next(transition).name)

            else:
                next_action = next(transition).name
                if a.name == "error_handler":
                    self.get_action_element(root, a, next_action)
                else:
                    self.get_action_element(root, a, next_action, error_node.name)

        return ElementTree(root)

    def get_action_element(
        self, root: Element, action: Action, next_action: str, error_action: str = None
    ) -> Element:
        # add workflow action
        action_name = SubElement(root, "action", name=action.name)

        config = iter(action.config)
        option = next(config)

        # add oozie action
        if action.config[option]:
            oozie = SubElement(action_name, option, **action.config[option])
        else:
            oozie = SubElement(action_name, option, action.config[option])

        sub_element = partial(SubElement, oozie)
        for opt in config:
            conf_option = action.config[opt]

            # set multi-value properties: List
            if isinstance(conf_option, list):
                for text in conf_option:
                    child = sub_element(opt)
                    child.text = text

            # set configuration properties: Dict[dict]
            elif opt == "configuration":
                conf_element = sub_element(opt)
                for name, value in conf_option["properties"].items():
                    prop_element = SubElement(conf_element, "property")
                    child_name = SubElement(prop_element, "name")
                    child_name.text = name
                    child_value = SubElement(prop_element, "value")
                    child_value.text = value

            # set single-value properties: Str
            else:
                child = sub_element(opt)
                child.text = conf_option

        # Oozie "ok" transition
        SubElement(action_name, "ok", to=next_action)

        # Oozie "error" transition
        if error_action:
            SubElement(action_name, "error", to=error_action)

        return root

    def write_xml(self, tree: ElementTree):
        tree.write(
            "assets/output.xml",
            pretty_print=True,
            xml_declaration=True,
            encoding="utf-8",
        )
