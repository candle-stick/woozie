from src.action import Action
from src.workflow import Workflow
from src.workflowgraph import WorkflowGraphBuilder
from src.oozieworkflow import OozieWorkflow
from src.io import File

from pathlib import Path


def main():
    Path("assets").mkdir(exist_ok=True)
    workflow_data = File.read("workflow.yaml")
    config_data = File.read("config.yaml")
    workflow = Workflow.build(workflow_data, config_data)
    OozieWorkflow(workflow).generate()


if __name__ == "__main__":
    main()