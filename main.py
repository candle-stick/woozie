from src.action import Action
from src.workflow import Workflow
from src.workflowgraph import WorkflowGraphBuilder
from src.oozieworkflow import OozieWorkflow

from pathlib import Path


def main():
    Path('assets').mkdir(exist_ok=True)
    workflow = Workflow().build('workflow.yaml', 'config.yaml')
    OozieWorkflow(workflow).generate()
    
if __name__ == '__main__':
    main()