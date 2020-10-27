from woozie.domain.action import Action
from woozie.domain.workflow import Workflow
from woozie.domain.workflowgraph import WorkflowGraphBuilder
from woozie.domain.oozieworkflow import OozieWorkflow
from woozie.domain.io import File

from pathlib import Path


def generate_workflow(
    output_directory: str,
    workflow_definition: str = "workflow.yaml",
    workflow_configuration: str = "config.yaml",
    show_graph: bool = True,
):
    """Generates Oozie workflow file.

    Parameters
    ----------
    workflow_definition: str
        Workflow definition file
    workflow_configuration: str
        Workflow configuration file

    """
    # Create output directory
    Path(output_directory).mkdir(exist_ok=True)
    # Generate Oozie Workflow
    workflow_data = File.read(workflow_definition)
    config_data = File.read(workflow_configuration)
    workflow = Workflow.build(workflow_data, config_data)

    try:
        workflow_graph = WorkflowGraphBuilder(workflow).build_workflow_graph(show_graph)
    except Exception as e:
        logging.exception(f"Unable to generate workflow\n{e}")

    OozieWorkflow(workflow_graph, workflow.name).generate(output_directory)