from dataclasses import dataclass
from typing import List, Tuple, Optional, Dict
from functools import partial
from dpcontracts import types, ensure, require
from woozie.domain.action import Action
from woozie.domain.workflowdefinition import WorkflowDefinition
from woozie.domain.config import Config


@dataclass
class Workflow:
    """Defines how to construct a Workflow object from YAML"""

    name: Optional[str] = None
    actions: Optional[List[Action]] = None
    error_handler: Optional[Action] = None

    @staticmethod
    @types(workflow_data=dict, config_data=dict)
    @ensure(
        "Must return a Workflow object",
        lambda args, result: isinstance(result, Workflow),
    )
    def build(workflow_data: dict, config_data: dict) -> "Workflow":
        workflow = WorkflowDefinition.build(**workflow_data)
        config = Config.build(**config_data)

        # Extract Actions from workflow definition and configuration file
        create = partial(Workflow.compose_action, config)
        actions = [
            create((name, definition))
            for name, definition in workflow.actions.items()
            if name != "error_handler"
        ]

        # Extract error-handler Action
        error_handler = create(("error_handler", workflow.actions.get("error_handler")))
        return Workflow(workflow.name, actions, error_handler)

    @staticmethod
    @ensure(
        "Each user defined action-type must match ones defined in Config.yaml",
        lambda args, result: result.config is not None,
    )
    @ensure(
        "A valid Action Class is returned",
        lambda args, result: isinstance(result, Action),
    )
    def compose_action(config: Dict[str, dict], action: Tuple[str, dict]) -> Action:
        name, definition = action

        # Build Action objects with type specific settings from config file
        action_dep = definition.get("dependencies")
        action_type = definition.get("type")
        parameters = definition.get("parameters")
        action_config = config.fetch(action_type).interpolate(parameters)
        return Action(name, action_type, action_dep, action_config)
