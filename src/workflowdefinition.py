from dataclasses import dataclass, field
from typing import Optional, Dict
from src.io import File
from dpcontracts import require, types


@dataclass
class WorkflowDefinition:
    """Data holder for YAML workflow file"""

    name: Optional[str] = None
    actions: Optional[Dict[str, dict]] = None

    @staticmethod
    @require(
        "Workflow data must have a valid name and at least one action",
        lambda args: args.name != ""
        and len([*args.actions]) >= 1
        and all(isinstance(action, str) for action in [*args.actions]),
    )
    @require(
        "Workflow data must have at least one action and valid names",
        lambda args: len([*args.actions]) >= 1
        and all(isinstance(action, str) for action in [*args.actions])
        and not any(action == "" for action in [*args.actions]),
    )
    @types(name=str, actions=dict)
    def build(name: str, actions: Dict[str, dict], **kwargs) -> "WorkflowDefinition":
        return WorkflowDefinition(name, actions)