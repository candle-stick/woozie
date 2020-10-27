from dataclasses import dataclass, field
from typing import Optional, Dict, Any
from woozie.domain.io import File
from dpcontracts import require, ensure, types

import json


@dataclass
class Config:
    """Data holder for YAML Oozie configuration file.
    Holds mapping of user-defined actions to Oozie actions
    """

    action_types: Dict[str, Dict[str, Any]]

    @staticmethod
    @require(
        "Configuration file must have at least one action-type",
        lambda args: len([*args.action_types]) >= 1,
    )
    @require(
        "Configuration file must have valid action-type names",
        lambda args: all(
            isinstance(action_type, str) for action_type in [*args.action_types]
        )
        and not any(action_type == "" for action_type in [*args.action_types]),
    )
    @types(action_types=dict)
    def build(action_types: Dict[str, dict], **kwargs) -> "Config":
        return Config(action_types=action_types)

    @types(action_type=str)
    @require(
        "User specified action type must be in configuration file",
        lambda args: args.action_type in [*args.self.action_types],
    )
    def fetch(self, action_type: str) -> "Config":
        """Fetch specific action configuration from configuration file.

        Config.action_types reduced to single action_type requested.
        """
        action_dict = {action_type: self.action_types.get(action_type)}
        return Config(action_types=action_dict)

    @require(
        "Workflow definition parameters must be dict",
        lambda args: isinstance(args.parameters, dict),
    )
    @ensure("Must return a dict", lambda args, result: isinstance(result, dict))
    def interpolate(self, parameters: dict) -> dict:

        # Convert action configuration to string from dict
        text = [json.dumps(config) for name, config in self.action_types.items()].pop()

        # Inject placeholders into action configuration and convert back to dict
        for placeholder, replacement in parameters.items():
            text = text.replace(f"{{{{ {placeholder} }}}}", replacement)
        return json.loads(text)
