from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple, Optional
from functools import partial
from src.action import Action

import yaml


@dataclass
class Workflow:
    """ Defines how to construct a Workflow object from YAML
    """
    name: Optional[str] = None
    actions: Optional[List[Action]]= None
    error_handler: Optional[Action]= None
        
    def build(self, workflow_file: str, config_file: str) -> Workflow:
        # load workflow file
        with open(workflow_file, 'r') as f:
            workflow = yaml.safe_load(f)
        
        # create workflow object components
        name = workflow['name']
        
        # Extract Actions from workflow definition and configuration file
        create_action = partial(self.get_action, config_file)
        actions = [create_action((name, definition)) 
                   for name, definition in workflow['actions'].items() 
                   if name!='error_handler']
        
        # Extract error-handler Action
        error_handler = create_action(('error_handler', workflow['actions']['error_handler']))
        
        return Workflow(name, actions, error_handler)

    def get_action(self, config_file: str, action: Tuple[str, dict]) -> Action:
        name, a = action
        
        # fill placedholders in configuration file then load into dict
        with open(config_file, 'r') as f:
            text = f.read()
            fill = self.interpolate(text, a['vars'])
            data = yaml.safe_load(fill)
        
        # Build Action objects with action specific settings    
        try: 
            dep = a['dependencies']
            configuration = data['action_types'][a['type']]
        except KeyError as e:
            dep = None
            configuration = None

        return Action(name = name, 
                      action_type = a['type'], 
                      dependencies = dep,
                      config = configuration)           
       
    def interpolate(self, text: str, replacement: dict) -> str:
        for placeholder, value in replacement.items():
            text = text.replace(f'{{{{ {placeholder} }}}}', value)
        return text

