from dataclasses import dataclass, field
from typing import List, Set, Optional


@dataclass(frozen=True)
class Action:
    """Represents an Oozie Action
    """
    name: str
    action_type: str
    dependencies: Optional[Set[str]] = field(default=None, hash=False)
    config: Optional[dict] = field(default=None, hash=False, repr=False)