from dataclasses import dataclass, field
from typing import Set, Optional


@dataclass(frozen=True, unsafe_hash=True)
class Action:
    """Represents an Oozie Action"""

    name: str
    action_type: str
    dependencies: Optional[Set[str]] = field(default=None, hash=False)
    config: Optional[dict] = field(default=None, hash=False, repr=False)

    def __post_init__(self):
        assert self.name != ""
        assert self.action_type != ""
