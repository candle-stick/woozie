from dataclasses import dataclass
import yaml


@dataclass
class File:
    """Utility class for reading and writing files"""

    filepath: str

    @staticmethod
    def read(filepath):
        with open(filepath, "r") as f:
            data = yaml.safe_load(f)
        return data
