class WorkflowGraphError(Exception):
    """Exception raised for errors during workflow graph construction.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message

class XMLBuildError(Exception):
    """Exception raised for errors during XML build from workflow graph.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message