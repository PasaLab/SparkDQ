class CheckResult:
    """
    Result of Check

    Args:
        check: the Check
        status: status of the Check
        constraint_results: all the results of constraints
    """
    def __init__(self, check, status, constraint_results):
        self.check = check
        self.status = status
        self.constraint_results = constraint_results
