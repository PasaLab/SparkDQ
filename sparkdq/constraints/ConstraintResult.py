class ConstraintResult:
    """
    Result of constraint

    Args:
        constraint: the constraint
        status: status of the constraint
        message: None if success, else the exception information
        metric: the metric if it exists, otherwise None
    """
    def __init__(self, constraint, status, message=None, metric=None):
        self.constraint = constraint
        self.status = status
        self.message = message
        self.metric = metric

    def __str__(self):
        return "Constraint: {}, Status: {}, Message: {}, Metric: {}".format(self.constraint, self.status, self.message,
                                                                            self.metric)
