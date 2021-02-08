class AnalysisException(Exception):
    """
    Analysis exception happens in the process of analysis, can be divided into AnalysisPreconditionException and
    AnalysisRuntimeException.
    """
    @staticmethod
    def wrap_if_necessary(exception):
        """
        Wrap the exception caught when executing Spark jobs as AnalysisCalculationException
        """
        if isinstance(exception, AnalysisException):
            return exception
        return AnalysisCalculationException(exception)


class AnalysisPreconditionException(AnalysisException):
    """
    Category 1: Analysis precondition exception happens in the process of checking preconditions,
    preconditions exceptions are almost common.
    """
    pass


class IllegalAnalyzerParameterException(AnalysisPreconditionException):
    pass


class AnalysisRuntimeException(AnalysisException):
    """
    Category 2: Analysis runtime exception happens in the process of executing analysis jobs,
    including calculation and other exceptions.
    """
    pass


class AnalysisCalculationException(AnalysisRuntimeException):
    pass


class EmptyStateException(AnalysisCalculationException):
    pass


# other analysis runtime exceptions
class UnknownAnalyzerException(AnalysisRuntimeException):
    pass


class ValuePickException(AnalysisRuntimeException):
    pass


class ConstraintAssertionException(AnalysisRuntimeException):
    pass


class AnalyzerSerializationException(AnalysisRuntimeException):
    pass
