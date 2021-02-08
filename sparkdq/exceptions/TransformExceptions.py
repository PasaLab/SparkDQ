class TransformException(Exception):
    """
    Transform exception happens in the process of transform, can be divided into TransformPreconditionException and
    TransformRuntimeException.
    """
    @staticmethod
    def wrap_if_necessary(exception):
        """
        Wrap the exception caught when executing Spark jobs as AnalysisCalculationException
        """
        if isinstance(exception, TransformException):
            return exception
        return


class TransformPreconditionException(TransformException):
    """
    Category 1: Transform precondition exception happens in the process of checking preconditions,
    preconditions exceptions are almost common.
    """
    pass


class IllegalTransformerParameterException(TransformPreconditionException):
    pass


class TransformRuntimeException(TransformException):
    """
    Category 2: Transform runtime exception happens in the process of executing transform jobs,
    including calculation and other exceptions.
    """
    pass


class TransformCalculationException(TransformRuntimeException):
    pass


class UnknownTransformerException(TransformRuntimeException):
    pass
