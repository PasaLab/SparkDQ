from sparkdq.exceptions.AnalysisExceptions import AnalysisPreconditionException, AnalysisCalculationException, \
    AnalysisRuntimeException
from sparkdq.exceptions.TransformExceptions import TransformPreconditionException, TransformCalculationException, \
    TransformRuntimeException


class UnknownStateException(AnalysisRuntimeException, TransformRuntimeException):
    pass


class UnsupportedOutlierModelException(AnalysisPreconditionException, TransformPreconditionException):
    pass


class NoSuchColumnException(AnalysisPreconditionException, TransformPreconditionException):
    pass


class MismatchColumnTypeException(AnalysisPreconditionException, TransformPreconditionException):
    pass


class InvalidFDException(AnalysisPreconditionException, TransformPreconditionException):
    pass


class NoColumnsSpecifiedException(AnalysisPreconditionException, TransformPreconditionException):
    pass


class NumberOfSpecifiedColumnsException(AnalysisPreconditionException, TransformPreconditionException):
    pass


class InconsistentParametersException(AnalysisPreconditionException, TransformPreconditionException):
    pass


class NoSummaryException(AnalysisCalculationException, TransformCalculationException):
    pass
