from sparkdq.constraints.Constraint import Constraint
from sparkdq.constraints.ConstraintResult import ConstraintResult
from sparkdq.constraints.ConstraintStatus import ConstraintStatus
from sparkdq.exceptions.AnalysisExceptions import ConstraintAssertionException, ValuePickException


class AnalysisBasedConstraint(Constraint):

    def __init__(self, name, analyzer, assertion, value_picker=None):
        self.name = name
        self.analyzer = analyzer
        self.assertion = assertion
        self.value_picker = value_picker

    def __str__(self):
        return "Name: {}, Analyzer: {}, Assertion: {}, ValuePicker: {}".format(self.name, self.analyzer, self.assertion,
                                                                               self.value_picker)

    def evaluate(self, analysis_result):
        if self.analyzer in analysis_result:
            metric = analysis_result[self.analyzer]
            return self.pick_value_and_assert(metric)
        else:
            return ConstraintResult(self, ConstraintStatus.Failure,
                                    message="Missing Analysis, can't run the constraint!")

    def pick_value_and_assert(self, metric):
        value = metric.value
        if isinstance(value, Exception):
            return ConstraintResult(self, ConstraintStatus.Failure, message=str(value), metric=metric)
        else:
            try:
                assert_on = self.run_picker_on_metric(value)
                assertion_ok = self.run_assertion(assert_on)
                if assertion_ok:
                    return ConstraintResult(self, ConstraintStatus.Success, metric=metric)
                else:
                    error_message = "Value: {} does not meet the constraint requirement!".format(assert_on)
                    return ConstraintResult(self, ConstraintStatus.Failure, message=error_message, metric=metric)
            except ValuePickException as vpe:
                return ConstraintResult(self, ConstraintStatus.Failure,
                                        message="{}: {}!".format("Can't retrieve the value to assert on", str(vpe)),
                                        metric=metric)
            except ConstraintAssertionException as cae:
                return ConstraintResult(self, ConstraintStatus.Failure,
                                        message="{}: {}!".format("Can't execute the assertion", str(cae)),
                                        metric=metric)

    def run_picker_on_metric(self, value):
        try:
            if self.value_picker is None:
                return value
            else:
                return self.value_picker(value)
        except Exception as e:
            raise ValuePickException(e)

    def run_assertion(self, assertion_on):
        try:
            return self.assertion(assertion_on)
        except Exception as e:
            raise ConstraintAssertionException(e)
