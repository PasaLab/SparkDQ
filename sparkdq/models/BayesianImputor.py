from pyspark import keyword_only
from pyspark.ml.param.shared import *
from pyspark.ml.wrapper import JavaModel, JavaEstimator, JavaWrapper

from sparkdq.models.CommonUtils import DQLIB_MODELS_PATH, DEFAULT_INDEX_COL


__all__ = ["BayesianImputor", "BayesianImputorModel", "BayesianImputorSummary"]


class BayesianImputor(JavaEstimator):

    indexCol = Param(parent=Params._dummy(), name="indexCol", doc="column of index",
                     typeConverter=TypeConverters.toString)
    targetCol = Param(parent=Params._dummy(), name="targetCol", doc="target column for imputing",
                      typeConverter=TypeConverters.toString)
    dependentColumns = Param(parent=Params._dummy(), name="dependentColumns", doc="dependent columns of target column",
                             typeConverter=TypeConverters.toListString)
    allowedValues = Param(parent=Params._dummy(), name="allowedValues", doc="allowed values for imputing",
                          typeConverter=TypeConverters.toListString)

    @keyword_only
    def __init__(self, targetCol, dependentColumns, allowedValues=[], indexCol=DEFAULT_INDEX_COL):
        super(BayesianImputor, self).__init__()
        self._java_obj = self._new_java_obj(DQLIB_MODELS_PATH.format("imputation.BayesianImputor"), self.uid)
        self._setDefault(allowedValues=[], indexCol=DEFAULT_INDEX_COL)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, targetCol, dependentColumns, allowedValues=[], indexCol=DEFAULT_INDEX_COL):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return BayesianImputorModel(java_model)

    def setIndexCol(self, value):
        return self._set(indexCol=value)

    def getIndexCol(self):
        return self.getOrDefault(self.indexCol)

    def setTargetCol(self, value):
        return self._set(targetCol=value)

    def getTargetCol(self):
        return self.getOrDefault(self.targetCol)

    def setDependentColumns(self, value):
        return self._set(dependentColumns=value)

    def getDependentColumns(self):
        return self.getOrDefault(self.dependentColumns)

    def setAllowedValues(self, value):
        return self._set(allowedValues=value)

    def getAllowedValues(self):
        return self.getOrDefault(self.allowedValues)


class BayesianImputorModel(JavaModel):

    @property
    def hasSummary(self):
        return self._call_java("hasSummary")

    @property
    def summary(self):
        if self.hasSummary:
            return BayesianImputorSummary(self._call_java("summary"))
        raise Exception("No training summary available for this %s" % self.__class__.__name__)


class BayesianImputorSummary(JavaWrapper):

    @property
    def imputedData(self):
        return self._call_java("imputedData")

    @property
    def imputeMap(self):
        return self._call_java("imputeMap")

    @property
    def success(self):
        return self._call_java("success")

    @property
    def message(self):
        return self._call_java("message")
