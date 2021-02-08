from pyspark import keyword_only
from pyspark.ml.param.shared import *
from pyspark.ml.wrapper import JavaModel, JavaEstimator, JavaWrapper

from sparkdq.models.CommonUtils import DEFAULT_FEATURES_COL, DEFAULT_ANOMALY_SCORE_COL, DQLIB_MODELS_PATH


__all__ = ['IForest', 'IForestModel', 'IForestSummary']


class IForest(JavaEstimator):

    columns = Param(parent=Params._dummy(), name="columns", doc="columns of all features",
                    typeConverter=TypeConverters.toListString)
    numRows = Param(parent=Params._dummy(), name="numRows", doc="number of total rows",
                    typeConverter=TypeConverters.toInt)
    featuresCol = Param(parent=Params._dummy(), name="featuresCol", doc="column of features vector",
                        typeConverter=TypeConverters.toString)
    anomalyScoreCol = Param(parent=Params._dummy(), name="anomalyScoreCol", doc="column of anomaly score",
                            typeConverter=TypeConverters.toString)
    numTrees = Param(parent=Params._dummy(), name="numTrees", doc="number of trees", typeConverter=TypeConverters.toInt)
    maxSamples = Param(parent=Params._dummy(), name="maxSamples", doc="number of samples for each tree",
                       typeConverter=TypeConverters.toFloat)
    maxFeatures = Param(parent=Params._dummy(), name="maxFeatures", doc="number of features for each tree",
                        typeConverter=TypeConverters.toFloat)
    maxDepth = Param(parent=Params._dummy(), name="maxDepth", doc="height limit while building trees",
                     typeConverter=TypeConverters.toInt)
    bootstrap = Param(parent=Params._dummy(), name="bootstrap", doc="if true, sampling with replacement",
                      typeConverter=TypeConverters.toBoolean)
    seed = Param(parent=Params._dummy(), name="seed", doc="random seed", typeConverter=TypeConverters.toInt)

    @keyword_only
    def __init__(self, columns, numRows=-1, featuresCol=DEFAULT_FEATURES_COL,
                 anomalyScoreCol=DEFAULT_ANOMALY_SCORE_COL, numTrees=100, maxSamples=1.0, maxFeatures=1.0, maxDepth=10,
                 bootstrap=False, seed=24):
        super(IForest, self).__init__()
        self._java_obj = self._new_java_obj(DQLIB_MODELS_PATH.format("iforest.IForest"), self.uid)
        self._setDefault(numRows=-1, featuresCol=DEFAULT_FEATURES_COL, anomalyScoreCol=DEFAULT_ANOMALY_SCORE_COL,
                         numTrees=100, maxSamples=1.0, maxFeatures=1.0, maxDepth=10, bootstrap=False, seed=24)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, columns, numRows=-1, featuresCol=DEFAULT_FEATURES_COL,
                  anomalyScoreCol=DEFAULT_ANOMALY_SCORE_COL, numTrees=100, maxSamples=1.0, maxFeatures=1.0, maxDepth=10,
                  bootstrap=False, seed=24):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return IForestModel(java_model)

    def setColumns(self, value):
        return self._set(columns=value)

    def getColumns(self):
        return self.getOrDefault(self.columns)

    def setNumRows(self, value):
        return self._set(numRows=value)

    def getNumRows(self):
        return self.numRows

    def setFeaturesCol(self, value):
        return self._set(featuresCol=value)

    def getFeaturesCol(self):
        return self.getOrDefault(self.featuresCol)

    def setAnomalyScoreCol(self, value):
        return self._set(anomalyScoreCol=value)

    def getAnomalyScoreCol(self):
        return self.getOrDefault(self.anomalyScoreCol)

    def setNumTrees(self, value):
        return self._set(numTrees=value)

    def getNumTrees(self):
        return self.getOrDefault(self.numTrees)

    def setMaxSamples(self, value):
        return self._set(maxSamples=value)

    def getMaxSamples(self):
        return self.getOrDefault(self.maxSamples)

    def setMaxFeatures(self, value):
        return self._set(maxFeatures=value)

    def getMaxFeatures(self):
        return self.getOrDefault(self.MaxSamples)

    def setMaxDepth(self, value):
        return self._set(maxDepth=value)

    def getMaxDepth(self):
        return self.getOrDefault(self.maxDepth)

    def setBootstrap(self, value):
        return self._set(bootstrap=value)

    def getBootstrap(self):
        return self.getOrDefault(self.bootstrap)

    def setSeed(self, value):
        return self._set(seed=value)

    def getSeed(self):
        return self.getOrDefault(self.seed)


class IForestModel(JavaModel):

    @property
    def hasSummary(self):
        return self._call_java("hasSummary")

    @property
    def summary(self):
        if self.hasSummary:
            return IForestSummary(self._call_java("summary"))
        raise Exception("No training summary available for this %s" % self.__class__.__name__)


class IForestSummary(JavaWrapper):

    @property
    def dataWithScores(self):
        return self._call_java("dataWithScores")

    @property
    def success(self):
        return self._call_java("success")

    @property
    def message(self):
        return self._call_java("message")
