from pyspark import keyword_only
from pyspark.ml.param.shared import *
from pyspark.ml.wrapper import JavaModel, JavaEstimator, JavaWrapper

from sparkdq.models.CommonUtils import DEFAULT_INDEX_COL, DEFAULT_FEATURES_COL, DEFAULT_ANOMALY_SCORE_COL, \
    DQLIB_MODELS_PATH


__all__ = ["LOF", "LOFModel", "LOFSummary"]


class LOF(JavaEstimator):

    columns = Param(parent=Params._dummy(), name="columns", doc="columns of all features",
                    typeConverter=TypeConverters.toListString)

    indexCol = Param(parent=Params._dummy(), name="indexCol", doc="column of index",
                     typeConverter=TypeConverters.toString)

    featuresCol = Param(parent=Params._dummy(), name="featuresCol", doc="column of features vector",
                        typeConverter=TypeConverters.toString)

    lofCol = Param(parent=Params._dummy(), name="lofCol", doc="column of lof", typeConverter=TypeConverters.toString)

    minPts = Param(parent=Params._dummy(), name="minPts", doc="minimum number of points",
                   typeConverter=TypeConverters.toInt)

    distType = Param(parent=Params._dummy(), name="distType", doc="distance type",
                     typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self, columns, indexCol=DEFAULT_INDEX_COL, featuresCol=DEFAULT_FEATURES_COL,
                 lofCol=DEFAULT_ANOMALY_SCORE_COL, minPts=5, distType="euclidean"):
        super(LOF, self).__init__()
        self._java_obj = self._new_java_obj(DQLIB_MODELS_PATH.format("LOF"), self.uid)
        self._setDefault(indexCol=DEFAULT_INDEX_COL, featuresCol=DEFAULT_FEATURES_COL, lofCol=DEFAULT_ANOMALY_SCORE_COL,
                         minPts=5, distType="euclidean")
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, columns, indexCol=DEFAULT_INDEX_COL, featuresCol=DEFAULT_FEATURES_COL,
                  lofCol=DEFAULT_ANOMALY_SCORE_COL, minPts=5, distType="euclidean"):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return LOFModel(java_model)

    def setColumns(self, value):
        return self._set(columns=value)

    def getColumns(self):
        return self.getOrDefault(self.columns)

    def setIndexCol(self, value):
        return self._set(indexCol=value)

    def getIndexCol(self):
        return self.getOrDefault(self.indexCol)

    def setFeaturesCol(self, value):
        return self._set(featuresCol=value)

    def getFeaturesCol(self):
        return self.getOrDefault(self.featuresCol)

    def setLofCol(self, value):
        return self._set(lofCol=value)

    def getLofCol(self):
        return self.getOrDefault(self.lofCol)

    def setMinPts(self, value):
        return self._set(minPts=value)

    def getMinPts(self):
        return self.getOrDefault(self.minPts)

    def setDistType(self, value):
        return self._set(distType=value)

    def getDistType(self):
        return self.getOrDefault(self.distType)


class LOFModel(JavaModel):

    @property
    def hasSummary(self):
        return self._call_java("hasSummary")

    @property
    def summary(self):
        if self.hasSummary:
            return LOFSummary(self._call_java("summary"))
        raise Exception("No training summary available for this %s" % self.__class__.__name__)


class LOFSummary(JavaWrapper):

    @property
    def dataWithScores(self):
        return self._call_java("dataWithScores")

    @property
    def success(self):
        return self._call_java("success")

    @property
    def message(self):
        return self._call_java("message")
