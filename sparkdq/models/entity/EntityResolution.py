from pyspark import keyword_only
from pyspark.ml.wrapper import JavaModel, JavaEstimator, JavaWrapper
from pyspark.ml.param.shared import *

from sparkdq.models.entity.ComparisonType import ComparisonType
from sparkdq.models.entity.SimilarityType import SimilarityType
from sparkdq.models.entity.ThresholdType import ThresholdType
from sparkdq.models.entity.WeightType import WeightType
from sparkdq.models.CommonUtils import DEFAULT_INDEX_COL, DQLIB_MODELS_PATH, TaskType, DEFAULT_CLUSTER_COL


__all__ = ['EntityResolutionSummary', 'EntityResolution', 'EntityResolutionModel']


class EntityResolution(JavaEstimator):

    columns = Param(parent=Params._dummy(), name="columns", doc="all related columns",
                    typeConverter=TypeConverters.toListString)

    indexCol = Param(parent=Params._dummy(), name="indexCol", doc="index column", typeConverter=TypeConverters.toString)

    taskType = Param(parent=Params._dummy(), name="taskType", doc="task type, can be detect and repair",
                     typeConverter=TypeConverters.toString)

    numHashes = Param(parent=Params._dummy(), name="numHashes", doc="number of hashes",
                      typeConverter=TypeConverters.toInt)

    targetThreshold = Param(parent=Params._dummy(), name="targetThreshold", doc="target threshold",
                            typeConverter=TypeConverters.toFloat)

    maxFactor = Param(parent=Params._dummy(), name="maxFactor", doc="maximum factor",
                      typeConverter=TypeConverters.toFloat)

    numBands = Param(parent=Params._dummy(), name="numBands", doc="number of bands", typeConverter=TypeConverters.toInt)

    smoothFactor = Param(parent=Params._dummy(), name="smoothFactor", doc="smooth factor",
                         typeConverter=TypeConverters.toFloat)

    keepRate = Param(parent=Params._dummy(), name="keepRate", doc="keep rate", typeConverter=TypeConverters.toFloat)

    thresholdType = Param(parent=Params._dummy(), name="thresholdType", doc="threshold type, can be avg and maxdiv2",
                          typeConverter=TypeConverters.toString)

    weightType = Param(parent=Params._dummy(), name="weightType", doc="weight type, can be cbs and chiSquare",
                       typeConverter=TypeConverters.toString)

    comparisonType = Param(parent=Params._dummy(), name="comparisonType", doc="comparison type, can be or, and",
                           typeConverter=TypeConverters.toString)

    chi2divider = Param(parent=Params._dummy(), name="chi2divider", doc="chi2divider",
                        typeConverter=TypeConverters.toFloat)

    similarityType = Param(parent=Params._dummy(), name="similarityType",
                           doc="similarity type, can be cosine, jaro-winkler, jaccrad index, normalized levenshtein, "
                               "sorensen-dice coefficient", typeConverter=TypeConverters.toString)

    similarityThreshold = Param(parent=Params._dummy(), name="similarityThreshold", doc="similarity threshold",
                                typeConverter=TypeConverters.toFloat)

    clusterCol = Param(parent=Params._dummy(), name="clusterCol", doc="cluster column",
                       typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self, columns, indexCol=DEFAULT_INDEX_COL, taskType=TaskType.DETECT.value, numHashes=128,
                 targetThreshold=0.3, maxFactor=1.0, numBands=-1, smoothFactor=1.015, keepRate=0.8,
                 thresholdType=ThresholdType.AVG.value, weightType=WeightType.CBS.value,
                 comparisonType=ComparisonType.OR.value, chi2divider=2.0, similarityType=SimilarityType.COSINE.value,
                 similarityThreshold=0.5, clusterCol=DEFAULT_CLUSTER_COL):
        super(EntityResolution, self).__init__()
        self._java_obj = self._new_java_obj(DQLIB_MODELS_PATH.format("entities.EntityResolution"), self.uid)
        self._setDefault(indexCol=DEFAULT_INDEX_COL, taskType=TaskType.DETECT.value, numHashes=128,
                         targetThreshold=0.3, maxFactor=1.0, numBands=-1, smoothFactor=1.015, keepRate=0.8,
                         thresholdType=ThresholdType.AVG.value, weightType=WeightType.CBS.value,
                         comparisonType=ComparisonType.OR.value, chi2divider=2.0,
                         similarityType=SimilarityType.COSINE.value, similarityThreshold=0.5,
                         clusterCol=DEFAULT_CLUSTER_COL)
        kwargs = self._input_kwargs
        self._set(**kwargs)

    @keyword_only
    def setParams(self, columns, indexCol=DEFAULT_INDEX_COL, taskType=TaskType.DETECT.value, numHashes=128,
                 targetThreshold=0.3, maxFactor=1.0, numBands=-1, smoothFactor=1.015, keepRate=0.8,
                 thresholdType=ThresholdType.AVG.value, weightType=WeightType.CBS.value,
                 comparisonType=ComparisonType.OR.value, chi2divider=2.0, similarityType=SimilarityType.COSINE.value,
                 similarityThreshold=0.5, clusterCol=DEFAULT_CLUSTER_COL):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return EntityResolutionModel(java_model)

    def setColumns(self, value):
        return self._set(columns=value)

    def getColumns(self):
        return self.getOrDefault(self.columns)

    def setIndexCol(self, value):
        return self._set(indexCol=value)

    def getIndexCol(self):
        return self.getOrDefault(self.indexCol)

    def setTaskType(self, value):
        return self._set(taskType=value)

    def getTaskType(self):
        return self.getOrDefault(self.taskType)

    def setNumHashes(self, value):
        return self._set(numHashes=value)

    def getNumHashes(self):
        return self.getOrDefault(self.numHashes)

    def setTargetThreshold(self, value):
        return self._set(targetThreshold=value)

    def getTargetThreshold(self):
        return self.getOrDefault(self.targetThreshold)

    def setMaxFactor(self, value):
        return self._set(maxFactor=value)

    def getMaxFactor(self):
        return self.getOrDefault(self.maxFactor)

    def setNumBands(self, value):
        return self._set(numBands=value)

    def getNumBands(self):
        return self.getOrDefault(self.numBands)

    def setSmoothFactor(self, value):
        return self._set(smoothFactor=value)

    def getSmoothFactor(self):
        return self.getOrDefault(self.smoothFactor)

    def setKeepRate(self, value):
        return self._set(keepRate=value)

    def getKeepRate(self):
        return self.getOrDefault(self.keepRate)

    def setThresholdType(self, value):
        return self._set(thresholdType=value)

    def getThresholdType(self):
        return self.getOrDefault(self.thresholdType)

    def setWeightType(self, value):
        return self._set(weightType=value)

    def getWeightType(self):
        return self.getOrDefault(self.weightType)

    def setComparisonType(self, value):
        return self._set(comparisonType=value)

    def getComparisonType(self):
        return self.getOrDefault(self.comparisonType)

    def setChi2Divider(self, value):
        return self._set(chi2divider=value)

    def getChi2Divider(self):
        return self.getOrDefault(self.chi2divider)

    def setSimilarityType(self, value):
        return self._set(similarityType=value)

    def getSimilarityType(self):
        return self.getOrDefault(self.similarityType)

    def setSimilarityThreshold(self, value):
        return self._set(similarityThreshold=value)

    def getSimilarityThreshold(self):
        return self.getOrDefault(self.similarityThreshold)

    def setClusterCol(self, value):
        return self._set(clusterCol=value)

    def getClusterCol(self):
        return self.getOrDefault(self.cluster_col)


class EntityResolutionModel(JavaModel):

    @property
    def hasSummary(self):
        """
        Indicate whether a summary exists for this model.
        :return:
        """
        return self._call_java("hasSummary")

    @property
    def summary(self):
        if self.hasSummary:
            return EntityResolutionSummary(self._call_java("summary"))
        raise Exception("No training summary available for this %s" % self.__class__.__name__)


class EntityResolutionSummary(JavaWrapper):
    """
    Detector results for DupDetector Model.
    """
    @property
    def targetData(self):
        return self._call_java("targetData")

    @property
    def numOfEntities(self):
        return self._call_java("numOfEntities")

    @property
    def success(self):
        return self._call_java("success")

    @property
    def message(self):
        return self._call_java("message")
