from pyspark import keyword_only
from pyspark.ml.param.shared import *
from pyspark.ml.wrapper import JavaModel, JavaEstimator, JavaWrapper

from sparkdq.models.CommonUtils import DQLIB_MODELS_PATH, DEFAULT_INDEX_COL, DEFAULT_SEPARATOR, TaskType


__all__ = ["CFDSolver", "CFDSolverModel", "CFDSolverSummary"]


class CFDSolver(JavaEstimator):

    indexCol = Param(parent=Params._dummy(), name="indexCol", doc="column of index",
                     typeConverter=TypeConverters.toString)
    separator = Param(parent=Params._dummy(), name="separator", doc="separator for LSH values",
                      typeConverter=TypeConverters.toString)
    cfds = Param(parent=Params._dummy(), name="cfds",
                 doc="conditional functional dependencies as relation|pat1|pat2...",
                 typeConverter=TypeConverters.toListString)
    priorities = Param(parent=Params._dummy(), name="priorities", doc="priorities of CFDs",
                       typeConverter=TypeConverters.toListInt)
    taskType = Param(parent=Params._dummy(), name="taskType", doc="task type",
                     typeConverter=TypeConverters.toString)
    maxRounds = Param(parent=Params._dummy(), name="maxRounds", doc="maximum repairing rounds")

    @keyword_only
    def __init__(self, cfds, priorities=[], taskType=TaskType.DETECT.value, maxRounds=3, indexCol=DEFAULT_INDEX_COL,
                 separator=DEFAULT_SEPARATOR):
        super(CFDSolver, self).__init__()
        self._java_obj = self._new_java_obj(DQLIB_MODELS_PATH.format("cfd.CFDSolver"), self.uid)
        self._setDefault(priorities=[], taskType=TaskType.DETECT.value, maxRounds=3, indexCol=DEFAULT_INDEX_COL,
                         separator=DEFAULT_SEPARATOR)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, cfds, priorities=[], taskType=TaskType.DETECT.value, maxRounds=3, indexCol=DEFAULT_INDEX_COL,
                  separator=DEFAULT_SEPARATOR):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return CFDSolverModel(java_model)

    def setIndexCol(self, value):
        return self._set(indexCol=value)

    def getIndexCol(self):
        return self.getOrDefault(self.indexCol)

    def setSeparator(self, value):
        return self._set(separator=value)

    def getSeparator(self):
        return self.getOrDefault(self.separator)

    def setCfds(self, value):
        return self._set(cfds=value)

    def getCfds(self):
        return self.getOrDefault(self.cfds)

    def setPriorities(self, value):
        return self._set(priorities=value)

    def getPriorities(self):
        return self.getOrDefault(self.priorities)

    def setTaskType(self, value):
        return self._set(taskType=value)

    def getTaskType(self):
        return self.getOrDefault(self.taskType)

    def setMaxRounds(self, value):
        return self._set(maxRounds=value)

    def getMaxRounds(self):
        return self.getOrDefault(self.maxRounds)


class CFDSolverModel(JavaModel):

    @property
    def hasSummary(self):
        return self._call_java("hasSummary")

    @property
    def summary(self):
        if self.hasSummary:
            return CFDSolverSummary(self._call_java("summary"))
        raise Exception("No training summary available for this %s" % self.__class__.__name__)


class CFDSolverSummary(JavaWrapper):

    @property
    def violations(self):
        return self._call_java("violations")

    @property
    def targetData(self):
        return self._call_java("targetData")

    @property
    def success(self):
        return self._call_java("success")

    @property
    def message(self):
        return self._call_java("message")
