from enum import Enum

from pyspark.ml.wrapper import JavaModel, JavaParams

# scala models package path
DQLIB_MODELS_PATH = "pasa.bigdata.dqlib.models.{}"

# default column name
DEFAULT_INDEX_COL = "id"
DEFAULT_FEATURES_COL = "features"
DEFAULT_NORMALIZED_FEATURES_COL = "normalized_features"
DEFAULT_STANDARDIZED_FEATURES_COL = "standardized_features"
DEFAULT_CLUSTER_COL = "cluster"
DEFAULT_ANOMALY_SCORE_COL = "anomaly_score"
DEFAULT_DISTANCE_COL = "distance"
DEFAULT_PREDICTION_COL = "prediction"

DEFAULT_SEPARATOR = ","


class TaskType(Enum):
    DETECT = "detect"
    REPAIR = "repair"


# def customized_from_java(java_stage):
#     """
#     Copied from pyspark.ml.wrapper, in order to modify the pyspark pkg name
#     Given a Java object, create and return a Python wrapper of it.
#     Used for ML persistence.
#     Meta-algorithms such as Pipeline should override this method as a classmethod.
#     """
#
#     def __get_class(clazz):
#         """
#         Loads Python class from its name.
#         """
#         parts = clazz.split('.')
#         module = ".".join(parts[:-1])
#         m = __import__(module)
#         for comp in parts[1:]:
#             m = getattr(m, comp)
#         return m
#
#     def _create_params_from_java(stage):
#         """
#         SPARK-10931: Temporary fix to create params that are defined in the Java obj but not here
#         """
#         java_params = stage._java_obj.params()
#         from pyspark.ml.param import Param
#         for java_param in java_params:
#             java_param_name = java_param.name()
#             if not hasattr(stage, java_param_name):
#                 param = Param(stage, java_param_name, java_param.doc())
#                 setattr(param, "created_from_java_param", True)
#                 setattr(stage, java_param_name, param)
#                 stage._params = None
#
#         # Change pyspark pkg name to qualitydetector
#         stage_name = java_stage.getClass().getName().replace("pasa.bigdata.dqlib", "sparkdq")
#         # Generate a default new instance from the stage_name class.
#         py_type = __get_class(stage_name)
#         if issubclass(py_type, JavaParams):
#             # Load information from java_stage to the instance.
#             py_stage = py_type()
#             py_stage._java_obj = java_stage
#
#             # SPARK-10931: Temporary fix so that persisted models would own params from Estimator
#             if issubclass(py_type, JavaModel):
#                 _create_params_from_java(py_stage)
#
#             py_stage._resetUid(java_stage.uid())
#             py_stage._transfer_params_from_java()
#         elif hasattr(py_type, "_from_java"):
#             py_stage = py_type._from_java(java_stage)
#         else:
#             raise NotImplementedError("This Java stage cannot be loaded into Python currently: %r"
#                                       % stage_name)
#         return py_stage
