from sparkdq.analytics.runners.AnalyzerContext import AnalyzerContext
from sparkdq.io.hdfs import read_file_from_hdfs
from sparkdq.io.local import read_file_from_local
from sparkdq.repository.AnalysisResult import AnalysisResult
from sparkdq.repository.AnalysisResultSerde import AnalysisResultSerde
from sparkdq.repository.MetricsRepositoryLoader import MetricsRepositoryLoader
from sparkdq.structures.FileSystem import FileSystem
from sparkdq.utils.CollectionsHelper import is_sub_dict


class FileSystemMetricsRepositoryLoader(MetricsRepositoryLoader):

    def __init__(self, system, path):
        self.system = system
        self.path = path
        self.tags = None
        self.analyzers = None
        self.after_time = None
        self.before_time = None

    def with_tag_values(self, tag_values):
        self.tags = tag_values
        return self

    def for_analyzers(self, analyzers):
        self.analyzers = analyzers
        return self

    def after(self, date_time):
        self.after_time = date_time
        return self

    def before(self, date_time):
        self.before_time = date_time
        return self

    def get(self):
        # get the list of analysis results
        if self.system is FileSystem.HDFS:
            content = read_file_from_hdfs(self.path)
        elif self.system is FileSystem.LOCAL:
            content = read_file_from_local(self.path)
        else:
            raise Exception("Unsupported File System {}!".format(self.system))
        all_analysis_results = AnalysisResultSerde.deserialize(content)

        target_analysis_results = []
        for analysis_result in all_analysis_results:
            result_key = analysis_result.result_key
            if ((self.before_time is None) or (result_key.data_set_date <= self.before_time)) and \
                    ((self.after_time is None) or (result_key.data_set_date >= self.after_time)) and \
                    ((self.tags is None) or is_sub_dict(result_key.tags, self.tags)):
                if self.analyzers is None:
                    target_analysis_results.append(analysis_result)
                else:
                    metric_map = analysis_result.analyzer_context.metric_map
                    target_metric_map = {k: v for k, v in metric_map.items() if k in self.analyzers}
                    target_analysis_results.append(AnalysisResult(result_key, AnalyzerContext(target_metric_map)))
        return target_analysis_results
