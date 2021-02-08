from sparkdq.analytics.runners.AnalyzerContext import AnalyzerContext
from sparkdq.io.hdfs import check_exist_for_hdfs, delete_file_on_hdfs, rename_file_on_hdfs, write_file_on_hdfs
from sparkdq.io.local import check_exist_for_local, delete_file_on_local, rename_file_on_local, write_file_to_local
from sparkdq.repository.AnalysisResult import AnalysisResult
from sparkdq.repository.AnalysisResultSerde import AnalysisResultSerde
from sparkdq.repository.FileSystemMetricsRepositoryLoader import FileSystemMetricsRepositoryLoader
from sparkdq.repository.MetricsRepository import MetricsRepository
from sparkdq.structures.FileSystem import FileSystem


class FileSystemMetricsRepository(MetricsRepository):

    def __init__(self, system, path):
        self.system = system
        self.path = path

    def save(self, result_key, analyzer_context):
        successful_metrics = {k: v for k, v in analyzer_context.metric_map.items() if v.is_success}
        analyzer_context_with_successful_metrics = AnalyzerContext(successful_metrics)

        analysis_results = [AnalysisResult(result_key, analyzer_context_with_successful_metrics)]
        serialized_results = AnalysisResultSerde.serialize(analysis_results)
        encoded_bytes = serialized_results.encode("utf-8")
        tmp_path = self.path + ".tmp"
        if self.system is FileSystem.HDFS:
            write_file_on_hdfs(tmp_path, encoded_bytes, False)
            if check_exist_for_hdfs(self.path):
                delete_file_on_hdfs(self.path)
            rename_file_on_hdfs(tmp_path, self.path)
        elif self.system is FileSystem.LOCAL:
            write_file_to_local(tmp_path, encoded_bytes)
            if check_exist_for_local(self.path):
                delete_file_on_local(self.path)
            rename_file_on_local(tmp_path, self.path)

    def load_by_key(self, result_key):
        analysis_results = self.load().get()
        for analysis_result in analysis_results:
            if analysis_result.result_key == result_key:
                return analysis_result.analyzer_context
        return AnalyzerContext.empty()

    def load(self):
        return FileSystemMetricsRepositoryLoader(self.system, self.path)
