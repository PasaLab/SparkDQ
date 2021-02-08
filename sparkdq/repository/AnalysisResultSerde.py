import json

from sparkdq.repository.AnalysisResult import AnalysisResult


class AnalysisResultSerde:

    @staticmethod
    def serialize(analysis_results):
        """
        Serialize analysis results to json String
        :param analysis_results: the analysis results of type AnalysisResult
        :return:
        """
        return json.dumps(list(map(lambda x: x.to_json(), analysis_results)))

    @staticmethod
    def deserialize(analysis_results_str):
        """
        Deserialize results string to analysis results of type AnalysisResult
        :param analysis_results_str: the serialized analysis results of type String
        :return:
        """
        analysis_results = []
        for result_map in json.loads(analysis_results_str):
            analysis_results.append(AnalysisResult.from_json(result_map))
        return analysis_results
