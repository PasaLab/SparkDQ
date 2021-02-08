from sparkdq.analytics.Analyzer import ComplexAnalyzer, GroupingAnalyzer, ScanShareableAnalyzer
from sparkdq.analytics.analyzers.ApproxQuantile import ApproxQuantile
from sparkdq.analytics.analyzers.Size import Size
from sparkdq.analytics.GroupingAnalyzers import FrequencyBasedAnalyzer, ScanShareableFrequencyBasedAnalyzer
from sparkdq.analytics.Preconditions import find_first_failing
from sparkdq.analytics.runners.AnalyzerContext import AnalyzerContext
from sparkdq.analytics.runners.OutputOptions import OutputOptions
from sparkdq.analytics.runners.RepositoryOptions import RepositoryOptions
from sparkdq.analytics.runners.StateProviderOptions import StateProviderOptions
from sparkdq.exceptions.AnalysisExceptions import AnalysisException, \
    AnalysisRuntimeException, UnknownAnalyzerException
from sparkdq.io.hdfs import write_file_on_hdfs


class AnalysisRunner:
    """
    Main processes for all analyzers
    """
    @staticmethod
    def on_data(data):
        from sparkdq.analytics.runners.AnalysisRunBuilder import AnalysisRunBuilder
        return AnalysisRunBuilder(data)

    @staticmethod
    def do_analysis_run(data, analyzers, state_provider_options=StateProviderOptions(),
                        metrics_repository_options=RepositoryOptions(), output_options=OutputOptions()):
        """
        Execution of analyzers on the data
        :param data: DataFrame, source data of type DataFrame
        :param analyzers: list of Analyzer, analyzers of different types
        :param state_provider_options: StateProviderOptions, for reusing and caching states
        :param metrics_repository_options: RepositoryOptions, for reusing and saving results in a repository
        :param output_options: OutputOptions, for outputing the result for persistence, output is the simplified version
        of repository and only contains metrics related to this analysis, repository contains many other metrics with
        other keys
        :return: AnalyzerContext, total analysis result
        """
        if len(analyzers) == 0:
            return AnalyzerContext.empty()

        # reuse previously calculated Metrics
        results_calculated_previously = AnalyzerContext.empty()
        if (metrics_repository_options.metrics_repository is not None) and \
                (metrics_repository_options.reuse_existing_result_key is not None):
            results_calculated_previously = metrics_repository_options.metrics_repository.load_by_key(
                metrics_repository_options.reuse_existing_result_key)

        ready_analyzers = list(results_calculated_previously.metric_map.keys())
        upcoming_analyzers = list(filter(lambda x: x not in ready_analyzers, analyzers))

        # 检查准备条件
        passed_analyzers = []
        precondition_failed_metrics_map = {}
        for analyzer in upcoming_analyzers:
            first_exception = find_first_failing(data.schema, analyzer.preconditions())
            if first_exception is None:
                passed_analyzers.append(analyzer)
            else:
                precondition_failed_metrics_map[analyzer] = analyzer.to_failure_metric(first_exception)
        precondition_failed_metrics = AnalyzerContext(precondition_failed_metrics_map)

        # 分类Analyzers
        scan_shareable_analyzers = []
        grouping_analyzers = []
        complex_analyzers = []
        for analyzer in passed_analyzers:
            if isinstance(analyzer, ScanShareableAnalyzer):
                scan_shareable_analyzers.append(analyzer)
            elif isinstance(analyzer, GroupingAnalyzer):
                grouping_analyzers.append(analyzer)
            elif isinstance(analyzer, ComplexAnalyzer):
                complex_analyzers.append(analyzer)
            else:
                raise UnknownAnalyzerException("Unknown analyzer type {} of analyzer {}"
                                               .format(type(analyzer), analyzer))

        # 处理第一类Analyzer： Scan-shareable Analyzer
        scan_shareable_metrics = AnalysisRunner.run_scan_shareable_analyzers(
            data, scan_shareable_analyzers, state_provider_options
        )

        # get number of rows
        state_provider = state_provider_options.state_provider
        size_analyzer = Size()
        if (state_provider is not None) and (state_provider.has_state(size_analyzer)):
            num_rows = state_provider.load(state_provider).num_matches
        else:
            num_rows = data.count()
            if state_provider is not None:
                state_provider.persist(size_analyzer, num_rows)

        # 处理第二类Analyzer： Grouping Analyzer
        grouped_analyzers = AnalysisRunner.group_analyzers_by_columns(grouping_analyzers)
        grouped_metrics = AnalyzerContext.empty()

        for _, current_analyzers in grouped_analyzers.items():
            # 每次执行一组基于相同列分组的任务
            current_grouping_columns = current_analyzers[0].grouping_columns()
            current_metrics = AnalysisRunner.run_grouping_analyzers(data, current_analyzers, current_grouping_columns,
                                                                    num_rows, state_provider_options)
            grouped_metrics = grouped_metrics.merge(current_metrics)

        # 处理第三类Analyzer： Complex Analyzer
        complex_metrics_map = {}
        for analyzer in complex_analyzers:
            complex_metrics_map[analyzer] = analyzer.compute_metric_from_data(
                data, num_rows, state_provider if state_provider_options.reuse_state else None)

        total_analyzer_context = results_calculated_previously.merge(precondition_failed_metrics)\
            .merge(scan_shareable_metrics).merge(grouped_metrics).add(complex_metrics_map)

        # 汇总到Metrics库中
        if (metrics_repository_options.metrics_repository is not None) and \
                (metrics_repository_options.save_result_key is not None):
            repository = metrics_repository_options.metrics_repository
            save_result_key = metrics_repository_options.save_result_key
            current_value = repository.load_by_key(save_result_key)
            new_value = current_value.merge(total_analyzer_context)
            repository.save(save_result_key, new_value)

        # 输出结果文件
        if output_options.file_name is not None:
            write_file_on_hdfs(
                output_options.file_name,
                AnalyzerContext.success_metrics_as_json(total_analyzer_context),
                output_options.over_write
            )

        return total_analyzer_context

    @staticmethod
    def run_scan_shareable_analyzers(data, analyzers, state_provider_options):
        """
        Execution of scan-shareable analyzers by scanning the data set once
        e.g. standard scan-shareable, data type, approx, histogram analyzers
        :param data:                    source data of type DataFrame
        :param analyzers:               scan-shareable analyzers
        :param state_provider_options:  state provider options
        :return: scan-shareable analyzers' analysis result of type AnalysisContext
        """
        if len(analyzers) == 0:
            shareable_results = AnalyzerContext.empty()
        else:
            shareable_metrics = {}

            # 按照column，relative_error，where合并支持多参数的ApproxQuantile模型
            aq_analyzers = {}
            single_analyzers = []
            for analyzer in analyzers:
                if isinstance(analyzer, ApproxQuantile):
                    key = (analyzer.instance, analyzer.relative_error, analyzer.where)
                    if key in aq_analyzers:
                        aq_analyzers[key].append(analyzer)
                    else:
                        aq_analyzers[key] = [analyzer]
                else:
                    single_analyzers.append(analyzer)

            # 预加载states
            target_single_analyzers = single_analyzers
            state_provider = state_provider_options.state_provider
            if (state_provider is not None) and state_provider_options.reuse_state:
                target_single_analyzers = []
                for analyzer in single_analyzers:
                    if state_provider.has_state(analyzer):
                        shareable_metrics[analyzer] = analyzer.compute_metric_from_state(state_provider.load(analyzer))
                    else:
                        target_single_analyzers.append(analyzer)
                if len(aq_analyzers) > 0:
                    for k in list(aq_analyzers.keys()):
                        if state_provider.has_state(aq_analyzers[k][0]):
                            state = state_provider.load(aq_analyzers[k][0])
                            for a in aq_analyzers[k]:
                                shareable_metrics[a] = a.compute_metric_from_state(state)
                            del aq_analyzers[k]

            # 此时target_single_analyzers和aq_analyzers即为实际执行的检测
            try:
                aggregations = []
                offsets = [0]
                current = 0

                # 合并approx quantile任务
                aq_map = {}
                actual_aq_analyzers = []
                for k, v in aq_analyzers.items():
                    actual_aq_analyzers.append(v[0])
                    aq_map[v[0]] = k

                for analyzer in target_single_analyzers + actual_aq_analyzers:
                    funcs = analyzer.aggregation_functions()
                    aggregations.extend(funcs)
                    offsets.append(current + len(funcs))
                    current += len(funcs)
                results = data.agg(*aggregations).collect()[0]

                len1 = len(target_single_analyzers)
                for i in range(len1):
                    shareable_metrics[target_single_analyzers[i]] = AnalysisRunner.success_or_failure_metric_from(
                        target_single_analyzers[i], results, offsets[i], state_provider)
                for i in range(len(actual_aq_analyzers)):
                    key = aq_map[actual_aq_analyzers[i]]
                    for a in aq_analyzers[key]:
                        shareable_metrics[a] = AnalysisRunner.success_or_failure_metric_from(
                            a, results, offsets[i+len1], state_provider)

            except Exception as exception:
                for analyzer in target_single_analyzers:
                    shareable_metrics[analyzer] = analyzer.to_failure_metric(
                        AnalysisException.wrap_if_necessary(exception))
                for k, v in aq_analyzers.items():
                    for a in v:
                        shareable_metrics[a] = a.to_failure_metric(AnalysisException.wrap_if_necessary(exception))
            shareable_results = AnalyzerContext(shareable_metrics)
        return shareable_results

    @staticmethod
    def run_grouping_analyzers(data, analyzers, grouping_columns, num_rows, state_provider_options):
        """
        Execution of grouping analyzers sharing common grouping columns
        :param data: source data of type DataFrame
        :param analyzers: grouping analyzers sharing common grouping columns
        :param grouping_columns: common grouping columns
        :param num_rows: number of rows
        :param state_provider_options
        :return:
        """
        # load frequencies state
        state_provider = state_provider_options.state_provider
        if (state_provider is not None) and state_provider_options.reuse_state and \
                (state_provider.has_state(analyzers[0])):
            frequencies_num_rows = state_provider.load(analyzers[0])
        else:
            frequencies_num_rows = FrequencyBasedAnalyzer.compute_frequencies(data, grouping_columns, num_rows)
            if state_provider is not None:
                state_provider.persist(analyzers[0], frequencies_num_rows)

        # shareable analyzers and others
        shareable_analyzers = []
        other_analyzers = []
        for analyzer in analyzers:
            if isinstance(analyzer, ScanShareableFrequencyBasedAnalyzer):
                shareable_analyzers.append(analyzer)
            else:
                other_analyzers.append(analyzer)

        # 缓存df
        if len(other_analyzers) > 0:
            frequencies_num_rows.frequencies.persist()

        # 先计算shareable analyzers
        if len(shareable_analyzers) == 0:
            shareable_results = AnalyzerContext.empty()
        else:
            shareable_metrics = {}
            try:
                aggregations = []
                offsets = [0]
                current = 0
                for analyzer in shareable_analyzers:
                    funcs = analyzer.aggregation_functions(num_rows)
                    aggregations.extend(funcs)
                    offsets.append(current + len(funcs))
                    current += len(funcs)

                results = frequencies_num_rows.frequencies.agg(*aggregations).collect()[0]
                for i in range(len(shareable_analyzers)):
                    shareable_metrics[shareable_analyzers[i]] = AnalysisRunner.success_or_failure_metric_from(
                        shareable_analyzers[i], results, offsets[i])
            except Exception as error:
                shareable_metrics.clear()
                for analyzer in shareable_analyzers:
                    shareable_metrics[analyzer] = analyzer.to_failure_metric(
                        AnalysisException.wrap_if_necessary(error))
            shareable_results = AnalyzerContext(shareable_metrics)

        other_metrics = {}
        for other_analyzer in other_analyzers:
            try:
                other_metrics[other_analyzer] = other_analyzer.compute_metric_from_state(frequencies_num_rows)
            except Exception as error:
                other_metrics[other_analyzer] = other_analyzer.to_failure_metric(
                    AnalysisException.wrap_if_necessary(error))

        frequencies_num_rows.frequencies.unpersist()
        return shareable_results.add(other_metrics)

    @staticmethod
    def success_or_failure_metric_from(analyzer, aggregation_result, offset, state_provider=None):
        """
        Generate metric from aggregation result and offset
        :param analyzer: analyzer
        :param aggregation_result: the whole aggregation result
        :param offset: offset of the analyzer in the aggregation result
        :param state_provider: for persisting states
        :return: target metric, success or failure
        """
        try:
            if isinstance(analyzer, ScanShareableAnalyzer):
                return analyzer.compute_metric_from_aggregation_result(aggregation_result, offset, state_provider)
            elif isinstance(analyzer, ScanShareableFrequencyBasedAnalyzer):
                return analyzer.compute_metric_from_aggregation_result(aggregation_result, offset)
            else:
                raise AnalysisRuntimeException("Invalid analyzer {} of type{}.".format(analyzer,
                                                                                       type(analyzer)))
        except Exception as exp:
            return analyzer.to_failure_metric(AnalysisException.wrap_if_necessary(exp))

    @staticmethod
    def group_analyzers_by_columns(analyzers):
        """
        Group analyzers by common grouping columns
        :param analyzers: analyzers for grouping
        :return: grouped analyzers of type Dict, key is a string of grouping columns, value is a list of analyzers
        """
        grouped_analyzers = {}
        for analyzer in analyzers:
            grouping_columns = ",".join(analyzer.grouping_columns())
            if grouping_columns in grouped_analyzers:
                grouped_analyzers[grouping_columns].append(analyzer)
            else:
                grouped_analyzers[grouping_columns] = [analyzer]
        return grouped_analyzers
