from pyspark.sql.functions import col

from sparkdq.analytics.Preconditions import find_first_failing
from sparkdq.analytics.runners.StateProviderOptions import StateProviderOptions
from sparkdq.exceptions.TransformExceptions import UnknownTransformerException, TransformCalculationException
from sparkdq.repairs.transformers.filters.Filter import Filter
from sparkdq.repairs.transformers.replacers.Replacer import Replacer
from sparkdq.repairs.transformers.complex_repairers.ComplexRepairer import ComplexRepairer
from sparkdq.repairs.transformers.complex_repairers.FDRepairer import FDRepairer
from sparkdq.repairs.RepairRunBuilder import RepairRunBuilder


class RepairSuite:
    @staticmethod
    def on_data(data):
        return RepairRunBuilder(data)

    @staticmethod
    def do_repair_run(data, transformers, state_provider_options=StateProviderOptions()):
        """
        Do a group of transformation tasks for one Clean
        :param data:                    current data
        :param transformers:            transformers of the Clean
        :param state_provider_options:  for loading and persisting states
        :return:
        """
        # If error repair exists, must stop all the operations, avoid doing meaningless repair and raise the exception
        # to user.
        for transformer in transformers:
            exception = find_first_failing(data.schema, transformer.preconditions())
            if exception is not None:
                raise exception

        filters = []
        replacers = []
        complex_repairers = []

        for transformer in transformers:
            if isinstance(transformer, Filter):
                filters.append(transformer)
            elif isinstance(transformer, Replacer):
                replacers.append(transformer)
            elif isinstance(transformer, ComplexRepairer):
                complex_repairers.append(transformer)
            else:
                raise UnknownTransformerException("Unknown operation type {} of operation {}!"
                                                  .format(type(transformer), transformer))

        tmp_data = data

        # Prepare replacements, prepare these replacements before the whole clean process, the statistics is based on
        # the original data.
        sp = state_provider_options.state_provider
        if len(replacers) > 0:
            unprepared_replacers = []
            if sp is None:
                for replacer in replacers:
                    if replacer.need_preparation():
                        unprepared_replacers.append(replacer)
            else:
                for replacer in replacers:
                    if replacer.need_preparation():
                        if state_provider_options.reuse_state and sp.has_state(replacer.related_analyzer()):
                            state = sp.load(replacer.related_analyzer())
                            replacer.prepare_replacement(Replacer.fetch_replacement_from_state(state))
                        else:
                            unprepared_replacers.append(replacer)
            RepairSuite.prepare_replacements(unprepared_replacers, tmp_data)

        # handle filter operations
        if len(filters) > 0:
            filter_condition = filters[0].filter_condition()
            for f in filters[1:]:
                filter_condition &= f.filter_condition()
            try:
                tmp_data = tmp_data.filter(filter_condition)
            except Exception as e:
                raise TransformCalculationException(e)

        # handle replacer operations
        if len(replacers) > 0:
            replacers_map = {}
            for replacer in replacers:
                c = replacer.column
                if c in replacers_map:
                    replacers_map[c].append(replacer)
                else:
                    replacers_map[c] = [replacer]

            columns = tmp_data.columns
            conditions = []
            for c in columns:
                if c not in replacers_map:
                    # 原数据
                    conditions.append(col(c))
                else:
                    # 合并同列的过滤条件
                    conditions.append(Replacer.merge_conditions(replacers_map[c], c))
            try:
                tmp_data = tmp_data.select(conditions)
            except Exception as e:
                raise TransformCalculationException(e)

        # handle transformer operations
        fd_repairs = []
        for transformer in complex_repairers:
            # 合并支持多参数的FDRepair模型
            if isinstance(transformer, FDRepairer):
                fd_repairs.append(transformer)
                complex_repairers.remove(transformer)
        if len(fd_repairs) > 1:
            cfds = []
            prios = []
            index_col = fd_repairs[0].index_col
            for fd_repair in fd_repairs:
                cfds.extend(fd_repair.cfds)
                prios.extend(fd_repair.priorities)
            new_repair = FDRepairer(cfds, index_col, prios)
            complex_repairers.append(new_repair)
        for transformer in complex_repairers:
            try:
                tmp_data = transformer.transform(tmp_data, sp if state_provider_options.reuse_state else None, False)
            except Exception as e:
                raise TransformCalculationException(e)
        return tmp_data

    @staticmethod
    def prepare_replacements(replacers, data):
        try:
            aggregations = []
            offsets = [0]
            current = 0

            analyzers = [replacer.related_analyzer() for replacer in replacers]
            for analyzer in analyzers:
                funcs = analyzer.aggregation_functions()
                aggregations.extend(funcs)
                offsets.append(current + len(funcs))
                current += len(funcs)

            results = data.agg(*aggregations).collect()[0]
            for i in range(len(analyzers)):
                analyzer = replacers[i].related_analyzer()
                state = analyzer.fetch_state_from_aggregation_result(results, offsets[i])
                # if state_persister is not None:
                #     state_persister.persist(analyzer, state)
                replacers[i].prepare_replacement(Replacer.fetch_replacement_from_state(state))
        except Exception as error:
            raise TransformCalculationException(error)
