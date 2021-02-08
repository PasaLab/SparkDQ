from sparkdq.conf.Context import Context


class DetectionResult:
    """
    Result of the detection

    Args:
        status: total status of all checks, Error if exists Error, else Success
        check_results:
    """
    def __init__(self, status, check_results, metrics):
        self.status = status
        self.check_results = check_results
        self.metrics = metrics

    def __str__(self):
        return "Status: {}\nCheck_results: {}\nMetrics: {}".format(self.status, self.check_results, self.metrics)

    def assertion_results_as_df(self):
        data = []
        for k, v in self.check_results.items():
            # check_des = k.description
            constraint_results = v.constraint_results
            for const_res in constraint_results:
                curr_res = [const_res.constraint.name, const_res.status.name, const_res.message]
                data.append(curr_res)
        return Context().spark.createDataFrame(data, ["constraint", "status", "message"])

    def success_metrics_as_df(self):
        data = []
        checks = self.check_results.keys()
        for check in checks:
            # check_des = check.description
            for a in check.required_analyzers():
                if (a in self.metrics) and self.metrics[a].is_success:
                    ins = ", ".join(a.instance) if isinstance(a.instance, list) else a.instance
                    data.append([a.name, ins, self.metrics[a].represent_value()])
        return Context().spark.createDataFrame(data, ["name", "instance", "value"])
