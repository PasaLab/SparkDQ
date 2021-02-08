class RepositoryOptions:
    """
    Repository options including metrics repository, reuse existing key, save result key
    metrics repository can use memory or file system for storing, reuse key is to fetch previously calculated
    results, save key is to store the new results.
    """
    def __init__(self, metrics_repository=None, reuse_existing_result_key=None, save_result_key=None):
        self.metrics_repository = metrics_repository
        self.reuse_existing_result_key = reuse_existing_result_key
        self.save_result_key = save_result_key
