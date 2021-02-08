from abc import abstractmethod


class ConstraintRule:

    @abstractmethod
    def is_verified(self, profile, num_records):
        pass

    @abstractmethod
    def generate_constraint_suggestion(self, profile, num_records):
        pass
