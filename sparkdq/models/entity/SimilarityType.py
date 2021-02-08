from enum import Enum


class SimilarityType(Enum):
    COSINE = "cosine"
    JARO_WINKLER = "jaro-winkler"
    JACCARD_INDEX = "jaccard index"
    NORMALIZED_LEVENSHTEIN = "normalized levenshtein"
    SORENSEN_DICE_COEFFICIENT = "sorensen-dice coefficient"
