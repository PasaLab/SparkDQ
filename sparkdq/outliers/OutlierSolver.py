from enum import Enum


class OutlierSolver(Enum):
    """
    All the models supplied by our system.
    1. K-Sigma rule is the general form of 3-sigma rule, which means values lie within a band around the mean in a
        normal distribution with a width of k stand deviations.
    2. Tukey test utilizes quantiles and IQR(interquartile range) which equals to the difference between 75th and 25th
        percentiles(IQR=Q3-Q1), most values lie with in the range of [Q1-k*IQR, Q3+k*IQR]
    3. MAD(median absolute deviation) utilizes MAD as the width of band centered on median, MAD is the median of all
        absolute deviations from the data's median
    4. SMA(simple moving average) calculates the average of a fixed-size window moving along the data, if the difference
        between SMA and current data is too large, current data may be a outlier
    5. GMM(Gaussian mixture model) based on clustering algorithm
    6. KMeans based on clustering algorithm
    7. LOF(local outlier factor) based on density
    8. IsolationForest based on ensemble algorithm
    9. PCA based on feature reduction
    10.AutoEncoder based on data reconstruction
    """
    kSigma = "k-sigma"
    TukeyTest = "tukey-test"
    MAD = "mad"
    SMA = "sma"
    GMM = "gmm"
    KMeans = "kmeans"
    DBSCAN = "dbscan"
    LOF = "lof"
    IForest = "iforest"
    PCA = "pca"
    AutoEncoder = "auto-encoder"

    def default_params(self):

        from sparkdq.outliers.params.AutoEncoderParams import AutoEncoderParams
        from sparkdq.outliers.params.DBSCANParams import DBSCANParams
        from sparkdq.outliers.params.GMMParams import GMMParams
        from sparkdq.outliers.params.IForestParams import IForestParams
        from sparkdq.outliers.params.KMeansParams import KMeansParams
        from sparkdq.outliers.params.KSigmaParams import KSigmaParams
        from sparkdq.outliers.params.LOFParams import LOFParams
        from sparkdq.outliers.params.MADParams import MADParams
        from sparkdq.outliers.params.SMAParams import SMAParams
        from sparkdq.outliers.params.PCAParams import PCAParams
        from sparkdq.outliers.params.TukeyTestParams import TukeyTestParams

        if self == OutlierSolver.kSigma:
            return KSigmaParams()
        elif self == OutlierSolver.TukeyTest:
            return TukeyTestParams()
        elif self == OutlierSolver.MAD:
            return MADParams()
        elif self == OutlierSolver.MA:
            return SMAParams()
        elif self == OutlierSolver.GMM:
            return GMMParams()
        elif self == OutlierSolver.KMeans:
            return KMeansParams()
        elif self == OutlierSolver.DBSCAN:
            return DBSCANParams()
        elif self == OutlierSolver.LOF:
            return LOFParams()
        elif self == OutlierSolver.IForest:
            return IForestParams()
        elif self == OutlierSolver.PCA:
            return PCAParams()
        elif self == OutlierSolver.AutoEncoder:
            return AutoEncoderParams()
        else:
            raise Exception("Unknown outlier model {}!".format(self))
