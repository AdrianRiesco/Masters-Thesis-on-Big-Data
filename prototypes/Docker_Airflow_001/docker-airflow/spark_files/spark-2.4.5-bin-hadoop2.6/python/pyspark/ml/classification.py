#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import operator
import sys
from multiprocessing.pool import ThreadPool

from pyspark import since, keyword_only
from pyspark.ml import Estimator, Model
from pyspark.ml.param.shared import *
from pyspark.ml.regression import DecisionTreeModel, DecisionTreeRegressionModel, \
    RandomForestParams, TreeEnsembleModel, TreeEnsembleParams
from pyspark.ml.util import *
from pyspark.ml.wrapper import JavaEstimator, JavaModel, JavaParams
from pyspark.ml.wrapper import JavaWrapper
from pyspark.ml.common import inherit_doc, _java2py, _py2java
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, when
from pyspark.sql.types import ArrayType, DoubleType
from pyspark.storagelevel import StorageLevel

__all__ = ['LinearSVC', 'LinearSVCModel',
           'LogisticRegression', 'LogisticRegressionModel',
           'LogisticRegressionSummary', 'LogisticRegressionTrainingSummary',
           'BinaryLogisticRegressionSummary', 'BinaryLogisticRegressionTrainingSummary',
           'DecisionTreeClassifier', 'DecisionTreeClassificationModel',
           'GBTClassifier', 'GBTClassificationModel',
           'RandomForestClassifier', 'RandomForestClassificationModel',
           'NaiveBayes', 'NaiveBayesModel',
           'MultilayerPerceptronClassifier', 'MultilayerPerceptronClassificationModel',
           'OneVsRest', 'OneVsRestModel']


@inherit_doc
class JavaClassificationModel(JavaPredictionModel):
    """
    (Private) Java Model produced by a ``Classifier``.
    Classes are indexed {0, 1, ..., numClasses - 1}.
    To be mixed in with class:`pyspark.ml.JavaModel`
    """

    @property
    @since("2.1.0")
    def numClasses(self):
        """
        Number of classes (values which the label can take).
        """
        return self._call_java("numClasses")


@inherit_doc
class LinearSVC(JavaEstimator, HasFeaturesCol, HasLabelCol, HasPredictionCol, HasMaxIter,
                HasRegParam, HasTol, HasRawPredictionCol, HasFitIntercept, HasStandardization,
                HasWeightCol, HasAggregationDepth, HasThreshold, JavaMLWritable, JavaMLReadable):
    """
    .. note:: Experimental

    `Linear SVM Classifier <https://en.wikipedia.org/wiki/Support_vector_machine#Linear_SVM>`_

    This binary classifier optimizes the Hinge Loss using the OWLQN optimizer.
    Only supports L2 regularization currently.

    >>> from pyspark.sql import Row
    >>> from pyspark.ml.linalg import Vectors
    >>> df = sc.parallelize([
    ...     Row(label=1.0, features=Vectors.dense(1.0, 1.0, 1.0)),
    ...     Row(label=0.0, features=Vectors.dense(1.0, 2.0, 3.0))]).toDF()
    >>> svm = LinearSVC(maxIter=5, regParam=0.01)
    >>> model = svm.fit(df)
    >>> model.coefficients
    DenseVector([0.0, -0.2792, -0.1833])
    >>> model.intercept
    1.0206118982229047
    >>> model.numClasses
    2
    >>> model.numFeatures
    3
    >>> test0 = sc.parallelize([Row(features=Vectors.dense(-1.0, -1.0, -1.0))]).toDF()
    >>> result = model.transform(test0).head()
    >>> result.prediction
    1.0
    >>> result.rawPrediction
    DenseVector([-1.4831, 1.4831])
    >>> svm_path = temp_path + "/svm"
    >>> svm.save(svm_path)
    >>> svm2 = LinearSVC.load(svm_path)
    >>> svm2.getMaxIter()
    5
    >>> model_path = temp_path + "/svm_model"
    >>> model.save(model_path)
    >>> model2 = LinearSVCModel.load(model_path)
    >>> model.coefficients[0] == model2.coefficients[0]
    True
    >>> model.intercept == model2.intercept
    True

    .. versionadded:: 2.2.0
    """

    threshold = Param(Params._dummy(), "threshold",
                      "The threshold in binary classification applied to the linear model"
                      " prediction.  This threshold can be any real number, where Inf will make"
                      " all predictions 0.0 and -Inf will make all predictions 1.0.",
                      typeConverter=TypeConverters.toFloat)

    @keyword_only
    def __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                 maxIter=100, regParam=0.0, tol=1e-6, rawPredictionCol="rawPrediction",
                 fitIntercept=True, standardization=True, threshold=0.0, weightCol=None,
                 aggregationDepth=2):
        """
        __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 maxIter=100, regParam=0.0, tol=1e-6, rawPredictionCol="rawPrediction", \
                 fitIntercept=True, standardization=True, threshold=0.0, weightCol=None, \
                 aggregationDepth=2):
        """
        super(LinearSVC, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.classification.LinearSVC", self.uid)
        self._setDefault(maxIter=100, regParam=0.0, tol=1e-6, fitIntercept=True,
                         standardization=True, threshold=0.0, aggregationDepth=2)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("2.2.0")
    def setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                  maxIter=100, regParam=0.0, tol=1e-6, rawPredictionCol="rawPrediction",
                  fitIntercept=True, standardization=True, threshold=0.0, weightCol=None,
                  aggregationDepth=2):
        """
        setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                  maxIter=100, regParam=0.0, tol=1e-6, rawPredictionCol="rawPrediction", \
                  fitIntercept=True, standardization=True, threshold=0.0, weightCol=None, \
                  aggregationDepth=2):
        Sets params for Linear SVM Classifier.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return LinearSVCModel(java_model)


class LinearSVCModel(JavaModel, JavaClassificationModel, JavaMLWritable, JavaMLReadable):
    """
    .. note:: Experimental

    Model fitted by LinearSVC.

    .. versionadded:: 2.2.0
    """

    @property
    @since("2.2.0")
    def coefficients(self):
        """
        Model coefficients of Linear SVM Classifier.
        """
        return self._call_java("coefficients")

    @property
    @since("2.2.0")
    def intercept(self):
        """
        Model intercept of Linear SVM Classifier.
        """
        return self._call_java("intercept")


@inherit_doc
class LogisticRegression(JavaEstimator, HasFeaturesCol, HasLabelCol, HasPredictionCol, HasMaxIter,
                         HasRegParam, HasTol, HasProbabilityCol, HasRawPredictionCol,
                         HasElasticNetParam, HasFitIntercept, HasStandardization, HasThresholds,
                         HasWeightCol, HasAggregationDepth, JavaMLWritable, JavaMLReadable):
    """
    Logistic regression.
    This class supports multinomial logistic (softmax) and binomial logistic regression.

    >>> from pyspark.sql import Row
    >>> from pyspark.ml.linalg import Vectors
    >>> bdf = sc.parallelize([
    ...     Row(label=1.0, weight=1.0, features=Vectors.dense(0.0, 5.0)),
    ...     Row(label=0.0, weight=2.0, features=Vectors.dense(1.0, 2.0)),
    ...     Row(label=1.0, weight=3.0, features=Vectors.dense(2.0, 1.0)),
    ...     Row(label=0.0, weight=4.0, features=Vectors.dense(3.0, 3.0))]).toDF()
    >>> blor = LogisticRegression(regParam=0.01, weightCol="weight")
    >>> blorModel = blor.fit(bdf)
    >>> blorModel.coefficients
    DenseVector([-1.080..., -0.646...])
    >>> blorModel.intercept
    3.112...
    >>> data_path = "data/mllib/sample_multiclass_classification_data.txt"
    >>> mdf = spark.read.format("libsvm").load(data_path)
    >>> mlor = LogisticRegression(regParam=0.1, elasticNetParam=1.0, family="multinomial")
    >>> mlorModel = mlor.fit(mdf)
    >>> mlorModel.coefficientMatrix
    SparseMatrix(3, 4, [0, 1, 2, 3], [3, 2, 1], [1.87..., -2.75..., -0.50...], 1)
    >>> mlorModel.interceptVector
    DenseVector([0.04..., -0.42..., 0.37...])
    >>> test0 = sc.parallelize([Row(features=Vectors.dense(-1.0, 1.0))]).toDF()
    >>> result = blorModel.transform(test0).head()
    >>> result.prediction
    1.0
    >>> result.probability
    DenseVector([0.02..., 0.97...])
    >>> result.rawPrediction
    DenseVector([-3.54..., 3.54...])
    >>> test1 = sc.parallelize([Row(features=Vectors.sparse(2, [0], [1.0]))]).toDF()
    >>> blorModel.transform(test1).head().prediction
    1.0
    >>> blor.setParams("vector")
    Traceback (most recent call last):
        ...
    TypeError: Method setParams forces keyword arguments.
    >>> lr_path = temp_path + "/lr"
    >>> blor.save(lr_path)
    >>> lr2 = LogisticRegression.load(lr_path)
    >>> lr2.getRegParam()
    0.01
    >>> model_path = temp_path + "/lr_model"
    >>> blorModel.save(model_path)
    >>> model2 = LogisticRegressionModel.load(model_path)
    >>> blorModel.coefficients[0] == model2.coefficients[0]
    True
    >>> blorModel.intercept == model2.intercept
    True
    >>> model2
    LogisticRegressionModel: uid = ..., numClasses = 2, numFeatures = 2

    .. versionadded:: 1.3.0
    """

    threshold = Param(Params._dummy(), "threshold",
                      "Threshold in binary classification prediction, in range [0, 1]." +
                      " If threshold and thresholds are both set, they must match." +
                      "e.g. if threshold is p, then thresholds must be equal to [1-p, p].",
                      typeConverter=TypeConverters.toFloat)

    family = Param(Params._dummy(), "family",
                   "The name of family which is a description of the label distribution to " +
                   "be used in the model. Supported options: auto, binomial, multinomial",
                   typeConverter=TypeConverters.toString)

    lowerBoundsOnCoefficients = Param(Params._dummy(), "lowerBoundsOnCoefficients",
                                      "The lower bounds on coefficients if fitting under bound "
                                      "constrained optimization. The bound matrix must be "
                                      "compatible with the shape "
                                      "(1, number of features) for binomial regression, or "
                                      "(number of classes, number of features) "
                                      "for multinomial regression.",
                                      typeConverter=TypeConverters.toMatrix)

    upperBoundsOnCoefficients = Param(Params._dummy(), "upperBoundsOnCoefficients",
                                      "The upper bounds on coefficients if fitting under bound "
                                      "constrained optimization. The bound matrix must be "
                                      "compatible with the shape "
                                      "(1, number of features) for binomial regression, or "
                                      "(number of classes, number of features) "
                                      "for multinomial regression.",
                                      typeConverter=TypeConverters.toMatrix)

    lowerBoundsOnIntercepts = Param(Params._dummy(), "lowerBoundsOnIntercepts",
                                    "The lower bounds on intercepts if fitting under bound "
                                    "constrained optimization. The bounds vector size must be"
                                    "equal with 1 for binomial regression, or the number of"
                                    "lasses for multinomial regression.",
                                    typeConverter=TypeConverters.toVector)

    upperBoundsOnIntercepts = Param(Params._dummy(), "upperBoundsOnIntercepts",
                                    "The upper bounds on intercepts if fitting under bound "
                                    "constrained optimization. The bound vector size must be "
                                    "equal with 1 for binomial regression, or the number of "
                                    "classes for multinomial regression.",
                                    typeConverter=TypeConverters.toVector)

    @keyword_only
    def __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                 maxIter=100, regParam=0.0, elasticNetParam=0.0, tol=1e-6, fitIntercept=True,
                 threshold=0.5, thresholds=None, probabilityCol="probability",
                 rawPredictionCol="rawPrediction", standardization=True, weightCol=None,
                 aggregationDepth=2, family="auto",
                 lowerBoundsOnCoefficients=None, upperBoundsOnCoefficients=None,
                 lowerBoundsOnIntercepts=None, upperBoundsOnIntercepts=None):

        """
        __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 maxIter=100, regParam=0.0, elasticNetParam=0.0, tol=1e-6, fitIntercept=True, \
                 threshold=0.5, thresholds=None, probabilityCol="probability", \
                 rawPredictionCol="rawPrediction", standardization=True, weightCol=None, \
                 aggregationDepth=2, family="auto", \
                 lowerBoundsOnCoefficients=None, upperBoundsOnCoefficients=None, \
                 lowerBoundsOnIntercepts=None, upperBoundsOnIntercepts=None):
        If the threshold and thresholds Params are both set, they must be equivalent.
        """
        super(LogisticRegression, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.classification.LogisticRegression", self.uid)
        self._setDefault(maxIter=100, regParam=0.0, tol=1E-6, threshold=0.5, family="auto")
        kwargs = self._input_kwargs
        self.setParams(**kwargs)
        self._checkThresholdConsistency()

    @keyword_only
    @since("1.3.0")
    def setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                  maxIter=100, regParam=0.0, elasticNetParam=0.0, tol=1e-6, fitIntercept=True,
                  threshold=0.5, thresholds=None, probabilityCol="probability",
                  rawPredictionCol="rawPrediction", standardization=True, weightCol=None,
                  aggregationDepth=2, family="auto",
                  lowerBoundsOnCoefficients=None, upperBoundsOnCoefficients=None,
                  lowerBoundsOnIntercepts=None, upperBoundsOnIntercepts=None):
        """
        setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                  maxIter=100, regParam=0.0, elasticNetParam=0.0, tol=1e-6, fitIntercept=True, \
                  threshold=0.5, thresholds=None, probabilityCol="probability", \
                  rawPredictionCol="rawPrediction", standardization=True, weightCol=None, \
                  aggregationDepth=2, family="auto", \
                  lowerBoundsOnCoefficients=None, upperBoundsOnCoefficients=None, \
                  lowerBoundsOnIntercepts=None, upperBoundsOnIntercepts=None):
        Sets params for logistic regression.
        If the threshold and thresholds Params are both set, they must be equivalent.
        """
        kwargs = self._input_kwargs
        self._set(**kwargs)
        self._checkThresholdConsistency()
        return self

    def _create_model(self, java_model):
        return LogisticRegressionModel(java_model)

    @since("1.4.0")
    def setThreshold(self, value):
        """
        Sets the value of :py:attr:`threshold`.
        Clears value of :py:attr:`thresholds` if it has been set.
        """
        self._set(threshold=value)
        self._clear(self.thresholds)
        return self

    @since("1.4.0")
    def getThreshold(self):
        """
        Get threshold for binary classification.

        If :py:attr:`thresholds` is set with length 2 (i.e., binary classification),
        this returns the equivalent threshold:
        :math:`\\frac{1}{1 + \\frac{thresholds(0)}{thresholds(1)}}`.
        Otherwise, returns :py:attr:`threshold` if set or its default value if unset.
        """
        self._checkThresholdConsistency()
        if self.isSet(self.thresholds):
            ts = self.getOrDefault(self.thresholds)
            if len(ts) != 2:
                raise ValueError("Logistic Regression getThreshold only applies to" +
                                 " binary classification, but thresholds has length != 2." +
                                 "  thresholds: " + ",".join(ts))
            return 1.0/(1.0 + ts[0]/ts[1])
        else:
            return self.getOrDefault(self.threshold)

    @since("1.5.0")
    def setThresholds(self, value):
        """
        Sets the value of :py:attr:`thresholds`.
        Clears value of :py:attr:`threshold` if it has been set.
        """
        self._set(thresholds=value)
        self._clear(self.threshold)
        return self

    @since("1.5.0")
    def getThresholds(self):
        """
        If :py:attr:`thresholds` is set, return its value.
        Otherwise, if :py:attr:`threshold` is set, return the equivalent thresholds for binary
        classification: (1-threshold, threshold).
        If neither are set, throw an error.
        """
        self._checkThresholdConsistency()
        if not self.isSet(self.thresholds) and self.isSet(self.threshold):
            t = self.getOrDefault(self.threshold)
            return [1.0-t, t]
        else:
            return self.getOrDefault(self.thresholds)

    def _checkThresholdConsistency(self):
        if self.isSet(self.threshold) and self.isSet(self.thresholds):
            ts = self.getOrDefault(self.thresholds)
            if len(ts) != 2:
                raise ValueError("Logistic Regression getThreshold only applies to" +
                                 " binary classification, but thresholds has length != 2." +
                                 " thresholds: {0}".format(str(ts)))
            t = 1.0/(1.0 + ts[0]/ts[1])
            t2 = self.getOrDefault(self.threshold)
            if abs(t2 - t) >= 1E-5:
                raise ValueError("Logistic Regression getThreshold found inconsistent values for" +
                                 " threshold (%g) and thresholds (equivalent to %g)" % (t2, t))

    @since("2.1.0")
    def setFamily(self, value):
        """
        Sets the value of :py:attr:`family`.
        """
        return self._set(family=value)

    @since("2.1.0")
    def getFamily(self):
        """
        Gets the value of :py:attr:`family` or its default value.
        """
        return self.getOrDefault(self.family)

    @since("2.3.0")
    def setLowerBoundsOnCoefficients(self, value):
        """
        Sets the value of :py:attr:`lowerBoundsOnCoefficients`
        """
        return self._set(lowerBoundsOnCoefficients=value)

    @since("2.3.0")
    def getLowerBoundsOnCoefficients(self):
        """
        Gets the value of :py:attr:`lowerBoundsOnCoefficients`
        """
        return self.getOrDefault(self.lowerBoundsOnCoefficients)

    @since("2.3.0")
    def setUpperBoundsOnCoefficients(self, value):
        """
        Sets the value of :py:attr:`upperBoundsOnCoefficients`
        """
        return self._set(upperBoundsOnCoefficients=value)

    @since("2.3.0")
    def getUpperBoundsOnCoefficients(self):
        """
        Gets the value of :py:attr:`upperBoundsOnCoefficients`
        """
        return self.getOrDefault(self.upperBoundsOnCoefficients)

    @since("2.3.0")
    def setLowerBoundsOnIntercepts(self, value):
        """
        Sets the value of :py:attr:`lowerBoundsOnIntercepts`
        """
        return self._set(lowerBoundsOnIntercepts=value)

    @since("2.3.0")
    def getLowerBoundsOnIntercepts(self):
        """
        Gets the value of :py:attr:`lowerBoundsOnIntercepts`
        """
        return self.getOrDefault(self.lowerBoundsOnIntercepts)

    @since("2.3.0")
    def setUpperBoundsOnIntercepts(self, value):
        """
        Sets the value of :py:attr:`upperBoundsOnIntercepts`
        """
        return self._set(upperBoundsOnIntercepts=value)

    @since("2.3.0")
    def getUpperBoundsOnIntercepts(self):
        """
        Gets the value of :py:attr:`upperBoundsOnIntercepts`
        """
        return self.getOrDefault(self.upperBoundsOnIntercepts)


class LogisticRegressionModel(JavaModel, JavaClassificationModel, JavaMLWritable, JavaMLReadable):
    """
    Model fitted by LogisticRegression.

    .. versionadded:: 1.3.0
    """

    @property
    @since("2.0.0")
    def coefficients(self):
        """
        Model coefficients of binomial logistic regression.
        An exception is thrown in the case of multinomial logistic regression.
        """
        return self._call_java("coefficients")

    @property
    @since("1.4.0")
    def intercept(self):
        """
        Model intercept of binomial logistic regression.
        An exception is thrown in the case of multinomial logistic regression.
        """
        return self._call_java("intercept")

    @property
    @since("2.1.0")
    def coefficientMatrix(self):
        """
        Model coefficients.
        """
        return self._call_java("coefficientMatrix")

    @property
    @since("2.1.0")
    def interceptVector(self):
        """
        Model intercept.
        """
        return self._call_java("interceptVector")

    @property
    @since("2.0.0")
    def summary(self):
        """
        Gets summary (e.g. accuracy/precision/recall, objective history, total iterations) of model
        trained on the training set. An exception is thrown if `trainingSummary is None`.
        """
        if self.hasSummary:
            java_lrt_summary = self._call_java("summary")
            if self.numClasses <= 2:
                return BinaryLogisticRegressionTrainingSummary(java_lrt_summary)
            else:
                return LogisticRegressionTrainingSummary(java_lrt_summary)
        else:
            raise RuntimeError("No training summary available for this %s" %
                               self.__class__.__name__)

    @property
    @since("2.0.0")
    def hasSummary(self):
        """
        Indicates whether a training summary exists for this model
        instance.
        """
        return self._call_java("hasSummary")

    @since("2.0.0")
    def evaluate(self, dataset):
        """
        Evaluates the model on a test dataset.

        :param dataset:
          Test dataset to evaluate model on, where dataset is an
          instance of :py:class:`pyspark.sql.DataFrame`
        """
        if not isinstance(dataset, DataFrame):
            raise ValueError("dataset must be a DataFrame but got %s." % type(dataset))
        java_blr_summary = self._call_java("evaluate", dataset)
        return BinaryLogisticRegressionSummary(java_blr_summary)

    def __repr__(self):
        return self._call_java("toString")


class LogisticRegressionSummary(JavaWrapper):
    """
    .. note:: Experimental

    Abstraction for Logistic Regression Results for a given model.

    .. versionadded:: 2.0.0
    """

    @property
    @since("2.0.0")
    def predictions(self):
        """
        Dataframe outputted by the model's `transform` method.
        """
        return self._call_java("predictions")

    @property
    @since("2.0.0")
    def probabilityCol(self):
        """
        Field in "predictions" which gives the probability
        of each class as a vector.
        """
        return self._call_java("probabilityCol")

    @property
    @since("2.3.0")
    def predictionCol(self):
        """
        Field in "predictions" which gives the prediction of each class.
        """
        return self._call_java("predictionCol")

    @property
    @since("2.0.0")
    def labelCol(self):
        """
        Field in "predictions" which gives the true label of each
        instance.
        """
        return self._call_java("labelCol")

    @property
    @since("2.0.0")
    def featuresCol(self):
        """
        Field in "predictions" which gives the features of each instance
        as a vector.
        """
        return self._call_java("featuresCol")

    @property
    @since("2.3.0")
    def labels(self):
        """
        Returns the sequence of labels in ascending order. This order matches the order used
        in metrics which are specified as arrays over labels, e.g., truePositiveRateByLabel.

        Note: In most cases, it will be values {0.0, 1.0, ..., numClasses-1}, However, if the
        training set is missing a label, then all of the arrays over labels
        (e.g., from truePositiveRateByLabel) will be of length numClasses-1 instead of the
        expected numClasses.
        """
        return self._call_java("labels")

    @property
    @since("2.3.0")
    def truePositiveRateByLabel(self):
        """
        Returns true positive rate for each label (category).
        """
        return self._call_java("truePositiveRateByLabel")

    @property
    @since("2.3.0")
    def falsePositiveRateByLabel(self):
        """
        Returns false positive rate for each label (category).
        """
        return self._call_java("falsePositiveRateByLabel")

    @property
    @since("2.3.0")
    def precisionByLabel(self):
        """
        Returns precision for each label (category).
        """
        return self._call_java("precisionByLabel")

    @property
    @since("2.3.0")
    def recallByLabel(self):
        """
        Returns recall for each label (category).
        """
        return self._call_java("recallByLabel")

    @since("2.3.0")
    def fMeasureByLabel(self, beta=1.0):
        """
        Returns f-measure for each label (category).
        """
        return self._call_java("fMeasureByLabel", beta)

    @property
    @since("2.3.0")
    def accuracy(self):
        """
        Returns accuracy.
        (equals to the total number of correctly classified instances
        out of the total number of instances.)
        """
        return self._call_java("accuracy")

    @property
    @since("2.3.0")
    def weightedTruePositiveRate(self):
        """
        Returns weighted true positive rate.
        (equals to precision, recall and f-measure)
        """
        return self._call_java("weightedTruePositiveRate")

    @property
    @since("2.3.0")
    def weightedFalsePositiveRate(self):
        """
        Returns weighted false positive rate.
        """
        return self._call_java("weightedFalsePositiveRate")

    @property
    @since("2.3.0")
    def weightedRecall(self):
        """
        Returns weighted averaged recall.
        (equals to precision, recall and f-measure)
        """
        return self._call_java("weightedRecall")

    @property
    @since("2.3.0")
    def weightedPrecision(self):
        """
        Returns weighted averaged precision.
        """
        return self._call_java("weightedPrecision")

    @since("2.3.0")
    def weightedFMeasure(self, beta=1.0):
        """
        Returns weighted averaged f-measure.
        """
        return self._call_java("weightedFMeasure", beta)


@inherit_doc
class LogisticRegressionTrainingSummary(LogisticRegressionSummary):
    """
    .. note:: Experimental

    Abstraction for multinomial Logistic Regression Training results.
    Currently, the training summary ignores the training weights except
    for the objective trace.

    .. versionadded:: 2.0.0
    """

    @property
    @since("2.0.0")
    def objectiveHistory(self):
        """
        Objective function (scaled loss + regularization) at each
        iteration.
        """
        return self._call_java("objectiveHistory")

    @property
    @since("2.0.0")
    def totalIterations(self):
        """
        Number of training iterations until termination.
        """
        return self._call_java("totalIterations")


@inherit_doc
class BinaryLogisticRegressionSummary(LogisticRegressionSummary):
    """
    .. note:: Experimental

    Binary Logistic regression results for a given model.

    .. versionadded:: 2.0.0
    """

    @property
    @since("2.0.0")
    def roc(self):
        """
        Returns the receiver operating characteristic (ROC) curve,
        which is a Dataframe having two fields (FPR, TPR) with
        (0.0, 0.0) prepended and (1.0, 1.0) appended to it.

        .. seealso:: `Wikipedia reference
            <http://en.wikipedia.org/wiki/Receiver_operating_characteristic>`_

        .. note:: This ignores instance weights (setting all to 1.0) from
            `LogisticRegression.weightCol`. This will change in later Spark
            versions.
        """
        return self._call_java("roc")

    @property
    @since("2.0.0")
    def areaUnderROC(self):
        """
        Computes the area under the receiver operating characteristic
        (ROC) curve.

        .. note:: This ignores instance weights (setting all to 1.0) from
            `LogisticRegression.weightCol`. This will change in later Spark
            versions.
        """
        return self._call_java("areaUnderROC")

    @property
    @since("2.0.0")
    def pr(self):
        """
        Returns the precision-recall curve, which is a Dataframe
        containing two fields recall, precision with (0.0, 1.0) prepended
        to it.

        .. note:: This ignores instance weights (setting all to 1.0) from
            `LogisticRegression.weightCol`. This will change in later Spark
            versions.
        """
        return self._call_java("pr")

    @property
    @since("2.0.0")
    def fMeasureByThreshold(self):
        """
        Returns a dataframe with two fields (threshold, F-Measure) curve
        with beta = 1.0.

        .. note:: This ignores instance weights (setting all to 1.0) from
            `LogisticRegression.weightCol`. This will change in later Spark
            versions.
        """
        return self._call_java("fMeasureByThreshold")

    @property
    @since("2.0.0")
    def precisionByThreshold(self):
        """
        Returns a dataframe with two fields (threshold, precision) curve.
        Every possible probability obtained in transforming the dataset
        are used as thresholds used in calculating the precision.

        .. note:: This ignores instance weights (setting all to 1.0) from
            `LogisticRegression.weightCol`. This will change in later Spark
            versions.
        """
        return self._call_java("precisionByThreshold")

    @property
    @since("2.0.0")
    def recallByThreshold(self):
        """
        Returns a dataframe with two fields (threshold, recall) curve.
        Every possible probability obtained in transforming the dataset
        are used as thresholds used in calculating the recall.

        .. note:: This ignores instance weights (setting all to 1.0) from
            `LogisticRegression.weightCol`. This will change in later Spark
            versions.
        """
        return self._call_java("recallByThreshold")


@inherit_doc
class BinaryLogisticRegressionTrainingSummary(BinaryLogisticRegressionSummary,
                                              LogisticRegressionTrainingSummary):
    """
    .. note:: Experimental

    Binary Logistic regression training results for a given model.

    .. versionadded:: 2.0.0
    """
    pass


class TreeClassifierParams(object):
    """
    Private class to track supported impurity measures.

    .. versionadded:: 1.4.0
    """
    supportedImpurities = ["entropy", "gini"]

    impurity = Param(Params._dummy(), "impurity",
                     "Criterion used for information gain calculation (case-insensitive). " +
                     "Supported options: " +
                     ", ".join(supportedImpurities), typeConverter=TypeConverters.toString)

    def __init__(self):
        super(TreeClassifierParams, self).__init__()

    @since("1.6.0")
    def setImpurity(self, value):
        """
        Sets the value of :py:attr:`impurity`.
        """
        return self._set(impurity=value)

    @since("1.6.0")
    def getImpurity(self):
        """
        Gets the value of impurity or its default value.
        """
        return self.getOrDefault(self.impurity)


class GBTParams(TreeEnsembleParams):
    """
    Private class to track supported GBT params.

    .. versionadded:: 1.4.0
    """
    supportedLossTypes = ["logistic"]


@inherit_doc
class DecisionTreeClassifier(JavaEstimator, HasFeaturesCol, HasLabelCol, HasPredictionCol,
                             HasProbabilityCol, HasRawPredictionCol, DecisionTreeParams,
                             TreeClassifierParams, HasCheckpointInterval, HasSeed, JavaMLWritable,
                             JavaMLReadable):
    """
    `Decision tree <http://en.wikipedia.org/wiki/Decision_tree_learning>`_
    learning algorithm for classification.
    It supports both binary and multiclass labels, as well as both continuous and categorical
    features.

    >>> from pyspark.ml.linalg import Vectors
    >>> from pyspark.ml.feature import StringIndexer
    >>> df = spark.createDataFrame([
    ...     (1.0, Vectors.dense(1.0)),
    ...     (0.0, Vectors.sparse(1, [], []))], ["label", "features"])
    >>> stringIndexer = StringIndexer(inputCol="label", outputCol="indexed")
    >>> si_model = stringIndexer.fit(df)
    >>> td = si_model.transform(df)
    >>> dt = DecisionTreeClassifier(maxDepth=2, labelCol="indexed")
    >>> model = dt.fit(td)
    >>> model.numNodes
    3
    >>> model.depth
    1
    >>> model.featureImportances
    SparseVector(1, {0: 1.0})
    >>> model.numFeatures
    1
    >>> model.numClasses
    2
    >>> print(model.toDebugString)
    DecisionTreeClassificationModel (uid=...) of depth 1 with 3 nodes...
    >>> test0 = spark.createDataFrame([(Vectors.dense(-1.0),)], ["features"])
    >>> result = model.transform(test0).head()
    >>> result.prediction
    0.0
    >>> result.probability
    DenseVector([1.0, 0.0])
    >>> result.rawPrediction
    DenseVector([1.0, 0.0])
    >>> test1 = spark.createDataFrame([(Vectors.sparse(1, [0], [1.0]),)], ["features"])
    >>> model.transform(test1).head().prediction
    1.0

    >>> dtc_path = temp_path + "/dtc"
    >>> dt.save(dtc_path)
    >>> dt2 = DecisionTreeClassifier.load(dtc_path)
    >>> dt2.getMaxDepth()
    2
    >>> model_path = temp_path + "/dtc_model"
    >>> model.save(model_path)
    >>> model2 = DecisionTreeClassificationModel.load(model_path)
    >>> model.featureImportances == model2.featureImportances
    True

    .. versionadded:: 1.4.0
    """

    @keyword_only
    def __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                 probabilityCol="probability", rawPredictionCol="rawPrediction",
                 maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                 maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, impurity="gini",
                 seed=None):
        """
        __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 probabilityCol="probability", rawPredictionCol="rawPrediction", \
                 maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0, \
                 maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, impurity="gini", \
                 seed=None)
        """
        super(DecisionTreeClassifier, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.classification.DecisionTreeClassifier", self.uid)
        self._setDefault(maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                         maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10,
                         impurity="gini")
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                  probabilityCol="probability", rawPredictionCol="rawPrediction",
                  maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                  maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10,
                  impurity="gini", seed=None):
        """
        setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                  probabilityCol="probability", rawPredictionCol="rawPrediction", \
                  maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0, \
                  maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, impurity="gini", \
                  seed=None)
        Sets params for the DecisionTreeClassifier.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return DecisionTreeClassificationModel(java_model)


@inherit_doc
class DecisionTreeClassificationModel(DecisionTreeModel, JavaClassificationModel, JavaMLWritable,
                                      JavaMLReadable):
    """
    Model fitted by DecisionTreeClassifier.

    .. versionadded:: 1.4.0
    """

    @property
    @since("2.0.0")
    def featureImportances(self):
        """
        Estimate of the importance of each feature.

        This generalizes the idea of "Gini" importance to other losses,
        following the explanation of Gini importance from "Random Forests" documentation
        by Leo Breiman and Adele Cutler, and following the implementation from scikit-learn.

        This feature importance is calculated as follows:
          - importance(feature j) = sum (over nodes which split on feature j) of the gain,
            where gain is scaled by the number of instances passing through node
          - Normalize importances for tree to sum to 1.

        .. note:: Feature importance for single decision trees can have high variance due to
            correlated predictor variables. Consider using a :py:class:`RandomForestClassifier`
            to determine feature importance instead.
        """
        return self._call_java("featureImportances")


@inherit_doc
class RandomForestClassifier(JavaEstimator, HasFeaturesCol, HasLabelCol, HasPredictionCol, HasSeed,
                             HasRawPredictionCol, HasProbabilityCol,
                             RandomForestParams, TreeClassifierParams, HasCheckpointInterval,
                             JavaMLWritable, JavaMLReadable):
    """
    `Random Forest <http://en.wikipedia.org/wiki/Random_forest>`_
    learning algorithm for classification.
    It supports both binary and multiclass labels, as well as both continuous and categorical
    features.

    >>> import numpy
    >>> from numpy import allclose
    >>> from pyspark.ml.linalg import Vectors
    >>> from pyspark.ml.feature import StringIndexer
    >>> df = spark.createDataFrame([
    ...     (1.0, Vectors.dense(1.0)),
    ...     (0.0, Vectors.sparse(1, [], []))], ["label", "features"])
    >>> stringIndexer = StringIndexer(inputCol="label", outputCol="indexed")
    >>> si_model = stringIndexer.fit(df)
    >>> td = si_model.transform(df)
    >>> rf = RandomForestClassifier(numTrees=3, maxDepth=2, labelCol="indexed", seed=42)
    >>> model = rf.fit(td)
    >>> model.featureImportances
    SparseVector(1, {0: 1.0})
    >>> allclose(model.treeWeights, [1.0, 1.0, 1.0])
    True
    >>> test0 = spark.createDataFrame([(Vectors.dense(-1.0),)], ["features"])
    >>> result = model.transform(test0).head()
    >>> result.prediction
    0.0
    >>> numpy.argmax(result.probability)
    0
    >>> numpy.argmax(result.rawPrediction)
    0
    >>> test1 = spark.createDataFrame([(Vectors.sparse(1, [0], [1.0]),)], ["features"])
    >>> model.transform(test1).head().prediction
    1.0
    >>> model.trees
    [DecisionTreeClassificationModel (uid=...) of depth..., DecisionTreeClassificationModel...]
    >>> rfc_path = temp_path + "/rfc"
    >>> rf.save(rfc_path)
    >>> rf2 = RandomForestClassifier.load(rfc_path)
    >>> rf2.getNumTrees()
    3
    >>> model_path = temp_path + "/rfc_model"
    >>> model.save(model_path)
    >>> model2 = RandomForestClassificationModel.load(model_path)
    >>> model.featureImportances == model2.featureImportances
    True

    .. versionadded:: 1.4.0
    """

    @keyword_only
    def __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                 probabilityCol="probability", rawPredictionCol="rawPrediction",
                 maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                 maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, impurity="gini",
                 numTrees=20, featureSubsetStrategy="auto", seed=None, subsamplingRate=1.0):
        """
        __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 probabilityCol="probability", rawPredictionCol="rawPrediction", \
                 maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0, \
                 maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, impurity="gini", \
                 numTrees=20, featureSubsetStrategy="auto", seed=None, subsamplingRate=1.0)
        """
        super(RandomForestClassifier, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.classification.RandomForestClassifier", self.uid)
        self._setDefault(maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                         maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10,
                         impurity="gini", numTrees=20, featureSubsetStrategy="auto",
                         subsamplingRate=1.0)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                  probabilityCol="probability", rawPredictionCol="rawPrediction",
                  maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                  maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, seed=None,
                  impurity="gini", numTrees=20, featureSubsetStrategy="auto", subsamplingRate=1.0):
        """
        setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 probabilityCol="probability", rawPredictionCol="rawPrediction", \
                  maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0, \
                  maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, seed=None, \
                  impurity="gini", numTrees=20, featureSubsetStrategy="auto", subsamplingRate=1.0)
        Sets params for linear classification.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return RandomForestClassificationModel(java_model)

    @since("2.4.0")
    def setFeatureSubsetStrategy(self, value):
        """
        Sets the value of :py:attr:`featureSubsetStrategy`.
        """
        return self._set(featureSubsetStrategy=value)


class RandomForestClassificationModel(TreeEnsembleModel, JavaClassificationModel, JavaMLWritable,
                                      JavaMLReadable):
    """
    Model fitted by RandomForestClassifier.

    .. versionadded:: 1.4.0
    """

    @property
    @since("2.0.0")
    def featureImportances(self):
        """
        Estimate of the importance of each feature.

        Each feature's importance is the average of its importance across all trees in the ensemble
        The importance vector is normalized to sum to 1. This method is suggested by Hastie et al.
        (Hastie, Tibshirani, Friedman. "The Elements of Statistical Learning, 2nd Edition." 2001.)
        and follows the implementation from scikit-learn.

        .. seealso:: :py:attr:`DecisionTreeClassificationModel.featureImportances`
        """
        return self._call_java("featureImportances")

    @property
    @since("2.0.0")
    def trees(self):
        """Trees in this ensemble. Warning: These have null parent Estimators."""
        return [DecisionTreeClassificationModel(m) for m in list(self._call_java("trees"))]


@inherit_doc
class GBTClassifier(JavaEstimator, HasFeaturesCol, HasLabelCol, HasPredictionCol, HasMaxIter,
                    GBTParams, HasCheckpointInterval, HasStepSize, HasSeed, JavaMLWritable,
                    JavaMLReadable):
    """
    `Gradient-Boosted Trees (GBTs) <http://en.wikipedia.org/wiki/Gradient_boosting>`_
    learning algorithm for classification.
    It supports binary labels, as well as both continuous and categorical features.

    The implementation is based upon: J.H. Friedman. "Stochastic Gradient Boosting." 1999.

    Notes on Gradient Boosting vs. TreeBoost:
    - This implementation is for Stochastic Gradient Boosting, not for TreeBoost.
    - Both algorithms learn tree ensembles by minimizing loss functions.
    - TreeBoost (Friedman, 1999) additionally modifies the outputs at tree leaf nodes
    based on the loss function, whereas the original gradient boosting method does not.
    - We expect to implement TreeBoost in the future:
    `SPARK-4240 <https://issues.apache.org/jira/browse/SPARK-4240>`_

    .. note:: Multiclass labels are not currently supported.

    >>> from numpy import allclose
    >>> from pyspark.ml.linalg import Vectors
    >>> from pyspark.ml.feature import StringIndexer
    >>> df = spark.createDataFrame([
    ...     (1.0, Vectors.dense(1.0)),
    ...     (0.0, Vectors.sparse(1, [], []))], ["label", "features"])
    >>> stringIndexer = StringIndexer(inputCol="label", outputCol="indexed")
    >>> si_model = stringIndexer.fit(df)
    >>> td = si_model.transform(df)
    >>> gbt = GBTClassifier(maxIter=5, maxDepth=2, labelCol="indexed", seed=42)
    >>> gbt.getFeatureSubsetStrategy()
    'all'
    >>> model = gbt.fit(td)
    >>> model.featureImportances
    SparseVector(1, {0: 1.0})
    >>> allclose(model.treeWeights, [1.0, 0.1, 0.1, 0.1, 0.1])
    True
    >>> test0 = spark.createDataFrame([(Vectors.dense(-1.0),)], ["features"])
    >>> model.transform(test0).head().prediction
    0.0
    >>> test1 = spark.createDataFrame([(Vectors.sparse(1, [0], [1.0]),)], ["features"])
    >>> model.transform(test1).head().prediction
    1.0
    >>> model.totalNumNodes
    15
    >>> print(model.toDebugString)
    GBTClassificationModel (uid=...)...with 5 trees...
    >>> gbtc_path = temp_path + "gbtc"
    >>> gbt.save(gbtc_path)
    >>> gbt2 = GBTClassifier.load(gbtc_path)
    >>> gbt2.getMaxDepth()
    2
    >>> model_path = temp_path + "gbtc_model"
    >>> model.save(model_path)
    >>> model2 = GBTClassificationModel.load(model_path)
    >>> model.featureImportances == model2.featureImportances
    True
    >>> model.treeWeights == model2.treeWeights
    True
    >>> model.trees
    [DecisionTreeRegressionModel (uid=...) of depth..., DecisionTreeRegressionModel...]
    >>> validation = spark.createDataFrame([(0.0, Vectors.dense(-1.0),)],
    ...              ["indexed", "features"])
    >>> model.evaluateEachIteration(validation)
    [0.25..., 0.23..., 0.21..., 0.19..., 0.18...]
    >>> model.numClasses
    2

    .. versionadded:: 1.4.0
    """

    lossType = Param(Params._dummy(), "lossType",
                     "Loss function which GBT tries to minimize (case-insensitive). " +
                     "Supported options: " + ", ".join(GBTParams.supportedLossTypes),
                     typeConverter=TypeConverters.toString)

    stepSize = Param(Params._dummy(), "stepSize",
                     "Step size (a.k.a. learning rate) in interval (0, 1] for shrinking " +
                     "the contribution of each estimator.",
                     typeConverter=TypeConverters.toFloat)

    @keyword_only
    def __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                 maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                 maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, lossType="logistic",
                 maxIter=20, stepSize=0.1, seed=None, subsamplingRate=1.0,
                 featureSubsetStrategy="all"):
        """
        __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0, \
                 maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, \
                 lossType="logistic", maxIter=20, stepSize=0.1, seed=None, subsamplingRate=1.0, \
                 featureSubsetStrategy="all")
        """
        super(GBTClassifier, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.classification.GBTClassifier", self.uid)
        self._setDefault(maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                         maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10,
                         lossType="logistic", maxIter=20, stepSize=0.1, subsamplingRate=1.0,
                         featureSubsetStrategy="all")
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                  maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                  maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10,
                  lossType="logistic", maxIter=20, stepSize=0.1, seed=None, subsamplingRate=1.0,
                  featureSubsetStrategy="all"):
        """
        setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                  maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0, \
                  maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, \
                  lossType="logistic", maxIter=20, stepSize=0.1, seed=None, subsamplingRate=1.0, \
                  featureSubsetStrategy="all")
        Sets params for Gradient Boosted Tree Classification.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return GBTClassificationModel(java_model)

    @since("1.4.0")
    def setLossType(self, value):
        """
        Sets the value of :py:attr:`lossType`.
        """
        return self._set(lossType=value)

    @since("1.4.0")
    def getLossType(self):
        """
        Gets the value of lossType or its default value.
        """
        return self.getOrDefault(self.lossType)

    @since("2.4.0")
    def setFeatureSubsetStrategy(self, value):
        """
        Sets the value of :py:attr:`featureSubsetStrategy`.
        """
        return self._set(featureSubsetStrategy=value)


class GBTClassificationModel(TreeEnsembleModel, JavaClassificationModel, JavaMLWritable,
                             JavaMLReadable):
    """
    Model fitted by GBTClassifier.

    .. versionadded:: 1.4.0
    """

    @property
    @since("2.0.0")
    def featureImportances(self):
        """
        Estimate of the importance of each feature.

        Each feature's importance is the average of its importance across all trees in the ensemble
        The importance vector is normalized to sum to 1. This method is suggested by Hastie et al.
        (Hastie, Tibshirani, Friedman. "The Elements of Statistical Learning, 2nd Edition." 2001.)
        and follows the implementation from scikit-learn.

        .. seealso:: :py:attr:`DecisionTreeClassificationModel.featureImportances`
        """
        return self._call_java("featureImportances")

    @property
    @since("2.0.0")
    def trees(self):
        """Trees in this ensemble. Warning: These have null parent Estimators."""
        return [DecisionTreeRegressionModel(m) for m in list(self._call_java("trees"))]

    @since("2.4.0")
    def evaluateEachIteration(self, dataset):
        """
        Method to compute error or loss for every iteration of gradient boosting.

        :param dataset:
            Test dataset to evaluate model on, where dataset is an
            instance of :py:class:`pyspark.sql.DataFrame`
        """
        return self._call_java("evaluateEachIteration", dataset)


@inherit_doc
class NaiveBayes(JavaEstimator, HasFeaturesCol, HasLabelCol, HasPredictionCol, HasProbabilityCol,
                 HasRawPredictionCol, HasThresholds, HasWeightCol, JavaMLWritable, JavaMLReadable):
    """
    Naive Bayes Classifiers.
    It supports both Multinomial and Bernoulli NB. `Multinomial NB
    <http://nlp.stanford.edu/IR-book/html/htmledition/naive-bayes-text-classification-1.html>`_
    can handle finitely supported discrete data. For example, by converting documents into
    TF-IDF vectors, it can be used for document classification. By making every vector a
    binary (0/1) data, it can also be used as `Bernoulli NB
    <http://nlp.stanford.edu/IR-book/html/htmledition/the-bernoulli-model-1.html>`_.
    The input feature values must be nonnegative.

    >>> from pyspark.sql import Row
    >>> from pyspark.ml.linalg import Vectors
    >>> df = spark.createDataFrame([
    ...     Row(label=0.0, weight=0.1, features=Vectors.dense([0.0, 0.0])),
    ...     Row(label=0.0, weight=0.5, features=Vectors.dense([0.0, 1.0])),
    ...     Row(label=1.0, weight=1.0, features=Vectors.dense([1.0, 0.0]))])
    >>> nb = NaiveBayes(smoothing=1.0, modelType="multinomial", weightCol="weight")
    >>> model = nb.fit(df)
    >>> model.pi
    DenseVector([-0.81..., -0.58...])
    >>> model.theta
    DenseMatrix(2, 2, [-0.91..., -0.51..., -0.40..., -1.09...], 1)
    >>> test0 = sc.parallelize([Row(features=Vectors.dense([1.0, 0.0]))]).toDF()
    >>> result = model.transform(test0).head()
    >>> result.prediction
    1.0
    >>> result.probability
    DenseVector([0.32..., 0.67...])
    >>> result.rawPrediction
    DenseVector([-1.72..., -0.99...])
    >>> test1 = sc.parallelize([Row(features=Vectors.sparse(2, [0], [1.0]))]).toDF()
    >>> model.transform(test1).head().prediction
    1.0
    >>> nb_path = temp_path + "/nb"
    >>> nb.save(nb_path)
    >>> nb2 = NaiveBayes.load(nb_path)
    >>> nb2.getSmoothing()
    1.0
    >>> model_path = temp_path + "/nb_model"
    >>> model.save(model_path)
    >>> model2 = NaiveBayesModel.load(model_path)
    >>> model.pi == model2.pi
    True
    >>> model.theta == model2.theta
    True
    >>> nb = nb.setThresholds([0.01, 10.00])
    >>> model3 = nb.fit(df)
    >>> result = model3.transform(test0).head()
    >>> result.prediction
    0.0

    .. versionadded:: 1.5.0
    """

    smoothing = Param(Params._dummy(), "smoothing", "The smoothing parameter, should be >= 0, " +
                      "default is 1.0", typeConverter=TypeConverters.toFloat)
    modelType = Param(Params._dummy(), "modelType", "The model type which is a string " +
                      "(case-sensitive). Supported options: multinomial (default) and bernoulli.",
                      typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                 probabilityCol="probability", rawPredictionCol="rawPrediction", smoothing=1.0,
                 modelType="multinomial", thresholds=None, weightCol=None):
        """
        __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 probabilityCol="probability", rawPredictionCol="rawPrediction", smoothing=1.0, \
                 modelType="multinomial", thresholds=None, weightCol=None)
        """
        super(NaiveBayes, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.classification.NaiveBayes", self.uid)
        self._setDefault(smoothing=1.0, modelType="multinomial")
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.5.0")
    def setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                  probabilityCol="probability", rawPredictionCol="rawPrediction", smoothing=1.0,
                  modelType="multinomial", thresholds=None, weightCol=None):
        """
        setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                  probabilityCol="probability", rawPredictionCol="rawPrediction", smoothing=1.0, \
                  modelType="multinomial", thresholds=None, weightCol=None)
        Sets params for Naive Bayes.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return NaiveBayesModel(java_model)

    @since("1.5.0")
    def setSmoothing(self, value):
        """
        Sets the value of :py:attr:`smoothing`.
        """
        return self._set(smoothing=value)

    @since("1.5.0")
    def getSmoothing(self):
        """
        Gets the value of smoothing or its default value.
        """
        return self.getOrDefault(self.smoothing)

    @since("1.5.0")
    def setModelType(self, value):
        """
        Sets the value of :py:attr:`modelType`.
        """
        return self._set(modelType=value)

    @since("1.5.0")
    def getModelType(self):
        """
        Gets the value of modelType or its default value.
        """
        return self.getOrDefault(self.modelType)


class NaiveBayesModel(JavaModel, JavaClassificationModel, JavaMLWritable, JavaMLReadable):
    """
    Model fitted by NaiveBayes.

    .. versionadded:: 1.5.0
    """

    @property
    @since("2.0.0")
    def pi(self):
        """
        log of class priors.
        """
        return self._call_java("pi")

    @property
    @since("2.0.0")
    def theta(self):
        """
        log of class conditional probabilities.
        """
        return self._call_java("theta")


@inherit_doc
class MultilayerPerceptronClassifier(JavaEstimator, HasFeaturesCol, HasLabelCol, HasPredictionCol,
                                     HasMaxIter, HasTol, HasSeed, HasStepSize, HasSolver,
                                     JavaMLWritable, JavaMLReadable, HasProbabilityCol,
                                     HasRawPredictionCol):
    """
    Classifier trainer based on the Multilayer Perceptron.
    Each layer has sigmoid activation function, output layer has softmax.
    Number of inputs has to be equal to the size of feature vectors.
    Number of outputs has to be equal to the total number of labels.

    >>> from pyspark.ml.linalg import Vectors
    >>> df = spark.createDataFrame([
    ...     (0.0, Vectors.dense([0.0, 0.0])),
    ...     (1.0, Vectors.dense([0.0, 1.0])),
    ...     (1.0, Vectors.dense([1.0, 0.0])),
    ...     (0.0, Vectors.dense([1.0, 1.0]))], ["label", "features"])
    >>> mlp = MultilayerPerceptronClassifier(maxIter=100, layers=[2, 2, 2], blockSize=1, seed=123)
    >>> model = mlp.fit(df)
    >>> model.layers
    [2, 2, 2]
    >>> model.weights.size
    12
    >>> testDF = spark.createDataFrame([
    ...     (Vectors.dense([1.0, 0.0]),),
    ...     (Vectors.dense([0.0, 0.0]),)], ["features"])
    >>> model.transform(testDF).select("features", "prediction").show()
    +---------+----------+
    | features|prediction|
    +---------+----------+
    |[1.0,0.0]|       1.0|
    |[0.0,0.0]|       0.0|
    +---------+----------+
    ...
    >>> mlp_path = temp_path + "/mlp"
    >>> mlp.save(mlp_path)
    >>> mlp2 = MultilayerPerceptronClassifier.load(mlp_path)
    >>> mlp2.getBlockSize()
    1
    >>> model_path = temp_path + "/mlp_model"
    >>> model.save(model_path)
    >>> model2 = MultilayerPerceptronClassificationModel.load(model_path)
    >>> model.layers == model2.layers
    True
    >>> model.weights == model2.weights
    True
    >>> mlp2 = mlp2.setInitialWeights(list(range(0, 12)))
    >>> model3 = mlp2.fit(df)
    >>> model3.weights != model2.weights
    True
    >>> model3.layers == model.layers
    True

    .. versionadded:: 1.6.0
    """

    layers = Param(Params._dummy(), "layers", "Sizes of layers from input layer to output layer " +
                   "E.g., Array(780, 100, 10) means 780 inputs, one hidden layer with 100 " +
                   "neurons and output layer of 10 neurons.",
                   typeConverter=TypeConverters.toListInt)
    blockSize = Param(Params._dummy(), "blockSize", "Block size for stacking input data in " +
                      "matrices. Data is stacked within partitions. If block size is more than " +
                      "remaining data in a partition then it is adjusted to the size of this " +
                      "data. Recommended size is between 10 and 1000, default is 128.",
                      typeConverter=TypeConverters.toInt)
    solver = Param(Params._dummy(), "solver", "The solver algorithm for optimization. Supported " +
                   "options: l-bfgs, gd.", typeConverter=TypeConverters.toString)
    initialWeights = Param(Params._dummy(), "initialWeights", "The initial weights of the model.",
                           typeConverter=TypeConverters.toVector)

    @keyword_only
    def __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                 maxIter=100, tol=1e-6, seed=None, layers=None, blockSize=128, stepSize=0.03,
                 solver="l-bfgs", initialWeights=None, probabilityCol="probability",
                 rawPredictionCol="rawPrediction"):
        """
        __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 maxIter=100, tol=1e-6, seed=None, layers=None, blockSize=128, stepSize=0.03, \
                 solver="l-bfgs", initialWeights=None, probabilityCol="probability", \
                 rawPredictionCol="rawPrediction")
        """
        super(MultilayerPerceptronClassifier, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.classification.MultilayerPerceptronClassifier", self.uid)
        self._setDefault(maxIter=100, tol=1E-6, blockSize=128, stepSize=0.03, solver="l-bfgs")
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.6.0")
    def setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                  maxIter=100, tol=1e-6, seed=None, layers=None, blockSize=128, stepSize=0.03,
                  solver="l-bfgs", initialWeights=None, probabilityCol="probability",
                  rawPredictionCol="rawPrediction"):
        """
        setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                  maxIter=100, tol=1e-6, seed=None, layers=None, blockSize=128, stepSize=0.03, \
                  solver="l-bfgs", initialWeights=None, probabilityCol="probability", \
                  rawPredictionCol="rawPrediction"):
        Sets params for MultilayerPerceptronClassifier.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return MultilayerPerceptronClassificationModel(java_model)

    @since("1.6.0")
    def setLayers(self, value):
        """
        Sets the value of :py:attr:`layers`.
        """
        return self._set(layers=value)

    @since("1.6.0")
    def getLayers(self):
        """
        Gets the value of layers or its default value.
        """
        return self.getOrDefault(self.layers)

    @since("1.6.0")
    def setBlockSize(self, value):
        """
        Sets the value of :py:attr:`blockSize`.
        """
        return self._set(blockSize=value)

    @since("1.6.0")
    def getBlockSize(self):
        """
        Gets the value of blockSize or its default value.
        """
        return self.getOrDefault(self.blockSize)

    @since("2.0.0")
    def setStepSize(self, value):
        """
        Sets the value of :py:attr:`stepSize`.
        """
        return self._set(stepSize=value)

    @since("2.0.0")
    def getStepSize(self):
        """
        Gets the value of stepSize or its default value.
        """
        return self.getOrDefault(self.stepSize)

    @since("2.0.0")
    def setInitialWeights(self, value):
        """
        Sets the value of :py:attr:`initialWeights`.
        """
        return self._set(initialWeights=value)

    @since("2.0.0")
    def getInitialWeights(self):
        """
        Gets the value of initialWeights or its default value.
        """
        return self.getOrDefault(self.initialWeights)


class MultilayerPerceptronClassificationModel(JavaModel, JavaClassificationModel, JavaMLWritable,
                                              JavaMLReadable):
    """
    Model fitted by MultilayerPerceptronClassifier.

    .. versionadded:: 1.6.0
    """

    @property
    @since("1.6.0")
    def layers(self):
        """
        array of layer sizes including input and output layers.
        """
        return self._call_java("javaLayers")

    @property
    @since("2.0.0")
    def weights(self):
        """
        the weights of layers.
        """
        return self._call_java("weights")


class OneVsRestParams(HasFeaturesCol, HasLabelCol, HasWeightCol, HasPredictionCol):
    """
    Parameters for OneVsRest and OneVsRestModel.
    """

    classifier = Param(Params._dummy(), "classifier", "base binary classifier")

    @since("2.0.0")
    def setClassifier(self, value):
        """
        Sets the value of :py:attr:`classifier`.

        .. note:: Only LogisticRegression and NaiveBayes are supported now.
        """
        return self._set(classifier=value)

    @since("2.0.0")
    def getClassifier(self):
        """
        Gets the value of classifier or its default value.
        """
        return self.getOrDefault(self.classifier)


@inherit_doc
class OneVsRest(Estimator, OneVsRestParams, HasParallelism, JavaMLReadable, JavaMLWritable):
    """
    .. note:: Experimental

    Reduction of Multiclass Classification to Binary Classification.
    Performs reduction using one against all strategy.
    For a multiclass classification with k classes, train k models (one per class).
    Each example is scored against all k models and the model with highest score
    is picked to label the example.

    >>> from pyspark.sql import Row
    >>> from pyspark.ml.linalg import Vectors
    >>> data_path = "data/mllib/sample_multiclass_classification_data.txt"
    >>> df = spark.read.format("libsvm").load(data_path)
    >>> lr = LogisticRegression(regParam=0.01)
    >>> ovr = OneVsRest(classifier=lr)
    >>> model = ovr.fit(df)
    >>> model.models[0].coefficients
    DenseVector([0.5..., -1.0..., 3.4..., 4.2...])
    >>> model.models[1].coefficients
    DenseVector([-2.1..., 3.1..., -2.6..., -2.3...])
    >>> model.models[2].coefficients
    DenseVector([0.3..., -3.4..., 1.0..., -1.1...])
    >>> [x.intercept for x in model.models]
    [-2.7..., -2.5..., -1.3...]
    >>> test0 = sc.parallelize([Row(features=Vectors.dense(-1.0, 0.0, 1.0, 1.0))]).toDF()
    >>> model.transform(test0).head().prediction
    0.0
    >>> test1 = sc.parallelize([Row(features=Vectors.sparse(4, [0], [1.0]))]).toDF()
    >>> model.transform(test1).head().prediction
    2.0
    >>> test2 = sc.parallelize([Row(features=Vectors.dense(0.5, 0.4, 0.3, 0.2))]).toDF()
    >>> model.transform(test2).head().prediction
    0.0
    >>> model_path = temp_path + "/ovr_model"
    >>> model.save(model_path)
    >>> model2 = OneVsRestModel.load(model_path)
    >>> model2.transform(test0).head().prediction
    0.0

    .. versionadded:: 2.0.0
    """

    @keyword_only
    def __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                 classifier=None, weightCol=None, parallelism=1):
        """
        __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 classifier=None, weightCol=None, parallelism=1):
        """
        super(OneVsRest, self).__init__()
        self._setDefault(parallelism=1)
        kwargs = self._input_kwargs
        self._set(**kwargs)

    @keyword_only
    @since("2.0.0")
    def setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                  classifier=None, weightCol=None, parallelism=1):
        """
        setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                  classifier=None, weightCol=None, parallelism=1):
        Sets params for OneVsRest.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _fit(self, dataset):
        labelCol = self.getLabelCol()
        featuresCol = self.getFeaturesCol()
        predictionCol = self.getPredictionCol()
        classifier = self.getClassifier()
        assert isinstance(classifier, HasRawPredictionCol),\
            "Classifier %s doesn't extend from HasRawPredictionCol." % type(classifier)

        numClasses = int(dataset.agg({labelCol: "max"}).head()["max("+labelCol+")"]) + 1

        weightCol = None
        if (self.isDefined(self.weightCol) and self.getWeightCol()):
            if isinstance(classifier, HasWeightCol):
                weightCol = self.getWeightCol()
            else:
                warnings.warn("weightCol is ignored, "
                              "as it is not supported by {} now.".format(classifier))

        if weightCol:
            multiclassLabeled = dataset.select(labelCol, featuresCol, weightCol)
        else:
            multiclassLabeled = dataset.select(labelCol, featuresCol)

        # persist if underlying dataset is not persistent.
        handlePersistence = dataset.storageLevel == StorageLevel(False, False, False, False)
        if handlePersistence:
            multiclassLabeled.persist(StorageLevel.MEMORY_AND_DISK)

        def trainSingleClass(index):
            binaryLabelCol = "mc2b$" + str(index)
            trainingDataset = multiclassLabeled.withColumn(
                binaryLabelCol,
                when(multiclassLabeled[labelCol] == float(index), 1.0).otherwise(0.0))
            paramMap = dict([(classifier.labelCol, binaryLabelCol),
                            (classifier.featuresCol, featuresCol),
                            (classifier.predictionCol, predictionCol)])
            if weightCol:
                paramMap[classifier.weightCol] = weightCol
            return classifier.fit(trainingDataset, paramMap)

        pool = ThreadPool(processes=min(self.getParallelism(), numClasses))

        models = pool.map(trainSingleClass, range(numClasses))

        if handlePersistence:
            multiclassLabeled.unpersist()

        return self._copyValues(OneVsRestModel(models=models))

    @since("2.0.0")
    def copy(self, extra=None):
        """
        Creates a copy of this instance with a randomly generated uid
        and some extra params. This creates a deep copy of the embedded paramMap,
        and copies the embedded and extra parameters over.

        :param extra: Extra parameters to copy to the new instance
        :return: Copy of this instance
        """
        if extra is None:
            extra = dict()
        newOvr = Params.copy(self, extra)
        if self.isSet(self.classifier):
            newOvr.setClassifier(self.getClassifier().copy(extra))
        return newOvr

    @classmethod
    def _from_java(cls, java_stage):
        """
        Given a Java OneVsRest, create and return a Python wrapper of it.
        Used for ML persistence.
        """
        featuresCol = java_stage.getFeaturesCol()
        labelCol = java_stage.getLabelCol()
        predictionCol = java_stage.getPredictionCol()
        classifier = JavaParams._from_java(java_stage.getClassifier())
        parallelism = java_stage.getParallelism()
        py_stage = cls(featuresCol=featuresCol, labelCol=labelCol, predictionCol=predictionCol,
                       classifier=classifier, parallelism=parallelism)
        py_stage._resetUid(java_stage.uid())
        return py_stage

    def _to_java(self):
        """
        Transfer this instance to a Java OneVsRest. Used for ML persistence.

        :return: Java object equivalent to this instance.
        """
        _java_obj = JavaParams._new_java_obj("org.apache.spark.ml.classification.OneVsRest",
                                             self.uid)
        _java_obj.setClassifier(self.getClassifier()._to_java())
        _java_obj.setParallelism(self.getParallelism())
        _java_obj.setFeaturesCol(self.getFeaturesCol())
        _java_obj.setLabelCol(self.getLabelCol())
        _java_obj.setPredictionCol(self.getPredictionCol())
        return _java_obj

    def _make_java_param_pair(self, param, value):
        """
        Makes a Java param pair.
        """
        sc = SparkContext._active_spark_context
        param = self._resolveParam(param)
        _java_obj = JavaParams._new_java_obj("org.apache.spark.ml.classification.OneVsRest",
                                             self.uid)
        java_param = _java_obj.getParam(param.name)
        if isinstance(value, JavaParams):
            # used in the case of an estimator having another estimator as a parameter
            # the reason why this is not in _py2java in common.py is that importing
            # Estimator and Model in common.py results in a circular import with inherit_doc
            java_value = value._to_java()
        else:
            java_value = _py2java(sc, value)
        return java_param.w(java_value)

    def _transfer_param_map_to_java(self, pyParamMap):
        """
        Transforms a Python ParamMap into a Java ParamMap.
        """
        paramMap = JavaWrapper._new_java_obj("org.apache.spark.ml.param.ParamMap")
        for param in self.params:
            if param in pyParamMap:
                pair = self._make_java_param_pair(param, pyParamMap[param])
                paramMap.put([pair])
        return paramMap

    def _transfer_param_map_from_java(self, javaParamMap):
        """
        Transforms a Java ParamMap into a Python ParamMap.
        """
        sc = SparkContext._active_spark_context
        paramMap = dict()
        for pair in javaParamMap.toList():
            param = pair.param()
            if self.hasParam(str(param.name())):
                if param.name() == "classifier":
                    paramMap[self.getParam(param.name())] = JavaParams._from_java(pair.value())
                else:
                    paramMap[self.getParam(param.name())] = _java2py(sc, pair.value())
        return paramMap


class OneVsRestModel(Model, OneVsRestParams, JavaMLReadable, JavaMLWritable):
    """
    .. note:: Experimental

    Model fitted by OneVsRest.
    This stores the models resulting from training k binary classifiers: one for each class.
    Each example is scored against all k models, and the model with the highest score
    is picked to label the example.

    .. versionadded:: 2.0.0
    """

    def __init__(self, models):
        super(OneVsRestModel, self).__init__()
        self.models = models
        java_models = [model._to_java() for model in self.models]
        sc = SparkContext._active_spark_context
        java_models_array = JavaWrapper._new_java_array(java_models,
                                                        sc._gateway.jvm.org.apache.spark.ml
                                                        .classification.ClassificationModel)
        # TODO: need to set metadata
        metadata = JavaParams._new_java_obj("org.apache.spark.sql.types.Metadata")
        self._java_obj = \
            JavaParams._new_java_obj("org.apache.spark.ml.classification.OneVsRestModel",
                                     self.uid, metadata.empty(), java_models_array)

    def _transform(self, dataset):
        # determine the input columns: these need to be passed through
        origCols = dataset.columns

        # add an accumulator column to store predictions of all the models
        accColName = "mbc$acc" + str(uuid.uuid4())
        initUDF = udf(lambda _: [], ArrayType(DoubleType()))
        newDataset = dataset.withColumn(accColName, initUDF(dataset[origCols[0]]))

        # persist if underlying dataset is not persistent.
        handlePersistence = dataset.storageLevel == StorageLevel(False, False, False, False)
        if handlePersistence:
            newDataset.persist(StorageLevel.MEMORY_AND_DISK)

        # update the accumulator column with the result of prediction of models
        aggregatedDataset = newDataset
        for index, model in enumerate(self.models):
            rawPredictionCol = model._call_java("getRawPredictionCol")
            columns = origCols + [rawPredictionCol, accColName]

            # add temporary column to store intermediate scores and update
            tmpColName = "mbc$tmp" + str(uuid.uuid4())
            updateUDF = udf(
                lambda predictions, prediction: predictions + [prediction.tolist()[1]],
                ArrayType(DoubleType()))
            transformedDataset = model.transform(aggregatedDataset).select(*columns)
            updatedDataset = transformedDataset.withColumn(
                tmpColName,
                updateUDF(transformedDataset[accColName], transformedDataset[rawPredictionCol]))
            newColumns = origCols + [tmpColName]

            # switch out the intermediate column with the accumulator column
            aggregatedDataset = updatedDataset\
                .select(*newColumns).withColumnRenamed(tmpColName, accColName)

        if handlePersistence:
            newDataset.unpersist()

        # output the index of the classifier with highest confidence as prediction
        labelUDF = udf(
            lambda predictions: float(max(enumerate(predictions), key=operator.itemgetter(1))[0]),
            DoubleType())

        # output label and label metadata as prediction
        return aggregatedDataset.withColumn(
            self.getPredictionCol(), labelUDF(aggregatedDataset[accColName])).drop(accColName)

    @since("2.0.0")
    def copy(self, extra=None):
        """
        Creates a copy of this instance with a randomly generated uid
        and some extra params. This creates a deep copy of the embedded paramMap,
        and copies the embedded and extra parameters over.

        :param extra: Extra parameters to copy to the new instance
        :return: Copy of this instance
        """
        if extra is None:
            extra = dict()
        newModel = Params.copy(self, extra)
        newModel.models = [model.copy(extra) for model in self.models]
        return newModel

    @classmethod
    def _from_java(cls, java_stage):
        """
        Given a Java OneVsRestModel, create and return a Python wrapper of it.
        Used for ML persistence.
        """
        featuresCol = java_stage.getFeaturesCol()
        labelCol = java_stage.getLabelCol()
        predictionCol = java_stage.getPredictionCol()
        classifier = JavaParams._from_java(java_stage.getClassifier())
        models = [JavaParams._from_java(model) for model in java_stage.models()]
        py_stage = cls(models=models).setPredictionCol(predictionCol).setLabelCol(labelCol)\
            .setFeaturesCol(featuresCol).setClassifier(classifier)
        py_stage._resetUid(java_stage.uid())
        return py_stage

    def _to_java(self):
        """
        Transfer this instance to a Java OneVsRestModel. Used for ML persistence.

        :return: Java object equivalent to this instance.
        """
        sc = SparkContext._active_spark_context
        java_models = [model._to_java() for model in self.models]
        java_models_array = JavaWrapper._new_java_array(
            java_models, sc._gateway.jvm.org.apache.spark.ml.classification.ClassificationModel)
        metadata = JavaParams._new_java_obj("org.apache.spark.sql.types.Metadata")
        _java_obj = JavaParams._new_java_obj("org.apache.spark.ml.classification.OneVsRestModel",
                                             self.uid, metadata.empty(), java_models_array)
        _java_obj.set("classifier", self.getClassifier()._to_java())
        _java_obj.set("featuresCol", self.getFeaturesCol())
        _java_obj.set("labelCol", self.getLabelCol())
        _java_obj.set("predictionCol", self.getPredictionCol())
        return _java_obj


if __name__ == "__main__":
    import doctest
    import pyspark.ml.classification
    from pyspark.sql import SparkSession
    globs = pyspark.ml.classification.__dict__.copy()
    # The small batch size here ensures that we see multiple batches,
    # even in these small test examples:
    spark = SparkSession.builder\
        .master("local[2]")\
        .appName("ml.classification tests")\
        .getOrCreate()
    sc = spark.sparkContext
    globs['sc'] = sc
    globs['spark'] = spark
    import tempfile
    temp_path = tempfile.mkdtemp()
    globs['temp_path'] = temp_path
    try:
        (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
        spark.stop()
    finally:
        from shutil import rmtree
        try:
            rmtree(temp_path)
        except OSError:
            pass
    if failure_count:
        sys.exit(-1)
