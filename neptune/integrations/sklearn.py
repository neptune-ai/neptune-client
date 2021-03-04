#
# Copyright (c) 2021, Neptune Labs Sp. z o.o.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from io import BytesIO
import pickle
from neptune.alpha.experiment import Experiment
from neptune.exceptions import NotNeptuneExperimentException

import matplotlib.pyplot as plt
import pandas as pd
from scikitplot.estimators import plot_learning_curve
from scikitplot.metrics import plot_precision_recall
from sklearn.base import is_regressor, is_classifier
from sklearn.cluster import KMeans
from sklearn.metrics import explained_variance_score, max_error, mean_absolute_error, r2_score, \
    precision_recall_fscore_support
from yellowbrick.classifier import ClassificationReport, ConfusionMatrix, ROCAUC, ClassPredictionError
from yellowbrick.cluster import SilhouetteVisualizer, KElbowVisualizer
from yellowbrick.model_selection import FeatureImportances
from yellowbrick.regressor import ResidualsPlot, PredictionError, CooksDistance
from neptunecontrib.api.table import log_csv
from neptunecontrib.api.utils import log_pickle

# INFO
# Yellowbrick and scikitplot are required
# pip install yellowbrick>=1.3
# pip install scikit-plot>=0.3.7


def log_regressor_summary(experiment, regressor, X_train, X_test, y_train, y_test,
                          model_name=None, nrows=1000, log_charts=True):
    """Log sklearn regressor summary.

    This method automatically logs all regressor parameters, pickled estimator (model),
    test predictions as table, model performance visualizations and test metrics.

    Regressor should be fitted before calling this function.

    Tip:
        Check Sklearn-Neptune integration
        `documentation <https://docs-beta.neptune.ai/essentials/integrations/machine-learning-frameworks/sklearn>`_
        for the full example.

    Args:
        experiment (:obj:`neptune.experiment.Experiment`):
            | Neptune ``Experiment`` to control to which experiment you log the data.
            | Create one by ``exp = neptune.init(project='my_workspace/my_project')``.
        regressor (:obj:`regressor`):
            | Fitted sklearn regressor object
        X_train (:obj:`ndarray`):
            | Training data matrix
        X_test (:obj:`ndarray`):
            | Testing data matrix
        y_train (:obj:`ndarray`):
            | The regression target for training
        y_test (:obj:`ndarray`):
            | The regression target for testing
        model_name (`str`, optional, default is ``None``):
            | Name of the pickled model file to upload.
            | If ``None``, then ``estimator.skl`` is used.
            | The model will be logged under namespace: ``model/<model_name>``.
        nrows (`int`, optional, default is 1000):
            | Log first ``nrows`` rows of test predictions.
        log_charts (:bool:, optional, default is ``True``):
            | If True, calculate and send chart visualizations.
            |
            | NOTE: calculating visualizations is potentially expensive depending on input data and regressor, and
            | may take some time to finish.
            |
            | This is equivalent to calling log_learning_curve_chart, log_feature_importance_chart,
            | log_residuals_chart, log_prediction_error_chart, log_cooks_distance_chart functions from this module.
            |
            | If not all visualizations are needed, it's recommended to set this parameter to ``False`` and call
            | only the desired log functions

    Returns:
        ``None``

    Examples:
        Log random forest regressor summary

        .. code:: python3

            rfr = RandomForestRegressor()
            rfr.fit(X_train, y_train)

            exp = neptune.init(project='my_workspace/my_project')

            log_regressor_summary(exp, rfr, X_train, X_test, y_train, y_test)
    """
    assert is_regressor(regressor), 'regressor should be sklearn regressor.'

    _validate_experiment(experiment)

    log_estimator_params(experiment, regressor)
    log_pickled_model(regressor, model_name, experiment)

    y_pred = regressor.predict(X_test)
    log_test_predictions(regressor, X_test, y_test, y_pred=y_pred, nrows=nrows, experiment=exp)
    log_scores(regressor, X_test, y_test, y_pred=y_pred, name='test', experiment=exp)

    # visualizations
    if log_charts:
        log_learning_curve_chart(regressor, X_train, y_train, experiment=exp)
        log_feature_importance_chart(regressor, X_train, y_train, experiment=exp)
        log_residuals_chart(regressor, X_train, X_test, y_train, y_test, experiment=exp)
        log_prediction_error_chart(regressor, X_train, X_test, y_train, y_test, experiment=exp)
        log_cooks_distance_chart(regressor, X_train, y_train, experiment=exp)


def log_classifier_summary(classifier, X_train, X_test, y_train, y_test,
                           model_name=None, nrows=1000, experiment=None, log_charts=True):
    """Log sklearn classifier summary.

    This method automatically logs all classifier parameters, pickled estimator (model),
    test predictions, predictions probabilities as table, model performance visualizations and test metrics.

    Classifier should be fitted before calling this function.

    Make sure you created an experiment by using ``neptune.create_experiment()`` before you use this method.

    Tip:
        Check `Neptune documentation <https://docs.neptune.ai/integrations/scikit_learn.html>`_ for the full example.

    Args:
        classifier (:obj:`classifier`):
            | Fitted sklearn classifier object
        X_train (:obj:`ndarray`):
            | Training data matrix
        X_test (:obj:`ndarray`):
            | Testing data matrix
        y_train (:obj:`ndarray`):
            | The classification target for training
        y_test (:obj:`ndarray`):
            | The classification target for testing
        model_name (`str`, optional, default is ``None``):
            | If logging picked model, define a name of the file to be logged to `model/<model_name>`
            | If ``None`` - `estimator.skl` is used.
        nrows (`int`, optional, default is 1000):
            | Log first ``nrows`` rows of test predictions and predictions probabilities.
        experiment (:obj:`neptune.experiments.Experiment`, optional, default is ``None``):
            | Neptune ``Experiment`` object to control to which experiment you log the data.
            | If ``None``, log to currently active, and most recent experiment.
        log_charts (:bool:, optional, default is ``True``):
            | If True, calculate and send chart visualizations.
            |
            | NOTE: calculating visualizations is potentially expensive depending on input data and classifier, and
            | may take some time to finish.
            |
            | This is equivalent to calling log_classification_report_chart, log_confusion_matrix_chart,
            | log_roc_auc_chart, log_precision_recall_chart, log_class_prediction_error_chart functions from this
            | module.
            |
            | If not all visualizations are needed, it's recommended to set this parameter to ``False`` and call
            | only the desired log functions

    Returns:
        ``None``

    Examples:
        Log random forest classifier summary

        .. code:: python3

            rfc = RandomForestClassifier()
            rfc.fit(X_train, y_train)

            neptune.init('my_workspace/my_project')
            neptune.create_experiment()

            log_classifier_summary(rfc, X_train, X_test, y_train, y_test)
    """
    assert is_classifier(classifier), 'classifier should be sklearn classifier.'

    exp = _validate_experiment(experiment)

    log_estimator_params(classifier, exp)
    log_pickled_model(classifier, model_name, exp)
    log_test_preds_proba(classifier, X_test, nrows=nrows, experiment=exp)

    y_pred = classifier.predict(X_test)
    log_test_predictions(classifier, X_test, y_test, y_pred=y_pred, nrows=nrows, experiment=exp)
    log_scores(classifier, X_test, y_test, y_pred=y_pred, name='test', experiment=exp)

    # visualizations
    if log_charts:
        log_classification_report_chart(classifier, X_train, X_test, y_train, y_test, experiment=exp)
        log_confusion_matrix_chart(classifier, X_train, X_test, y_train, y_test, experiment=exp)
        log_roc_auc_chart(classifier, X_train, X_test, y_train, y_test, experiment=exp)
        log_precision_recall_chart(classifier, X_test, y_test, experiment=exp)
        log_class_prediction_error_chart(classifier, X_train, X_test, y_train, y_test, experiment=exp)


def log_estimator_params(experiment, estimator):
    """Log estimator parameters.

    Log all estimator parameters in 'all_params/' namespace.

    Tip:
        Check Sklearn-Neptune integration
        `documentation <https://docs-beta.neptune.ai/essentials/integrations/machine-learning-frameworks/sklearn>`_
        for the full example.

    Args:
        experiment (:obj:`neptune.experiment.Experiment`):
            | Neptune ``Experiment`` to control to which experiment you log the data.
            | Create one by ``exp = neptune.init(project='my_workspace/my_project')``.
        estimator (:obj:`estimator`):
            | Scikit-learn estimator from which to log parameters.

    Returns:
        ``None``

    Examples:
        .. code:: python3

            rfr = RandomForestRegressor()
            exp = neptune.init(project='my_workspace/my_project')

            log_estimator_params(exp, rfr)
    """
    assert is_regressor(estimator) or is_classifier(estimator) or isinstance(estimator, KMeans),\
        'Estimator should be sklearn regressor, classifier or kmeans clusterer.'

    _validate_experiment(experiment)

    experiment['all_params'] = estimator.get_params()


def log_pickled_model(experiment, estimator, model_name=None):
    """Log pickled estimator.

    Log estimator as pickled file.

    Estimator should be fitted before calling this function.

    The model will be logged under namespace: ``model/<model_name>``,
    by default: ``model/estimator.skl``.

    Tip:
        Check Sklearn-Neptune integration
        `documentation <https://docs-beta.neptune.ai/essentials/integrations/machine-learning-frameworks/sklearn>`_
        for the full example.

    Args:
        experiment (:obj:`neptune.experiment.Experiment`):
            | Neptune ``Experiment`` to control to which experiment you log the data.
            | Create one by ``exp = neptune.init(project='my_workspace/my_project')``.
        estimator (:obj:`estimator`):
            | Scikit-learn estimator to log.
        model_name (`str`, optional, default is ``None``):
            | Name of the pickled model file to upload.
            | If ``None``, then ``estimator.skl`` is used.
            | The model will be logged under namespace: ``model/<model_name>``.

    Returns:
        ``None``

    Examples:
        .. code:: python3

            rfr = RandomForestRegressor()
            rfr.fit(X_train, y_train)

            exp = neptune.init(project='my_workspace/my_project')

            log_pickled_model(exp, rfr)
    """
    assert is_regressor(estimator) or is_classifier(estimator),\
        'Estimator should be sklearn regressor or classifier.'
    assert isinstance(model_name, str) or model_name is None, 'model_name should be str,' \
                                                              ' {} was passed instead.'.format(type(model_name))

    _validate_experiment(experiment)

    buffer = BytesIO()
    pickle.dump(estimator, buffer)
    buffer.seek(0)

    if model_name:
        experiment['model/{}'.format(model_name)] = buffer
    else:
        experiment['model/estimator.skl'] = buffer
    experiment.wait()
    buffer.close()


def log_test_predictions(estimator, X_test, y_test, y_pred=None, nrows=1000, experiment=None):
    """Log test predictions.

    Calculate and log test predictions and have them as csv file in the Neptune artifacts.

    If you pass ``y_pred``, then predictions are logged without computing from ``X_test`` data.

    Estimator should be fitted before calling this function.

    Path to predictions in the Neptune artifacts is 'csv/test_predictions.csv'.

    Make sure you created an experiment by using ``neptune.create_experiment()`` before you use this method.

    Tip:
        Check `Neptune documentation <https://docs.neptune.ai/integrations/scikit_learn.html>`_ for the full example.

    Args:
        estimator (:obj:`estimator`):
            | Scikit-learn estimator to compute predictions.
        X_test (:obj:`ndarray`):
            | Testing data matrix.
        y_test (:obj:`ndarray`):
            | Target for testing.
        y_pred (:obj:`ndarray`, optional, default is ``None``):
            | Estimator predictions on test data.
        nrows (`int`, optional, default is 1000):
            | Number of rows to log.
        experiment (:obj:`neptune.experiments.Experiment`, optional, default is ``None``):
            | Neptune ``Experiment`` object to control to which experiment you log the data.
            | If ``None``, log to currently active, and most recent experiment.

    Returns:
        ``None``

    Examples:
        .. code:: python3

            rfr = RandomForestRegressor()
            rfr.fit(X_train, y_train)

            neptune.init('my_workspace/my_project')
            neptune.create_experiment()

            log_test_predictions(rfr, X_test, y_test)
    """
    assert is_regressor(estimator) or is_classifier(estimator),\
        'Estimator should be sklearn regressor or classifier.'
    assert isinstance(nrows, int), 'nrows should be integer, {} was passed'.format(type(nrows))

    exp = _validate_experiment(experiment)

    if y_pred is None:
        y_pred = estimator.predict(X_test)

    # single output
    if len(y_pred.shape) == 1:
        df = pd.DataFrame(data={'y_true': y_test, 'y_pred': y_pred})
        log_csv('test_predictions', df.head(nrows), exp)

    # multi output
    if len(y_pred.shape) == 2:
        df = pd.DataFrame()
        for j in range(y_pred.shape[1]):
            df['y_test_output_{}'.format(j)] = y_test[:, j]
            df['y_pred_output_{}'.format(j)] = y_pred[:, j]
        log_csv('test_predictions', df.head(nrows), exp)


def log_test_preds_proba(classifier, X_test, y_pred_proba=None, nrows=1000, experiment=None):
    """Log test predictions probabilities.

    Calculate and log test predictions probabilities and have them as csv file in the Neptune artifacts.

    If you pass ``y_pred_proba``, then predictions probabilities are logged without computing from ``X_test`` data.

    Estimator should be fitted before calling this function.

    Path to predictions probabilities in the Neptune artifacts is 'csv/test_preds_proba.csv'.

    Make sure you created an experiment by using ``neptune.create_experiment()`` before you use this method.

    Tip:
        Check `Neptune documentation <https://docs.neptune.ai/integrations/scikit_learn.html>`_ for the full example.

    Args:
        classifier (:obj:`classifier`):
            | Scikit-learn classifier to compute predictions probabilities.
        X_test (:obj:`ndarray`):
            | Testing data matrix.
        y_pred_proba (:obj:`ndarray`, optional, default is ``None``):
            | Classifier predictions probabilities on test data.
        nrows (`int`, optional, default is 1000):
            | Number of rows to log.
        experiment (:obj:`neptune.experiments.Experiment`, optional, default is ``None``):
            | Neptune ``Experiment`` object to control to which experiment you log the data.
            | If ``None``, log to currently active, and most recent experiment.

    Returns:
        ``None``

    Examples:
        .. code:: python3

            rfc = RandomForestClassifier()
            rfc.fit(X_train, y_train)

            neptune.init('my_workspace/my_project')
            neptune.create_experiment()

            log_test_preds_proba(rfc, X_test, y_test)
    """
    assert is_classifier(classifier), 'Classifier should be sklearn classifier.'
    assert isinstance(nrows, int), 'nrows should be integer, {} was passed'.format(type(nrows))

    exp = _validate_experiment(experiment)

    if y_pred_proba is None:
        try:
            y_pred_proba = classifier.predict_proba(X_test)
        except Exception as e:
            print('This classifier does not provide predictions probabilities. Error: {}'.format(e))
            return

    df = pd.DataFrame(data=y_pred_proba, columns=classifier.classes_)
    log_csv('test_preds_proba', df.head(nrows), exp)


def log_scores(estimator, X, y, y_pred=None, name=None, experiment=None):
    """Log estimator scores on ``X``.

    Calculate and log scores on data and have them as metrics in Neptune.
    If you pass ``y_pred``, then predictions are not computed from ``X`` data.

    Estimator should be fitted before calling this function.

    Make sure you created an experiment by using ``neptune.create_experiment()`` before you use this method.

    **Regressor**

    For regressors that outputs single value, following scores are logged:

    * explained variance
    * max error
    * mean absolute error
    * r2

    For multi-output regressor:

    * r2

    **Classifier**

    For classifier, following scores are logged:

    * precision
    * recall
    * f beta score
    * support

    Tip:
        Check `Neptune documentation <https://docs.neptune.ai/integrations/scikit_learn.html>`_ for the full example.

    Args:
        estimator (:obj:`estimator`):
            | Scikit-learn estimator to compute scores.
        X (:obj:`ndarray`):
            | Data matrix.
        y (:obj:`ndarray`):
            | Target for testing.
        y_pred (:obj:`ndarray`, optional, default is ``None``):
            | Estimator predictions on data.
        name (`str`, optional, default is ``None``):
            | Use 'train', 'valid', 'test' to better define on what data scores are logged.
        experiment (:obj:`neptune.experiments.Experiment`, optional, default is ``None``):
            | Neptune ``Experiment`` object to control to which experiment you log the data.
            | If ``None``, log to currently active, and most recent experiment.

    Returns:
        ``None``

    Examples:
        .. code:: python3

            rfc = RandomForestClassifier()
            rfc.fit(X_train, y_train)

            neptune.init('my_workspace/my_project')
            neptune.create_experiment()

            log_scores(rfc, X, y, name='test', experiment=exp)
    """
    assert is_regressor(estimator) or is_classifier(estimator),\
        'Estimator should be sklearn regressor or classifier.'
    assert isinstance(name, str), 'name should be str. {} was passed.'.format(type(name))

    exp = _validate_experiment(experiment)

    if y_pred is None:
        y_pred = estimator.predict(X)

    if is_regressor(estimator):
        # single output
        if len(y_pred.shape) == 1:
            evs = explained_variance_score(y, y_pred)
            me = max_error(y, y_pred)
            mae = mean_absolute_error(y, y_pred)
            r2 = r2_score(y, y_pred)

            exp.log_metric('evs_{}_sklearn'.format(name), evs)
            exp.log_metric('me_{}_sklearn'.format(name), me)
            exp.log_metric('mae_{}_sklearn'.format(name), mae)
            exp.log_metric('r2_{}_sklearn'.format(name), r2)

        # multi output
        if len(y_pred.shape) == 2:
            r2 = estimator.score(X, y)
            exp.log_metric('r2_{}_sklearn'.format(name), r2)
    elif is_classifier(estimator):
        for metric_name, values in zip(['precision', 'recall', 'fbeta_score', 'support'],
                                       precision_recall_fscore_support(y, y_pred)):
            for i, value in enumerate(values):
                exp.log_metric('{}_class_{}_{}_sklearn'.format(metric_name, i, name), value)


def log_learning_curve_chart(regressor, X_train, y_train, experiment=None):
    """Log learning curve chart.

    Make sure you created an experiment by using ``neptune.create_experiment()`` before you use this method.

    Tip:
        Check `Neptune documentation <https://docs.neptune.ai/integrations/scikit_learn.html>`_ for the full example.

    Args:
        regressor (:obj:`regressor`):
            | Fitted sklearn regressor object
        X_train (:obj:`ndarray`):
            | Training data matrix
        y_train (:obj:`ndarray`):
            | The regression target for training
        experiment (:obj:`neptune.experiments.Experiment`, optional, default is ``None``):
            | Neptune ``Experiment`` object to control to which experiment you log the data.
            | If ``None``, log to currently active, and most recent experiment.

    Returns:
        ``None``

    Examples:
        .. code:: python3

            rfr = RandomForestRegressor()
            rfr.fit(X_train, y_train)

            neptune.init('my_workspace/my_project')
            neptune.create_experiment()

            log_learning_curve_chart(rfr, X_train, y_train)
    """
    assert is_regressor(regressor), 'regressor should be sklearn regressor.'
    exp = _validate_experiment(experiment)

    try:
        fig, ax = plt.subplots()
        plot_learning_curve(regressor, X_train, y_train, ax=ax)
        exp.log_image('charts_sklearn', fig, image_name='Learning Curve')
        plt.close(fig)
    except Exception as e:
        print('Did not log learning curve chart. Error: {}'.format(e))


def log_feature_importance_chart(regressor, X_train, y_train, experiment=None):
    """Log feature importance chart.

    Make sure you created an experiment by using ``neptune.create_experiment()`` before you use this method.

    Tip:
        Check `Neptune documentation <https://docs.neptune.ai/integrations/scikit_learn.html>`_ for the full example.

    Args:
        regressor (:obj:`regressor`):
            | Fitted sklearn regressor object
        X_train (:obj:`ndarray`):
            | Training data matrix
        y_train (:obj:`ndarray`):
            | The regression target for training
        experiment (:obj:`neptune.experiments.Experiment`, optional, default is ``None``):
            | Neptune ``Experiment`` object to control to which experiment you log the data.
            | If ``None``, log to currently active, and most recent experiment.

    Returns:
        ``None``

    Examples:
        .. code:: python3

            rfr = RandomForestRegressor()
            rfr.fit(X_train, y_train)

            neptune.init('my_workspace/my_project')
            neptune.create_experiment()

            log_feature_importance_chart(rfr, X_train, y_train)
    """
    assert is_regressor(regressor), 'regressor should be sklearn regressor.'
    exp = _validate_experiment(experiment)

    try:
        fig, ax = plt.subplots()
        visualizer = FeatureImportances(regressor, is_fitted=True, ax=ax)
        visualizer.fit(X_train, y_train)
        visualizer.finalize()
        exp.log_image('charts_sklearn', fig, image_name='Feature Importance')
        plt.close(fig)
    except Exception as e:
        print('Did not log feature importance chart. Error: {}'.format(e))


def log_residuals_chart(regressor, X_train, X_test, y_train, y_test, experiment=None):
    """Log residuals chart.

    Make sure you created an experiment by using ``neptune.create_experiment()`` before you use this method.

    Tip:
        Check `Neptune documentation <https://docs.neptune.ai/integrations/scikit_learn.html>`_ for the full example.

    Args:
        regressor (:obj:`regressor`):
            | Fitted sklearn regressor object
        X_train (:obj:`ndarray`):
            | Training data matrix
        X_test (:obj:`ndarray`):
            | Testing data matrix
        y_train (:obj:`ndarray`):
            | The regression target for training
        y_test (:obj:`ndarray`):
            | The regression target for testing
        experiment (:obj:`neptune.experiments.Experiment`, optional, default is ``None``):
            | Neptune ``Experiment`` object to control to which experiment you log the data.
            | If ``None``, log to currently active, and most recent experiment.

    Returns:
        ``None``

    Examples:
        .. code:: python3

            rfr = RandomForestRegressor()
            rfr.fit(X_train, y_train)

            neptune.init('my_workspace/my_project')
            exp = neptune.create_experiment()

            log_residuals_chart(rfr, X_train, X_test, y_train, y_test, experiment=exp)
    """
    assert is_regressor(regressor), 'regressor should be sklearn regressor.'
    exp = _validate_experiment(experiment)

    try:
        fig, ax = plt.subplots()
        visualizer = ResidualsPlot(regressor, is_fitted=True, ax=ax)
        visualizer.fit(X_train, y_train)
        visualizer.score(X_test, y_test)
        visualizer.finalize()
        exp.log_image('charts_sklearn', fig, image_name='Residuals Plot')
        plt.close(fig)
    except Exception as e:
        print('Did not log residuals chart. Error: {}'.format(e))


def log_prediction_error_chart(regressor, X_train, X_test, y_train, y_test, experiment=None):
    """Log prediction error chart.

    Make sure you created an experiment by using ``neptune.create_experiment()`` before you use this method.

    Tip:
        Check `Neptune documentation <https://docs.neptune.ai/integrations/scikit_learn.html>`_ for the full example.

    Args:
        regressor (:obj:`regressor`):
            | Fitted sklearn regressor object
        X_train (:obj:`ndarray`):
            | Training data matrix
        X_test (:obj:`ndarray`):
            | Testing data matrix
        y_train (:obj:`ndarray`):
            | The regression target for training
        y_test (:obj:`ndarray`):
            | The regression target for testing
        experiment (:obj:`neptune.experiments.Experiment`, optional, default is ``None``):
            | Neptune ``Experiment`` object to control to which experiment you log the data.
            | If ``None``, log to currently active, and most recent experiment.

    Returns:
        ``None``

    Examples:
        .. code:: python3

            rfr = RandomForestRegressor()
            rfr.fit(X_train, y_train)

            neptune.init('my_workspace/my_project')
            neptune.create_experiment()

            log_prediction_error_chart(rfr, X_train, X_test, y_train, y_test)
    """
    assert is_regressor(regressor), 'regressor should be sklearn regressor.'
    exp = _validate_experiment(experiment)

    try:
        fig, ax = plt.subplots()
        visualizer = PredictionError(regressor, is_fitted=True, ax=ax)
        visualizer.fit(X_train, y_train)
        visualizer.score(X_test, y_test)
        visualizer.finalize()
        exp.log_image('charts_sklearn', fig, image_name='Prediction Error')
        plt.close(fig)
    except Exception as e:
        print('Did not log prediction error chart. Error: {}'.format(e))


def log_cooks_distance_chart(regressor, X_train, y_train, experiment=None):
    """Log feature importance chart.

    Make sure you created an experiment by using ``neptune.create_experiment()`` before you use this method.

    Tip:
        Check `Neptune documentation <https://docs.neptune.ai/integrations/scikit_learn.html>`_ for the full example.

    Args:
        regressor (:obj:`regressor`):
            | Fitted sklearn regressor object
        X_train (:obj:`ndarray`):
            | Training data matrix
        y_train (:obj:`ndarray`):
            | The regression target for training
        experiment (:obj:`neptune.experiments.Experiment`, optional, default is ``None``):
            | Neptune ``Experiment`` object to control to which experiment you log the data.
            | If ``None``, log to currently active, and most recent experiment.

    Returns:
        ``None``

    Examples:
        .. code:: python3

            rfr = RandomForestRegressor()
            rfr.fit(X_train, y_train)

            neptune.init('my_workspace/my_project')
            neptune.create_experiment()

            log_cooks_distance_chart(rfr, X_train, y_train)
    """
    assert is_regressor(regressor), 'regressor should be sklearn regressor.'
    exp = _validate_experiment(experiment)

    try:
        fig, ax = plt.subplots()
        visualizer = CooksDistance(ax=ax)
        visualizer.fit(X_train, y_train)
        visualizer.finalize()
        exp.log_image('charts_sklearn', fig, image_name='Cooks Distance')
        plt.close(fig)
    except Exception as e:
        print('Did not log cooks distance chart. Error: {}'.format(e))


def log_classification_report_chart(classifier, X_train, X_test, y_train, y_test, experiment=None):
    """Log classification report chart.

    Make sure you created an experiment by using ``neptune.create_experiment()`` before you use this method.

    Tip:
        Check `Neptune documentation <https://docs.neptune.ai/integrations/scikit_learn.html>`_ for the full example.

    Args:
        classifier (:obj:`classifier`):
            | Fitted sklearn classifier object
        X_train (:obj:`ndarray`):
            | Training data matrix
        X_test (:obj:`ndarray`):
            | Testing data matrix
        y_train (:obj:`ndarray`):
            | The classification target for training
        y_test (:obj:`ndarray`):
            | The classification target for testing
        experiment (:obj:`neptune.experiments.Experiment`, optional, default is ``None``):
            | Neptune ``Experiment`` object to control to which experiment you log the data.
            | If ``None``, log to currently active, and most recent experiment.

    Returns:
        ``None``

    Examples:
        .. code:: python3

            rfc = RandomForestClassifier()
            rfc.fit(X_train, y_train)

            neptune.init('my_workspace/my_project')
            exp = neptune.create_experiment()

            log_classification_report_chart(rfc, X_train, X_test, y_train, y_test, experiment=exp)
    """
    assert is_classifier(classifier), 'classifier should be sklearn classifier.'
    exp = _validate_experiment(experiment)

    try:
        fig, ax = plt.subplots()
        visualizer = ClassificationReport(classifier, support=True, is_fitted=True, ax=ax)
        visualizer.fit(X_train, y_train)
        visualizer.score(X_test, y_test)
        visualizer.finalize()
        exp.log_image('charts_sklearn', fig, image_name='Classification Report')
        plt.close(fig)
    except Exception as e:
        print('Did not log Classification Report chart. Error: {}'.format(e))


def log_confusion_matrix_chart(classifier, X_train, X_test, y_train, y_test, experiment=None):
    """Log confusion matrix.

    Make sure you created an experiment by using ``neptune.create_experiment()`` before you use this method.

    Tip:
        Check `Neptune documentation <https://docs.neptune.ai/integrations/scikit_learn.html>`_ for the full example.

    Args:
        classifier (:obj:`classifier`):
            | Fitted sklearn classifier object
        X_train (:obj:`ndarray`):
            | Training data matrix
        X_test (:obj:`ndarray`):
            | Testing data matrix
        y_train (:obj:`ndarray`):
            | The classification target for training
        y_test (:obj:`ndarray`):
            | The classification target for testing
        experiment (:obj:`neptune.experiments.Experiment`, optional, default is ``None``):
            | Neptune ``Experiment`` object to control to which experiment you log the data.
            | If ``None``, log to currently active, and most recent experiment.

    Returns:
        ``None``

    Examples:
        .. code:: python3

            rfc = RandomForestClassifier()
            rfc.fit(X_train, y_train)

            neptune.init('my_workspace/my_project')
            neptune.create_experiment()

            log_confusion_matrix_chart(rfc, X_train, X_test, y_train, y_test)
    """
    assert is_classifier(classifier), 'classifier should be sklearn classifier.'
    exp = _validate_experiment(experiment)

    try:
        fig, ax = plt.subplots()
        visualizer = ConfusionMatrix(classifier, is_fitted=True, ax=ax)
        visualizer.fit(X_train, y_train)
        visualizer.score(X_test, y_test)
        visualizer.finalize()
        exp.log_image('charts_sklearn', fig, image_name='Confusion Matrix')
        plt.close(fig)
    except Exception as e:
        print('Did not log Confusion Matrix chart. Error: {}'.format(e))


def log_roc_auc_chart(classifier, X_train, X_test, y_train, y_test, experiment=None):
    """Log ROC-AUC chart.

    Make sure you created an experiment by using ``neptune.create_experiment()`` before you use this method.

    Tip:
        Check `Neptune documentation <https://docs.neptune.ai/integrations/scikit_learn.html>`_ for the full example.

    Args:
        classifier (:obj:`classifier`):
            | Fitted sklearn classifier object
        X_train (:obj:`ndarray`):
            | Training data matrix
        X_test (:obj:`ndarray`):
            | Testing data matrix
        y_train (:obj:`ndarray`):
            | The classification target for training
        y_test (:obj:`ndarray`):
            | The classification target for testing
        experiment (:obj:`neptune.experiments.Experiment`, optional, default is ``None``):
            | Neptune ``Experiment`` object to control to which experiment you log the data.
            | If ``None``, log to currently active, and most recent experiment.

    Returns:
        ``None``

    Examples:
        .. code:: python3

            rfc = RandomForestClassifier()
            rfc.fit(X_train, y_train)

            neptune.init('my_workspace/my_project')
            exp = neptune.create_experiment()

            log_roc_auc_chart(rfc, X_train, X_test, y_train, y_test, experiment=exp)
    """
    assert is_classifier(classifier), 'classifier should be sklearn classifier.'
    exp = _validate_experiment(experiment)

    try:
        fig, ax = plt.subplots()
        visualizer = ROCAUC(classifier, is_fitted=True, ax=ax)
        visualizer.fit(X_train, y_train)
        visualizer.score(X_test, y_test)
        visualizer.finalize()
        exp.log_image('charts_sklearn', fig, image_name='ROC-AUC')
        plt.close(fig)
    except Exception as e:
        print('Did not log ROC-AUC chart. Error {}'.format(e))


def log_precision_recall_chart(classifier, X_test, y_test, y_pred_proba=None, experiment=None):
    """Log precision recall chart.

    Make sure you created an experiment by using ``neptune.create_experiment()`` before you use this method.

    Tip:
        Check `Neptune documentation <https://docs.neptune.ai/integrations/scikit_learn.html>`_ for the full example.

    Args:
        classifier (:obj:`classifier`):
            | Fitted sklearn classifier object
        X_test (:obj:`ndarray`):
            | Testing data matrix
        y_test (:obj:`ndarray`):
            | The classification target for testing
        y_pred_proba (:obj:`ndarray`, optional, default is ``None``):
            | Classifier predictions probabilities on test data.
        experiment (:obj:`neptune.experiments.Experiment`, optional, default is ``None``):
            | Neptune ``Experiment`` object to control to which experiment you log the data.
            | If ``None``, log to currently active, and most recent experiment.

    Returns:
        ``None``

    Examples:
        .. code:: python3

            rfc = RandomForestClassifier()
            rfc.fit(X_train, y_train)

            neptune.init('my_workspace/my_project')
            neptune.create_experiment()

            log_precision_recall_chart(rfc, X_test, y_test)
    """
    assert is_classifier(classifier), 'classifier should be sklearn classifier.'
    exp = _validate_experiment(experiment)

    if y_pred_proba is None:
        try:
            y_pred_proba = classifier.predict_proba(X_test)
        except Exception as e:
            print('Did not log Precision-Recall chart: this classifier does not provide predictions probabilities.'
                  'Error {}'.format(e))
            return

    try:
        fig, ax = plt.subplots()
        plot_precision_recall(y_test, y_pred_proba, ax=ax)
        exp.log_image('charts_sklearn', fig, image_name='Precision Recall Curve')
        plt.close(fig)
    except Exception as e:
        print('Did not log Precision-Recall chart. Error {}'.format(e))


def log_class_prediction_error_chart(classifier, X_train, X_test, y_train, y_test, experiment=None):
    """Log class prediction error chart.

    Make sure you created an experiment by using ``neptune.create_experiment()`` before you use this method.

    Tip:
        Check `Neptune documentation <https://docs.neptune.ai/integrations/scikit_learn.html>`_ for the full example.

    Args:
        classifier (:obj:`classifier`):
            | Fitted sklearn classifier object
        X_train (:obj:`ndarray`):
            | Training data matrix
        X_test (:obj:`ndarray`):
            | Testing data matrix
        y_train (:obj:`ndarray`):
            | The classification target for training
        y_test (:obj:`ndarray`):
            | The classification target for testing
        experiment (:obj:`neptune.experiments.Experiment`, optional, default is ``None``):
            | Neptune ``Experiment`` object to control to which experiment you log the data.
            | If ``None``, log to currently active, and most recent experiment.

    Returns:
        ``None``

    Examples:
        .. code:: python3

            rfc = RandomForestClassifier()
            rfc.fit(X_train, y_train)

            neptune.init('my_workspace/my_project')
            exp = neptune.create_experiment()

            log_class_prediction_error_chart(rfc, X_train, X_test, y_train, y_test, experiment=exp)
    """
    assert is_classifier(classifier), 'classifier should be sklearn classifier.'
    exp = _validate_experiment(experiment)

    try:
        fig, ax = plt.subplots()
        visualizer = ClassPredictionError(classifier, is_fitted=True, ax=ax)
        visualizer.fit(X_train, y_train)
        visualizer.score(X_test, y_test)
        visualizer.finalize()
        exp.log_image('charts_sklearn', fig, image_name='Class Prediction Error')
        plt.close(fig)
    except Exception as e:
        print('Did not log Class Prediction Error chart. Error {}'.format(e))


def log_kmeans_clustering_summary(model, X,
                                  nrows=1000, experiment=None, **kwargs):
    """Log sklearn kmeans summary.

    This method fit KMeans model to data and logs cluster labels, all kmeans parameters
    and clustering visualizations: KMeans elbow chart and silhouette coefficients chart.

    Make sure you created an experiment by using ``neptune.create_experiment()`` before you use this method.

    Tip:
        Check `Neptune documentation <https://docs.neptune.ai/integrations/scikit_learn.html>`_ for the full example.

    Args:
        model (:obj:`KMeans`):
            | KMeans object.
        X (:obj:`ndarray`):
            | Training instances to cluster.
        nrows (`int`, optional, default is 1000):
            | Number of rows to log in the cluster labels
        experiment (:obj:`neptune.experiments.Experiment`, optional, default is ``None``):
            | Neptune ``Experiment`` object to control to which experiment you log the data.
            | If ``None``, log to currently active, and most recent experiment.
        kwargs:
            KMeans parameters.

    Returns:
        ``None``

    Examples:
        .. code:: python3

            km = KMeans(n_init=11, max_iter=270)
            X, y = make_blobs(n_samples=579, n_features=17, centers=7, random_state=28743)

            neptune.init('my_workspace/my_project')
            neptune.create_experiment()

            log_kmeans_clustering_summary(km, X=X)
    """
    assert isinstance(model, KMeans), 'model should be sklearn KMeans instance'

    exp = _validate_experiment(experiment)

    model.set_params(**kwargs)
    log_estimator_params(model, exp)
    log_cluster_labels(model, X, nrows=nrows, experiment=exp, **kwargs)

    # visualizations
    log_kelbow_chart(model, X, experiment=exp, **kwargs)
    log_silhouette_chart(model, X, experiment=exp, **kwargs)


def log_cluster_labels(model, X, nrows=1000, experiment=None, **kwargs):
    """Log index of the cluster label each sample belongs to.

    Make sure you created an experiment by using ``neptune.create_experiment()`` before you use this method.

    Tip:
        Check `Neptune documentation <https://docs.neptune.ai/integrations/scikit_learn.html>`_ for the full example.

    Args:
        model (:obj:`KMeans`):
            | KMeans object.
        X (:obj:`ndarray`):
            | Training instances to cluster.
        nrows (`int`, optional, default is 1000):
            | Number of rows to log.
        experiment (:obj:`neptune.experiments.Experiment`, optional, default is ``None``):
            | Neptune ``Experiment`` object to control to which experiment you log the data.
            | If ``None``, log to currently active, and most recent experiment.
        kwargs:
            KMeans parameters.

    Returns:
        ``None``

    Examples:
        .. code:: python3

            km = KMeans(n_init=11, max_iter=270)
            X, y = make_blobs(n_samples=579, n_features=17, centers=7, random_state=28743)

            neptune.init('my_workspace/my_project')
            neptune.create_experiment()

            log_cluster_labels(km, X=X)
    """
    assert isinstance(model, KMeans), 'Model should be sklearn KMeans instance.'
    assert isinstance(nrows, int), 'nrows should be integer, {} was passed'.format(type(nrows))
    exp = _validate_experiment(experiment)

    model.set_params(**kwargs)
    labels = model.fit_predict(X)
    df = pd.DataFrame(data={'cluster_labels': labels})
    log_csv('cluster_labels', df.head(nrows), exp)


def log_kelbow_chart(model, X, experiment=None, **kwargs):
    """Log K-elbow chart for KMeans clusterer.

    Make sure you created an experiment by using ``neptune.create_experiment()`` before you use this method.

    Tip:
        Check `Neptune documentation <https://docs.neptune.ai/integrations/scikit_learn.html>`_ for the full example.

    Args:
        model (:obj:`KMeans`):
            | KMeans object.
        X (:obj:`ndarray`):
            | Training instances to cluster.
        experiment (:obj:`neptune.experiments.Experiment`, optional, default is ``None``):
            | Neptune ``Experiment`` object to control to which experiment you log the data.
            | If ``None``, log to currently active, and most recent experiment.
        kwargs:
            KMeans parameters.

    Returns:
        ``None``

    Examples:
        .. code:: python3

            km = KMeans(n_init=11, max_iter=270)
            X, y = make_blobs(n_samples=579, n_features=17, centers=7, random_state=28743)

            neptune.init('my_workspace/my_project')
            neptune.create_experiment()

            log_kelbow_chart(km, X=X)
    """
    assert isinstance(model, KMeans), 'Model should be sklearn KMeans instance.'
    exp = _validate_experiment(experiment)

    model.set_params(**kwargs)

    if 'n_clusters' in kwargs:
        k = kwargs['n_clusters']
    else:
        k = 10

    try:
        fig, ax = plt.subplots()
        visualizer = KElbowVisualizer(model, k=k, ax=ax)
        visualizer.fit(X)
        visualizer.finalize()
        exp.log_image('charts_sklearn', fig, image_name='KMeans elbow chart')
        plt.close(fig)
    except Exception as e:
        print('Did not log KMeans elbow chart. Error {}'.format(e))


def log_silhouette_chart(model, X, experiment=None, **kwargs):
    """Log Silhouette Coefficients charts for KMeans clusterer.

    Charts are computed for j = 2, 3, ..., n_clusters.

    Make sure you created an experiment by using ``neptune.create_experiment()`` before you use this method.

    Tip:
        Check `Neptune documentation <https://docs.neptune.ai/integrations/scikit_learn.html>`_ for the full example.

    Args:
        model (:obj:`KMeans`):
            | KMeans object.
        X (:obj:`ndarray`):
            | Training instances to cluster.
        experiment (:obj:`neptune.experiments.Experiment`, optional, default is ``None``):
            | Neptune ``Experiment`` object to control to which experiment you log the data.
            | If ``None``, log to currently active, and most recent experiment.
        kwargs:
            KMeans parameters.

    Returns:
        ``None``

    Examples:
        .. code:: python3

            km = KMeans(n_init=11, max_iter=270)
            X, y = make_blobs(n_samples=579, n_features=17, centers=7, random_state=28743)

            neptune.init('my_workspace/my_project')
            neptune.create_experiment()

            log_silhouette_chart(km, X=X, n_clusters=12)
    """
    assert isinstance(model, KMeans), 'Model should be sklearn KMeans instance.'
    exp = _validate_experiment(experiment)

    model.set_params(**kwargs)

    n_clusters = model.get_params()['n_clusters']

    for j in range(2, n_clusters+1):
        model.set_params(**{'n_clusters': j})
        model.fit(X)

        try:
            fig, ax = plt.subplots()
            visualizer = SilhouetteVisualizer(model, is_fitted=True, ax=ax)
            visualizer.fit(X)
            visualizer.finalize()
            exp.log_image('charts_sklearn', fig, image_name='Silhouette Coefficients for k={}'.format(j))
            plt.close(fig)
        except Exception as e:
            print('Did not log Silhouette Coefficients chart. Error {}'.format(e))


def _validate_experiment(experiment):
    if not isinstance(experiment, Experiment):
        raise NotNeptuneExperimentException()
