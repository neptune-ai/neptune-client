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
import os
import pickle
import tempfile
from io import BytesIO

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

import neptune.alpha as neptune
from neptune.alpha.experiment import Experiment
from neptune.exceptions import NotNeptuneExperimentException


# ToDo
# Yellowbrick and scikitplot are required
# pip install yellowbrick>=1.3
# pip install scikit-plot>=0.3.7


def log_regressor_summary(experiment, regressor, X_train, X_test, y_train, y_test,
                          nrows=1000, log_charts=True):
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
        nrows (`int`, optional, default is 1000):
            | Log first ``nrows`` rows of test predictions.
        log_charts (:bool:, optional, default is ``True``):
            | If ``True``, calculate and log chart visualizations.
            |
            | NOTE: calculating visualizations is potentially expensive depending on input data and regressor, and
            | may take some time to finish.
            |
            | This is equivalent to calling ``log_learning_curve_chart``, ``log_feature_importance_chart``,
            | ``log_residuals_chart``, ``log_prediction_error_chart``, ``log_cooks_distance_chart``
            | functions from this module.

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
    log_pickled_model(experiment, regressor)

    y_pred = regressor.predict(X_test)
    log_test_predictions(experiment, regressor, X_test, y_test, y_pred=y_pred, nrows=nrows)
    log_scores(experiment, regressor, X_test, y_test, y_pred=y_pred)

    # visualizations
    if log_charts:
        log_learning_curve_chart(experiment, regressor, X_train, y_train)
        log_feature_importance_chart(experiment, regressor, X_train, y_train)
        log_residuals_chart(experiment, regressor, X_train, X_test, y_train, y_test)
        log_prediction_error_chart(experiment, regressor, X_train, X_test, y_train, y_test)
        log_cooks_distance_chart(experiment, regressor, X_train, y_train)


def log_classifier_summary(experiment, classifier, X_train, X_test, y_train, y_test,
                           nrows=1000, log_charts=True):
    """Log sklearn classifier summary.

    This method automatically logs all classifier parameters, pickled estimator (model),
    test predictions, predictions probabilities as table, model performance visualizations and test metrics.

    Classifier should be fitted before calling this function.

    Tip:
        Check Sklearn-Neptune integration
        `documentation <https://docs-beta.neptune.ai/essentials/integrations/machine-learning-frameworks/sklearn>`_
        for the full example.

    Args:
        experiment (:obj:`neptune.experiment.Experiment`):
            | Neptune ``Experiment`` to control to which experiment you log the data.
            | Create one by ``exp = neptune.init(project='my_workspace/my_project')``.
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
        nrows (`int`, optional, default is 1000):
            | Log first ``nrows`` rows of test predictions and predictions probabilities.
        log_charts (:bool:, optional, default is ``True``):
            | If True, calculate and send chart visualizations.
            |
            | NOTE: calculating visualizations is potentially expensive depending on input data and classifier, and
            | may take some time to finish.
            |
            | This is equivalent to calling ``log_classification_report_chart``, ``log_confusion_matrix_chart``,
            | ``log_roc_auc_chart``, ``log_precision_recall_chart``, ``log_class_prediction_error_chart``
            | functions from this module.

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

    _validate_experiment(experiment)

    log_estimator_params(experiment, classifier)
    log_pickled_model(experiment, classifier,)
    log_test_preds_proba(experiment, classifier, X_test, nrows=nrows)

    y_pred = classifier.predict(X_test)
    log_test_predictions(experiment, classifier, X_test, y_test, y_pred=y_pred, nrows=nrows)
    log_scores(experiment, classifier, X_test, y_test, y_pred=y_pred)

    # visualizations
    if log_charts:
        log_classification_report_chart(experiment, classifier, X_train, X_test, y_train, y_test)
        log_confusion_matrix_chart(experiment, classifier, X_train, X_test, y_train, y_test)
        log_roc_auc_chart(experiment, classifier, X_train, X_test, y_train, y_test)
        log_precision_recall_chart(experiment, classifier, X_test, y_test)
        log_class_prediction_error_chart(experiment, classifier, X_train, X_test, y_train, y_test)


def log_estimator_params(experiment, estimator, namespace=None):
    """Log estimator parameters.

    Log all estimator parameters under given <namespace>, by default ``all_params/``.

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
        namespace (:str:, optional, default is ``None``):
            | Neptune 'namespace' under which to log parameters.
            | If ``None``, then ``all_params/`` is used.

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
    assert isinstance(namespace, str) or namespace is None,\
        'namespace must be str, but {} was passed'.format(type(namespace))

    _validate_experiment(experiment)

    if namespace is None:
        namespace = 'all_params'

    experiment[namespace] = estimator.get_params()


def log_pickled_model(experiment, estimator, namespace=None):
    """Log pickled estimator.

    Log estimator as pickled file under given <namespace>, by default ``model/estimator.skl``.

    Estimator should be fitted before calling this function.

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
        namespace (:str:, optional, default is ``None``):
            | Neptune 'namespace' under which to store pickled model.
            | If ``None``, then ``model/estimator.skl`` is used.

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
    assert isinstance(namespace, str) or namespace is None,\
        'namespace must be str, but {} was passed'.format(type(namespace))

    _validate_experiment(experiment)

    if namespace is None:
        namespace = 'model/estimator.skl'

    buffer = BytesIO()
    pickle.dump(estimator, buffer)
    buffer.seek(0)

    experiment[namespace] = buffer
    experiment.wait()
    buffer.close()


def log_test_predictions(experiment, estimator, X_test, y_test, y_pred=None, nrows=1000, namespace=None):
    """Log test predictions.

    Calculate and log test predictions, and have them as csv file under given <namespace>,
    by default ``test/y_preds.csv``.

    If you pass ``y_pred``, then predictions are logged without computing from ``X_test`` data.

    Estimator should be fitted before calling this function.

    Tip:
        Check Sklearn-Neptune integration
        `documentation <https://docs-beta.neptune.ai/essentials/integrations/machine-learning-frameworks/sklearn>`_
        for the full example.

    Args:
        experiment (:obj:`neptune.experiment.Experiment`):
            | Neptune ``Experiment`` to control to which experiment you log the data.
            | Create one by ``exp = neptune.init(project='my_workspace/my_project')``.
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
        namespace (:str:, optional, default is ``None``):
            | Neptune 'namespace' under which to store test preds.
            | If ``None``, then ``test/y_preds.csv`` is used.

    Returns:
        ``None``

    Examples:
        .. code:: python3

            rfr = RandomForestRegressor()
            rfr.fit(X_train, y_train)

            exp = neptune.init(project='my_workspace/my_project')

            log_test_predictions(exp, rfr, X_test, y_test)
    """
    assert is_regressor(estimator) or is_classifier(estimator),\
        'Estimator should be sklearn regressor or classifier.'
    assert isinstance(nrows, int), 'nrows should be integer, {} was passed'.format(type(nrows))
    assert isinstance(namespace, str) or namespace is None,\
        'namespace must be str, but {} was passed'.format(type(namespace))

    _validate_experiment(experiment)

    if namespace is None:
        namespace = 'test/y_preds.csv'

    if y_pred is None:
        y_pred = estimator.predict(X_test)

    with tempfile.TemporaryDirectory(dir='.') as d:
        path = os.path.join(d, 'y_preds.csv')

        # single output
        if len(y_pred.shape) == 1:
            df = pd.DataFrame(data={'y_true': y_test, 'y_pred': y_pred})
            df = df.head(n=nrows)
            df.to_csv(path)
            experiment[namespace].save(path)
        # multi output
        if len(y_pred.shape) == 2:
            df = pd.DataFrame()
            for j in range(y_pred.shape[1]):
                df['y_test_output_{}'.format(j)] = y_test[:, j]
                df['y_pred_output_{}'.format(j)] = y_pred[:, j]
            df = df.head(n=nrows)
            df.to_csv(path)
            experiment[namespace].save(path)
        experiment.wait()


def log_test_preds_proba(experiment, classifier, X_test, y_pred_proba=None, nrows=1000, namespace=None):
    """Log test predictions probabilities.

    Calculate and log test preds probabilities, and have them as csv file under given <namespace>,
    by default ``test/y_preds_proba.csv``.

    If you pass ``y_pred_proba``, then predictions probabilities are logged without computing from ``X_test`` data.

    Estimator should be fitted before calling this function.

    Tip:
        Check Sklearn-Neptune integration
        `documentation <https://docs-beta.neptune.ai/essentials/integrations/machine-learning-frameworks/sklearn>`_
        for the full example.

    Args:
        experiment (:obj:`neptune.experiment.Experiment`):
            | Neptune ``Experiment`` to control to which experiment you log the data.
            | Create one by ``exp = neptune.init(project='my_workspace/my_project')``.
        classifier (:obj:`classifier`):
            | Scikit-learn classifier to compute predictions probabilities.
        X_test (:obj:`ndarray`):
            | Testing data matrix.
        y_pred_proba (:obj:`ndarray`, optional, default is ``None``):
            | Classifier predictions probabilities on test data.
        nrows (`int`, optional, default is 1000):
            | Number of rows to log.
        namespace (:str:, optional, default is ``None``):
            | Neptune 'namespace' under which to store test preds probabilities.
            | If ``None``, then ``test/y_preds_proba.csv`` is used.

    Returns:
        ``None``

    Examples:
        .. code:: python3

            rfc = RandomForestClassifier()
            rfc.fit(X_train, y_train)

            exp = neptune.init(project='my_workspace/my_project')

            log_test_preds_proba(exp, rfc, X_test, y_test)
    """
    assert is_classifier(classifier), 'Classifier should be sklearn classifier.'
    assert isinstance(nrows, int), 'nrows should be integer, {} was passed'.format(type(nrows))
    assert isinstance(namespace, str) or namespace is None,\
        'namespace must be str, but {} was passed'.format(type(namespace))

    _validate_experiment(experiment)

    if namespace is None:
        namespace = 'test/y_preds_proba.csv'

    if y_pred_proba is None:
        try:
            y_pred_proba = classifier.predict_proba(X_test)
        except Exception as e:
            print('This classifier does not provide predictions probabilities. Error: {}'.format(e))
            return

    with tempfile.TemporaryDirectory(dir='.') as d:
        path = os.path.join(d, 'y_preds_proba.csv')
        df = pd.DataFrame(data=y_pred_proba, columns=classifier.classes_)
        df = df.head(n=nrows)
        df.to_csv(path)

        experiment[namespace].save(path)
        experiment.wait()


def log_scores(experiment, estimator, X, y, y_pred=None, namespace=None):
    """Log estimator scores on ``X``.

    Calculate and log scores on data and have them under given <namespace>,
    by default ``test/scores/``.

    If you pass ``y_pred``, then predictions are not computed from ``X`` data.

    Estimator should be fitted before calling this function.

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
        Check Sklearn-Neptune integration
        `documentation <https://docs-beta.neptune.ai/essentials/integrations/machine-learning-frameworks/sklearn>`_
        for the full example.

    Args:
        experiment (:obj:`neptune.experiment.Experiment`):
            | Neptune ``Experiment`` to control to which experiment you log the data.
            | Create one by ``exp = neptune.init(project='my_workspace/my_project')``.
        estimator (:obj:`estimator`):
            | Scikit-learn estimator to compute scores.
        X (:obj:`ndarray`):
            | Data matrix.
        y (:obj:`ndarray`):
            | Target for testing.
        y_pred (:obj:`ndarray`, optional, default is ``None``):
            | Estimator predictions on data.
        namespace (:str:, optional, default is ``None``):
            | Neptune 'namespace' under which to store scores.
            | If ``None``, then ``test/scores/`` is used.

    Returns:
        ``None``

    Examples:
        .. code:: python3

            rfc = RandomForestClassifier()
            rfc.fit(X_train, y_train)

            exp = neptune.init(project='my_workspace/my_project')

            log_scores(exp, rfc, X, y)
    """
    assert is_regressor(estimator) or is_classifier(estimator),\
        'Estimator should be sklearn regressor or classifier.'
    assert isinstance(namespace, str) or namespace is None,\
        'namespace must be str, but {} was passed'.format(type(namespace))

    _validate_experiment(experiment)

    if namespace is None:
        namespace = 'test/scores/'

    if y_pred is None:
        y_pred = estimator.predict(X)

    if is_regressor(estimator):
        # single output
        if len(y_pred.shape) == 1:
            evs = explained_variance_score(y, y_pred)
            me = max_error(y, y_pred)
            mae = mean_absolute_error(y, y_pred)
            r2 = r2_score(y, y_pred)

            experiment['{}/explained_variance_score'.format(namespace)] = evs
            experiment['{}/max_error'.format(namespace)] = me
            experiment['{}/mean_absolute_error'.format(namespace)] = mae
            experiment['{}/r2_score'.format(namespace)] = r2
        # multi output
        if len(y_pred.shape) == 2:
            r2 = estimator.score(X, y)
            experiment['{}/r2_score'.format(namespace)] = r2
    elif is_classifier(estimator):
        for metric_name, values in zip(['precision', 'recall', 'fbeta_score', 'support'],
                                       precision_recall_fscore_support(y, y_pred)):
            for i, value in enumerate(values):
                experiment['{}/{}/class_{}'.format(namespace, metric_name, i)] = value


def log_learning_curve_chart(experiment, regressor, X_train, y_train, namespace=None):
    """Log learning curve chart.

    Create and log learning curve chart, and have it under given <namespace>,
    by default ``diagnostics_charts/learning_curve``.

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
        y_train (:obj:`ndarray`):
            | The regression target for training
        namespace (:str:, optional, default is ``None``):
            | Neptune 'namespace' under which to store test preds probabilities.
            | If ``None``, then ``test/y_preds_proba.csv`` is used.

    Returns:
        ``None``

    Examples:
        .. code:: python3

            rfr = RandomForestRegressor()
            rfr.fit(X_train, y_train)

            exp = neptune.init(project='my_workspace/my_project')

            log_learning_curve_chart(exp, rfr, X_train, y_train)
    """
    assert is_regressor(regressor), 'regressor should be sklearn regressor.'
    assert isinstance(namespace, str) or namespace is None,\
        'namespace must be str, but {} was passed'.format(type(namespace))

    if namespace is None:
        namespace = 'diagnostics_charts/learning_curve'

    _validate_experiment(experiment)

    try:
        fig, ax = plt.subplots()
        plot_learning_curve(regressor, X_train, y_train, ax=ax)
        experiment[namespace].log(neptune.types.Image(fig))
        plt.close(fig)
    except Exception as e:
        print('Did not log learning curve chart. Error: {}'.format(e))


def log_feature_importance_chart(experiment, regressor, X_train, y_train, namespace=None):
    """Log feature importance chart.

    Create and log learning curve chart, and have it under given <namespace>,
    by default ``diagnostics_charts/feature_importance``.

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
        y_train (:obj:`ndarray`):
            | The regression target for training
        namespace (:str:, optional, default is ``None``):
            | Neptune 'namespace' under which to store test preds probabilities.
            | If ``None``, then ``test/y_preds_proba.csv`` is used.

    Returns:
        ``None``

    Examples:
        .. code:: python3

            rfr = RandomForestRegressor()
            rfr.fit(X_train, y_train)

            exp = neptune.init(project='my_workspace/my_project')

            log_feature_importance_chart(exp, rfr, X_train, y_train)
    """
    assert is_regressor(regressor), 'regressor should be sklearn regressor.'
    assert isinstance(namespace, str) or namespace is None,\
        'namespace must be str, but {} was passed'.format(type(namespace))

    if namespace is None:
        namespace = 'diagnostics_charts/feature_importance'

    _validate_experiment(experiment)

    try:
        fig, ax = plt.subplots()
        visualizer = FeatureImportances(regressor, is_fitted=True, ax=ax)
        visualizer.fit(X_train, y_train)
        visualizer.finalize()
        experiment[namespace].log(neptune.types.Image(fig))
        plt.close(fig)
    except Exception as e:
        print('Did not log feature importance chart. Error: {}'.format(e))


def log_residuals_chart(experiment, regressor, X_train, X_test, y_train, y_test, namespace=None):
    """Log residuals chart.

    Create and log learning curve chart, and have it under given <namespace>,
    by default ``diagnostics_charts/residuals``.

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
        namespace (:str:, optional, default is ``None``):
            | Neptune 'namespace' under which to store test preds probabilities.
            | If ``None``, then ``test/y_preds_proba.csv`` is used.

    Returns:
        ``None``

    Examples:
        .. code:: python3

            rfr = RandomForestRegressor()
            rfr.fit(X_train, y_train)

            exp = neptune.init(project='my_workspace/my_project')

            log_residuals_chart(exp, rfr, X_train, X_test, y_train, y_test)
    """
    assert is_regressor(regressor), 'regressor should be sklearn regressor.'
    assert isinstance(namespace, str) or namespace is None,\
        'namespace must be str, but {} was passed'.format(type(namespace))

    if namespace is None:
        namespace = 'diagnostics_charts/residuals'

    _validate_experiment(experiment)

    try:
        fig, ax = plt.subplots()
        visualizer = ResidualsPlot(regressor, is_fitted=True, ax=ax)
        visualizer.fit(X_train, y_train)
        visualizer.score(X_test, y_test)
        visualizer.finalize()
        experiment[namespace].log(neptune.types.Image(fig))
        plt.close(fig)
    except Exception as e:
        print('Did not log residuals chart. Error: {}'.format(e))


def log_prediction_error_chart(experiment, regressor, X_train, X_test, y_train, y_test, namespace=None):
    """Log prediction error chart.

    Create and log learning curve chart, and have it under given <namespace>,
    by default ``diagnostics_charts/prediction_error``.

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
        namespace (:str:, optional, default is ``None``):
            | Neptune 'namespace' under which to store test preds probabilities.
            | If ``None``, then ``test/y_preds_proba.csv`` is used.

    Returns:
        ``None``

    Examples:
        .. code:: python3

            rfr = RandomForestRegressor()
            rfr.fit(X_train, y_train)

            exp = neptune.init(project='my_workspace/my_project')

            log_prediction_error_chart(exp, rfr, X_train, X_test, y_train, y_test)
    """
    assert is_regressor(regressor), 'regressor should be sklearn regressor.'
    assert isinstance(namespace, str) or namespace is None,\
        'namespace must be str, but {} was passed'.format(type(namespace))

    if namespace is None:
        namespace = 'diagnostics_charts/prediction_error'

    _validate_experiment(experiment)

    try:
        fig, ax = plt.subplots()
        visualizer = PredictionError(regressor, is_fitted=True, ax=ax)
        visualizer.fit(X_train, y_train)
        visualizer.score(X_test, y_test)
        visualizer.finalize()
        experiment[namespace].log(neptune.types.Image(fig))
        plt.close(fig)
    except Exception as e:
        print('Did not log prediction error chart. Error: {}'.format(e))


def log_cooks_distance_chart(experiment, regressor, X_train, y_train, namespace=None):
    """Log feature importance chart.

    Create and log learning curve chart, and have it under given <namespace>,
    by default ``diagnostics_charts/cooks_distance``.

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
        y_train (:obj:`ndarray`):
            | The regression target for training
        namespace (:str:, optional, default is ``None``):
            | Neptune 'namespace' under which to store test preds probabilities.
            | If ``None``, then ``test/y_preds_proba.csv`` is used.

    Returns:
        ``None``

    Examples:
        .. code:: python3

            rfr = RandomForestRegressor()
            rfr.fit(X_train, y_train)

            exp = neptune.init(project='my_workspace/my_project')

            log_cooks_distance_chart(exp, rfr, X_train, y_train)
    """
    assert is_regressor(regressor), 'regressor should be sklearn regressor.'
    assert isinstance(namespace, str) or namespace is None,\
        'namespace must be str, but {} was passed'.format(type(namespace))

    if namespace is None:
        namespace = 'diagnostics_charts/cooks_distance'

    _validate_experiment(experiment)

    try:
        fig, ax = plt.subplots()
        visualizer = CooksDistance(ax=ax)
        visualizer.fit(X_train, y_train)
        visualizer.finalize()
        experiment[namespace].log(neptune.types.Image(fig))
        plt.close(fig)
    except Exception as e:
        print('Did not log cooks distance chart. Error: {}'.format(e))


def log_classification_report_chart(experiment, classifier, X_train, X_test, y_train, y_test, namespace=None):
    """Log classification report chart.

    Create and log learning curve chart, and have it under given <namespace>,
    by default ``diagnostics_charts/classification_report``.

    Tip:
        Check Sklearn-Neptune integration
        `documentation <https://docs-beta.neptune.ai/essentials/integrations/machine-learning-frameworks/sklearn>`_
        for the full example.

    Args:
        experiment (:obj:`neptune.experiment.Experiment`):
            | Neptune ``Experiment`` to control to which experiment you log the data.
            | Create one by ``exp = neptune.init(project='my_workspace/my_project')``.
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
        namespace (:str:, optional, default is ``None``):
            | Neptune 'namespace' under which to store test preds probabilities.
            | If ``None``, then ``test/y_preds_proba.csv`` is used.

    Returns:
        ``None``

    Examples:
        .. code:: python3

            rfc = RandomForestClassifier()
            rfc.fit(X_train, y_train)

            exp = neptune.init(project='my_workspace/my_project')

            log_classification_report_chart(exp, rfc, X_train, X_test, y_train, y_test)
    """
    assert is_classifier(classifier), 'classifier should be sklearn classifier.'
    assert isinstance(namespace, str) or namespace is None,\
        'namespace must be str, but {} was passed'.format(type(namespace))

    if namespace is None:
        namespace = 'diagnostics_charts/classification_report'

    _validate_experiment(experiment)

    try:
        fig, ax = plt.subplots()
        visualizer = ClassificationReport(classifier, support=True, is_fitted=True, ax=ax)
        visualizer.fit(X_train, y_train)
        visualizer.score(X_test, y_test)
        visualizer.finalize()
        experiment[namespace].log(neptune.types.Image(fig))
        plt.close(fig)
    except Exception as e:
        print('Did not log Classification Report chart. Error: {}'.format(e))


def log_confusion_matrix_chart(experiment, classifier, X_train, X_test, y_train, y_test, namespace=None):
    """Log confusion matrix.

    Create and log learning curve chart, and have it under given <namespace>,
    by default ``diagnostics_charts/confusion_matrix``.

    Tip:
        Check Sklearn-Neptune integration
        `documentation <https://docs-beta.neptune.ai/essentials/integrations/machine-learning-frameworks/sklearn>`_
        for the full example.

    Args:
        experiment (:obj:`neptune.experiment.Experiment`):
            | Neptune ``Experiment`` to control to which experiment you log the data.
            | Create one by ``exp = neptune.init(project='my_workspace/my_project')``.
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
        namespace (:str:, optional, default is ``None``):
            | Neptune 'namespace' under which to store test preds probabilities.
            | If ``None``, then ``test/y_preds_proba.csv`` is used.

    Returns:
        ``None``

    Examples:
        .. code:: python3

            rfc = RandomForestClassifier()
            rfc.fit(X_train, y_train)

            exp = neptune.init(project='my_workspace/my_project')

            log_confusion_matrix_chart(exp, rfc, X_train, X_test, y_train, y_test)
    """
    assert is_classifier(classifier), 'classifier should be sklearn classifier.'
    assert isinstance(namespace, str) or namespace is None,\
        'namespace must be str, but {} was passed'.format(type(namespace))

    if namespace is None:
        namespace = 'diagnostics_charts/confusion_matrix'

    _validate_experiment(experiment)

    try:
        fig, ax = plt.subplots()
        visualizer = ConfusionMatrix(classifier, is_fitted=True, ax=ax)
        visualizer.fit(X_train, y_train)
        visualizer.score(X_test, y_test)
        visualizer.finalize()
        experiment[namespace].log(neptune.types.Image(fig))
        plt.close(fig)
    except Exception as e:
        print('Did not log Confusion Matrix chart. Error: {}'.format(e))


def log_roc_auc_chart(experiment, classifier, X_train, X_test, y_train, y_test, namespace=None):
    """Log ROC-AUC chart.

    Create and log learning curve chart, and have it under given <namespace>,
    by default ``diagnostics_charts/ROC_AUC``.

    Tip:
        Check Sklearn-Neptune integration
        `documentation <https://docs-beta.neptune.ai/essentials/integrations/machine-learning-frameworks/sklearn>`_
        for the full example.

    Args:
        experiment (:obj:`neptune.experiment.Experiment`):
            | Neptune ``Experiment`` to control to which experiment you log the data.
            | Create one by ``exp = neptune.init(project='my_workspace/my_project')``.
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
        namespace (:str:, optional, default is ``None``):
            | Neptune 'namespace' under which to store test preds probabilities.
            | If ``None``, then ``test/y_preds_proba.csv`` is used.

    Returns:
        ``None``

    Examples:
        .. code:: python3

            rfc = RandomForestClassifier()
            rfc.fit(X_train, y_train)

            exp = neptune.init(project='my_workspace/my_project')

            log_roc_auc_chart(exp, rfc, X_train, X_test, y_train, y_test)
    """
    assert is_classifier(classifier), 'classifier should be sklearn classifier.'
    assert isinstance(namespace, str) or namespace is None,\
        'namespace must be str, but {} was passed'.format(type(namespace))

    if namespace is None:
        namespace = 'diagnostics_charts/ROC_AUC'

    _validate_experiment(experiment)

    try:
        fig, ax = plt.subplots()
        visualizer = ROCAUC(classifier, is_fitted=True, ax=ax)
        visualizer.fit(X_train, y_train)
        visualizer.score(X_test, y_test)
        visualizer.finalize()
        experiment[namespace].log(neptune.types.Image(fig))
        plt.close(fig)
    except Exception as e:
        print('Did not log ROC-AUC chart. Error {}'.format(e))


def log_precision_recall_chart(experiment, classifier, X_test, y_test, y_pred_proba=None, namespace=None):
    """Log precision recall chart.

    Create and log learning curve chart, and have it under given <namespace>,
    by default ``diagnostics_charts/precision_recall``.

    Tip:
        Check Sklearn-Neptune integration
        `documentation <https://docs-beta.neptune.ai/essentials/integrations/machine-learning-frameworks/sklearn>`_
        for the full example.

    Args:
        experiment (:obj:`neptune.experiment.Experiment`):
            | Neptune ``Experiment`` to control to which experiment you log the data.
            | Create one by ``exp = neptune.init(project='my_workspace/my_project')``.
        classifier (:obj:`classifier`):
            | Fitted sklearn classifier object
        X_test (:obj:`ndarray`):
            | Testing data matrix
        y_test (:obj:`ndarray`):
            | The classification target for testing
        y_pred_proba (:obj:`ndarray`, optional, default is ``None``):
            | Classifier predictions probabilities on test data.
        namespace (:str:, optional, default is ``None``):
            | Neptune 'namespace' under which to store test preds probabilities.
            | If ``None``, then ``test/y_preds_proba.csv`` is used.

    Returns:
        ``None``

    Examples:
        .. code:: python3

            rfc = RandomForestClassifier()
            rfc.fit(X_train, y_train)

            exp = neptune.init(project='my_workspace/my_project')

            log_precision_recall_chart(exp, rfc, X_test, y_test)
    """
    assert is_classifier(classifier), 'classifier should be sklearn classifier.'
    assert isinstance(namespace, str) or namespace is None,\
        'namespace must be str, but {} was passed'.format(type(namespace))

    if namespace is None:
        namespace = 'diagnostics_charts/precision_recall'

    _validate_experiment(experiment)

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
        experiment[namespace].log(neptune.types.Image(fig))
        plt.close(fig)
    except Exception as e:
        print('Did not log Precision-Recall chart. Error {}'.format(e))


def log_class_prediction_error_chart(experiment, classifier, X_train, X_test, y_train, y_test, namespace=None):
    """Log class prediction error chart.

    Create and log learning curve chart, and have it under given <namespace>,
    by default ``diagnostics_charts/class_prediction_error``.

    Tip:
        Check Sklearn-Neptune integration
        `documentation <https://docs-beta.neptune.ai/essentials/integrations/machine-learning-frameworks/sklearn>`_
        for the full example.

    Args:
        experiment (:obj:`neptune.experiment.Experiment`):
            | Neptune ``Experiment`` to control to which experiment you log the data.
            | Create one by ``exp = neptune.init(project='my_workspace/my_project')``.
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
        namespace (:str:, optional, default is ``None``):
            | Neptune 'namespace' under which to store test preds probabilities.
            | If ``None``, then ``test/y_preds_proba.csv`` is used.

    Returns:
        ``None``

    Examples:
        .. code:: python3

            rfc = RandomForestClassifier()
            rfc.fit(X_train, y_train)

            exp = neptune.init(project='my_workspace/my_project')

            log_class_prediction_error_chart(exp, rfc, X_train, X_test, y_train, y_test)
    """
    assert is_classifier(classifier), 'classifier should be sklearn classifier.'
    assert isinstance(namespace, str) or namespace is None,\
        'namespace must be str, but {} was passed'.format(type(namespace))

    if namespace is None:
        namespace = 'diagnostics_charts/class_prediction_error'

    _validate_experiment(experiment)

    try:
        fig, ax = plt.subplots()
        visualizer = ClassPredictionError(classifier, is_fitted=True, ax=ax)
        visualizer.fit(X_train, y_train)
        visualizer.score(X_test, y_test)
        visualizer.finalize()
        experiment[namespace].log(neptune.types.Image(fig))
        plt.close(fig)
    except Exception as e:
        print('Did not log Class Prediction Error chart. Error {}'.format(e))


def fit_and_log_kmeans_summary(experiment, model, X, nrows=1000, **kwargs):
    """Log sklearn kmeans summary.

    This method fit KMeans model to data and logs cluster labels, all kmeans parameters
    and clustering visualizations: KMeans elbow chart and silhouette coefficients chart.

    Tip:
        Check Sklearn-Neptune integration
        `documentation <https://docs-beta.neptune.ai/essentials/integrations/machine-learning-frameworks/sklearn>`_
        for the full example.

    Args:
        experiment (:obj:`neptune.experiment.Experiment`):
            | Neptune ``Experiment`` to control to which experiment you log the data.
            | Create one by ``exp = neptune.init(project='my_workspace/my_project')``.
        model (:obj:`KMeans`):
            | KMeans object.
        X (:obj:`ndarray`):
            | Training instances to cluster.
        nrows (`int`, optional, default is 1000):
            | Number of rows to log in the cluster labels
        kwargs:
            KMeans parameters.

    Returns:
        ``None``

    Examples:
        .. code:: python3

            km = KMeans(n_init=11, max_iter=270)
            X, y = make_blobs(n_samples=579, n_features=17, centers=7, random_state=28743)

            exp = neptune.init(project='my_workspace/my_project')

            log_kmeans_clustering_summary(exp, km, X=X)
    """
    assert isinstance(model, KMeans), 'model should be sklearn KMeans instance'

    _validate_experiment(experiment)

    model.set_params(**kwargs)
    log_estimator_params(experiment, model)
    log_cluster_labels(experiment, model, X, nrows=nrows, **kwargs)

    # visualizations
    log_kelbow_chart(experiment, model, X, **kwargs)
    log_silhouette_chart(experiment, model, X, **kwargs)


def log_cluster_labels(experiment, model, X, nrows=1000, namespace=None, **kwargs):
    """Log index of the cluster label each sample belongs to.

    Calculate and log index of the cluster label each sample belongs to and have them as csv file
    under given <namespace>, by default ``cluster_labels``.

    Tip:
        Check Sklearn-Neptune integration
        `documentation <https://docs-beta.neptune.ai/essentials/integrations/machine-learning-frameworks/sklearn>`_
        for the full example.

    Args:
        experiment (:obj:`neptune.experiment.Experiment`):
            | Neptune ``Experiment`` to control to which experiment you log the data.
            | Create one by ``exp = neptune.init(project='my_workspace/my_project')``.
        model (:obj:`KMeans`):
            | KMeans object.
        X (:obj:`ndarray`):
            | Training instances to cluster.
        nrows (`int`, optional, default is 1000):
            | Number of rows to log.
        namespace (:str:, optional, default is ``None``):
            | Neptune 'namespace' under which to store test preds probabilities.
            | If ``None``, then ``cluster_labels`` is used.
        kwargs:
            KMeans parameters.

    Returns:
        ``None``

    Examples:
        .. code:: python3

            km = KMeans(n_init=11, max_iter=270)
            X, y = make_blobs(n_samples=579, n_features=17, centers=7, random_state=28743)

            exp = neptune.init(project='my_workspace/my_project')

            log_cluster_labels(exp, km, X=X)
    """
    assert isinstance(model, KMeans), 'Model should be sklearn KMeans instance.'
    assert isinstance(nrows, int), 'nrows should be integer, {} was passed'.format(type(nrows))
    assert isinstance(namespace, str) or namespace is None,\
        'namespace must be str, but {} was passed'.format(type(namespace))

    _validate_experiment(experiment)

    if namespace is None:
        namespace = 'cluster_labels'

    model.set_params(**kwargs)
    labels = model.fit_predict(X)

    with tempfile.TemporaryDirectory(dir='.') as d:
        path = os.path.join(d, 'cluster_labels.csv')
        df = pd.DataFrame(data={'cluster_labels': labels})
        df = df.head(n=nrows)
        df.to_csv(path)
        experiment[namespace].save(path)
        experiment.wait()


def log_kelbow_chart(experiment, model, X, namespace=None, **kwargs):
    """Log K-elbow chart for KMeans clusterer.

    Create and log K-elbow chart, and have it under given <namespace>,
    by default ``diagnostics_charts/kelbow``.

    Tip:
        Check Sklearn-Neptune integration
        `documentation <https://docs-beta.neptune.ai/essentials/integrations/machine-learning-frameworks/sklearn>`_
        for the full example.

    Args:
        experiment (:obj:`neptune.experiment.Experiment`):
            | Neptune ``Experiment`` to control to which experiment you log the data.
            | Create one by ``exp = neptune.init(project='my_workspace/my_project')``.
        model (:obj:`KMeans`):
            | KMeans object.
        X (:obj:`ndarray`):
            | Training instances to cluster.
        namespace (:str:, optional, default is ``None``):
            | Neptune 'namespace' under which to store test preds probabilities.
            | If ``None``, then ``diagnostics_charts/kelbow`` is used.
        kwargs:
            KMeans parameters.

    Returns:
        ``None``

    Examples:
        .. code:: python3

            km = KMeans(n_init=11, max_iter=270)
            X, y = make_blobs(n_samples=579, n_features=17, centers=7, random_state=28743)

            exp = neptune.init(project='my_workspace/my_project')

            log_kelbow_chart(exp, km, X=X)
    """
    assert isinstance(model, KMeans), 'Model should be sklearn KMeans instance.'
    assert isinstance(namespace, str) or namespace is None,\
        'namespace must be str, but {} was passed'.format(type(namespace))

    _validate_experiment(experiment)

    if namespace is None:
        namespace = 'diagnostics_charts/kelbow'

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
        experiment[namespace].log(neptune.types.Image(fig))
        plt.close(fig)
    except Exception as e:
        print('Did not log KMeans elbow chart. Error {}'.format(e))


def log_silhouette_chart(experiment, model, X, namespace=None, **kwargs):
    """Log Silhouette Coefficients charts for KMeans clusterer.

    Charts are computed for j = 2, 3, ..., n_clusters.

    Create and log silhouette charts, and have it under given <namespace>,
    by default ``diagnostics_charts/silhouette/``.

    Tip:
        Check Sklearn-Neptune integration
        `documentation <https://docs-beta.neptune.ai/essentials/integrations/machine-learning-frameworks/sklearn>`_
        for the full example.

    Args:
        experiment (:obj:`neptune.experiment.Experiment`):
            | Neptune ``Experiment`` to control to which experiment you log the data.
            | Create one by ``exp = neptune.init(project='my_workspace/my_project')``.
        model (:obj:`KMeans`):
            | KMeans object.
        X (:obj:`ndarray`):
            | Training instances to cluster.
        namespace (:str:, optional, default is ``None``):
            | Neptune 'namespace' under which to store test preds probabilities.
            | If ``None``, then ``diagnostics_charts/silhouette/`` is used.
        kwargs:
            KMeans parameters.

    Returns:
        ``None``

    Examples:
        .. code:: python3

            km = KMeans(n_init=11, max_iter=270)
            X, y = make_blobs(n_samples=579, n_features=17, centers=7, random_state=28743)

            exp = neptune.init(project='my_workspace/my_project')

            log_silhouette_chart(exp, km, X=X, n_clusters=12)
    """
    assert isinstance(model, KMeans), 'Model should be sklearn KMeans instance.'
    assert isinstance(namespace, str) or namespace is None,\
        'namespace must be str, but {} was passed'.format(type(namespace))

    _validate_experiment(experiment)

    if namespace is None:
        namespace = 'diagnostics_charts/silhouette/'

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
            experiment['{}/k={}'.format(namespace, j)].log(neptune.types.Image(fig))
            plt.close(fig)
        except Exception as e:
            print('Did not log Silhouette Coefficients chart. Error {}'.format(e))


def _validate_experiment(experiment):
    if not isinstance(experiment, Experiment):
        raise NotNeptuneExperimentException()
