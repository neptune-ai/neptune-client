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

import neptune.new as neptune

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions


def create_regressor_summary(regressor, X_train, X_test, y_train, y_test, nrows=1000, log_charts=True):
    """Create sklearn regressor summary.

    This method creates regressor summary that includes:

    * all regressor parameters,
    * pickled estimator (model),
    * test predictions,
    * test scores,
    * model performance visualizations.

    Returned ``dict`` can be assigned to the experiment's namespace defined by the user (see example below).

    Regressor should be fitted before calling this function.

    Tip:
        Check Sklearn-Neptune integration
        `documentation <https://docs-beta.neptune.ai/essentials/integrations/machine-learning-frameworks/sklearn>`_
        for the full example.

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
        ``dict`` with all summary items.

    Examples:
        Log random forest regressor summary.

        .. code:: python3

            import neptune.new.integrations.sklearn as npt_utils

            rfr = RandomForestRegressor()
            rfr.fit(X_train, y_train)

            exp = neptune.init(project='my_workspace/my_project')
            exp['random_forest/summary'] = npt_utils.create_regressor_summary(rfr, X_train, X_test, y_train, y_test)
    """
    assert is_regressor(regressor), 'regressor should be sklearn regressor.'

    reg_summary = dict()

    reg_summary['all_params'] = get_estimator_params(regressor)
    reg_summary['pickled_model'] = get_pickled_model(regressor)

    y_pred = regressor.predict(X_test)

    reg_summary['test'] = {'preds': get_test_preds(regressor, X_test, y_test, y_pred=y_pred, nrows=nrows),
                           'scores': get_scores(regressor, X_test, y_test, y_pred=y_pred)}

    if log_charts:
        reg_summary['diagnostics_charts'] = {
            'learning_curve': create_learning_curve_chart(regressor, X_train, y_train),
            'feature_importance': create_feature_importance_chart(regressor, X_train, y_train),
            'residuals': create_residuals_chart(regressor, X_train, X_test, y_train, y_test),
            'prediction_error': create_prediction_error_chart(regressor, X_train, X_test, y_train, y_test),
            'cooks_distance': create_cooks_distance_chart(regressor, X_train, y_train)}

    return reg_summary


def create_classifier_summary(classifier, X_train, X_test, y_train, y_test, nrows=1000, log_charts=True):
    """Create sklearn classifier summary.

    This method creates classifier summary that includes:

    * all classifier parameters,
    * pickled estimator (model),
    * test predictions,
    * test predictions probabilities,
    * test scores,
    * model performance visualizations.

    Returned ``dict`` can be assigned to the experiment's namespace defined by the user (see example below).

    Classifier should be fitted before calling this function.

    Tip:
        Check Sklearn-Neptune integration
        `documentation <https://docs-beta.neptune.ai/essentials/integrations/machine-learning-frameworks/sklearn>`_
        for the full example.

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
        ``dict`` with all summary items.

    Examples:
        Log random forest classifier summary.

        .. code:: python3

            import neptune.new.integrations.sklearn as npt_utils

            rfc = RandomForestClassifier()
            rfc.fit(X_train, y_train)

            exp = neptune.init(project='my_workspace/my_project')
            exp['random_forest/summary'] = npt_utils.create_classifier_summary(rfc, X_train, X_test, y_train, y_test)
    """
    assert is_classifier(classifier), 'classifier should be sklearn classifier.'

    cls_summary = dict()

    cls_summary['all_params'] = get_estimator_params(classifier)
    cls_summary['pickled_model'] = get_pickled_model(classifier)

    y_pred = classifier.predict(X_test)

    cls_summary['test'] = {'preds': get_test_preds(classifier, X_test, y_test, y_pred=y_pred, nrows=nrows),
                           'preds_proba': get_test_preds_proba(classifier, X_test, nrows=nrows),
                           'scores': get_scores(classifier, X_test, y_test, y_pred=y_pred)}

    if log_charts:
        cls_summary['diagnostics_charts'] = {
            'classification_report': create_classification_report_chart(classifier, X_train, X_test, y_train, y_test),
            'confusion_matrix': create_confusion_matrix_chart(classifier, X_train, X_test, y_train, y_test),
            'ROC_AUC': create_roc_auc_chart(classifier, X_train, X_test, y_train, y_test),
            'precision_recall': create_precision_recall_chart(classifier, X_test, y_test),
            'class_prediction_error': create_class_prediction_error_chart(classifier, X_train, X_test, y_train, y_test)}

    return cls_summary


def create_kmeans_summary(model, X, nrows=1000, **kwargs):
    """Create sklearn kmeans summary.

    This method fit KMeans model to data and logs:

    * all kmeans parameters,
    * cluster labels,
    * clustering visualizations: KMeans elbow chart and silhouette coefficients chart.

    Returned ``dict`` can be assigned to the experiment's namespace defined by the user (see example below).

    Tip:
        Check Sklearn-Neptune integration
        `documentation <https://docs-beta.neptune.ai/essentials/integrations/machine-learning-frameworks/sklearn>`_
        for the full example.

    Args:
        model (:obj:`KMeans`):
            | KMeans object.
        X (:obj:`ndarray`):
            | Training instances to cluster.
        nrows (`int`, optional, default is 1000):
            | Number of rows to log in the cluster labels.
        kwargs:
            KMeans parameters.

    Returns:
        ``dict`` with all summary items.

    Examples:
        .. code:: python3

            import neptune.new.integrations.sklearn as npt_utils

            km = KMeans(n_init=11, max_iter=270)
            X, y = make_blobs(n_samples=579, n_features=17, centers=7, random_state=28743)

            exp = neptune.init(project='my_workspace/my_project')
            exp['kmeans/summary'] = npt_utils.create_kmeans_summary(km, X)
    """
    assert isinstance(model, KMeans), 'model should be sklearn KMeans instance'

    kmeans_summary = dict()
    model.set_params(**kwargs)

    kmeans_summary['all_params'] = get_estimator_params(model)
    kmeans_summary['cluster_labels'] = get_cluster_labels(model, X, nrows=nrows, **kwargs)
    kmeans_summary['diagnostics_charts'] = {
        'kelbow': create_kelbow_chart(model, X, **kwargs),
        'silhouette': create_silhouette_chart(model, X, **kwargs)}

    return kmeans_summary


def get_estimator_params(estimator):
    """Get estimator parameters.

    Tip:
        Check Sklearn-Neptune integration
        `documentation <https://docs-beta.neptune.ai/essentials/integrations/machine-learning-frameworks/sklearn>`_
        for the full example.

    Args:
        estimator (:obj:`estimator`):
            | Scikit-learn estimator from which to log parameters.

    Returns:
        ``dict`` with all parameters mapped to their values.

    Examples:
        .. code:: python3

            import neptune.new.integrations.sklearn as npt_utils

            rfr = RandomForestRegressor()

            exp = neptune.init(project='my_workspace/my_project')
            exp['estimator/params'] = npt_utils.get_estimator_params(rfr)
    """
    assert is_regressor(estimator) or is_classifier(estimator) or isinstance(estimator, KMeans),\
        'Estimator should be sklearn regressor, classifier or kmeans clusterer.'

    return estimator.get_params()


def get_pickled_model(estimator):
    """Get pickled estimator.

    Tip:
        Check Sklearn-Neptune integration
        `documentation <https://docs-beta.neptune.ai/essentials/integrations/machine-learning-frameworks/sklearn>`_
        for the full example.

    Args:
        estimator (:obj:`estimator`):
            | Scikit-learn estimator to pickle.

    Returns:
        ``neptune.types.File`` object that you can assign to experiment ``base_namespace``.

    Examples:
        .. code:: python3

            import neptune.new.integrations.sklearn as npt_utils

            rfr = RandomForestRegressor()

            exp = neptune.init(project='my_workspace/my_project')
            exp['estimator/pickled_model'] = npt_utils.get_pickled_model(rfr)
    """
    assert is_regressor(estimator) or is_classifier(estimator),\
        'Estimator should be sklearn regressor or classifier.'

    return neptune.types.File.as_pickle(estimator)


def get_test_preds(estimator, X_test, y_test, y_pred=None, nrows=1000):
    """Get test predictions.

    If you pass ``y_pred``, then predictions are not computed from ``X_test`` data.

    Estimator should be fitted before calling this function.

    Tip:
        Check Sklearn-Neptune integration
        `documentation <https://docs-beta.neptune.ai/essentials/integrations/machine-learning-frameworks/sklearn>`_
        for the full example.

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

    Returns:
        ``neptune.types.File`` object that you can assign to experiment ``base_namespace``.

    Examples:
        .. code:: python3

            import neptune.new.integrations.sklearn as npt_utils

            rfr = RandomForestRegressor()

            exp = neptune.init(project='my_workspace/my_project')
            exp['estimator/pickled_model'] = npt_utils.compute_test_preds(rfr, X_test, y_test)
    """
    assert is_regressor(estimator) or is_classifier(estimator),\
        'Estimator should be sklearn regressor or classifier.'
    assert isinstance(nrows, int), 'nrows should be integer, {} was passed'.format(type(nrows))

    preds = None

    if y_pred is None:
        y_pred = estimator.predict(X_test)

    # single output
    if len(y_pred.shape) == 1:
        df = pd.DataFrame(data={'y_true': y_test, 'y_pred': y_pred})
        df = df.head(n=nrows)
        preds = neptune.types.File.as_html(df)
    # multi output
    if len(y_pred.shape) == 2:
        df = pd.DataFrame()
        for j in range(y_pred.shape[1]):
            df['y_test_output_{}'.format(j)] = y_test[:, j]
            df['y_pred_output_{}'.format(j)] = y_pred[:, j]
        df = df.head(n=nrows)
        preds = neptune.types.File.as_html(df)

    return preds


def get_test_preds_proba(classifier, X_test=None, y_pred_proba=None, nrows=1000):
    """Get test predictions probabilities.

    If you pass ``X_test``, then predictions probabilities are computed from data.

    If you pass ``y_pred_proba``, then predictions probabilities are not computed from ``X_test`` data.

    Estimator should be fitted before calling this function.

    Tip:
        Check Sklearn-Neptune integration
        `documentation <https://docs-beta.neptune.ai/essentials/integrations/machine-learning-frameworks/sklearn>`_
        for the full example.

    Args:
        classifier (:obj:`classifier`):
            | Scikit-learn classifier to compute predictions probabilities.
        X_test (:obj:`ndarray`):
            | Testing data matrix.
        y_pred_proba (:obj:`ndarray`, optional, default is ``None``):
            | Classifier predictions probabilities on test data.
        nrows (`int`, optional, default is 1000):
            | Number of rows to log.

    Returns:
        ``neptune.types.File`` object that you can assign to experiment ``base_namespace``.

    Examples:
        .. code:: python3

            import neptune.new.integrations.sklearn as npt_utils

            rfc = RandomForestClassifier()
            rfc.fit(X_train, y_train)

            exp = neptune.init(project='my_workspace/my_project')
            exp['estimator/pickled_model'] = npt_utils.compute_test_preds(rfc, X_test)
    """
    assert is_classifier(classifier), 'Classifier should be sklearn classifier.'
    assert isinstance(nrows, int), 'nrows should be integer, {} was passed'.format(type(nrows))

    if X_test is not None and y_pred_proba is not None:
        raise ValueError('X_test and y_pred_proba are mutually exclusive')
    if X_test is None and y_pred_proba is None:
        raise ValueError('X_test or y_pred_proba is required')

    if y_pred_proba is None:
        try:
            y_pred_proba = classifier.predict_proba(X_test)
        except Exception as e:
            print('This classifier does not provide predictions probabilities. Error: {}'.format(e))
            return

    df = pd.DataFrame(data=y_pred_proba, columns=classifier.classes_)
    df = df.head(n=nrows)

    return neptune.types.File.as_html(df)


def get_scores(estimator, X, y, y_pred=None):
    """Get estimator scores on ``X``.

    If you pass ``y_pred``, then predictions are not computed from ``X`` and ``y`` data.

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
        estimator (:obj:`estimator`):
            | Scikit-learn estimator to compute scores.
        X (:obj:`ndarray`):
            | Data matrix.
        y (:obj:`ndarray`):
            | Target for testing.
        y_pred (:obj:`ndarray`, optional, default is ``None``):
            | Estimator predictions on data.

    Returns:
        ``dict`` with scores.

    Examples:
        .. code:: python3

            import neptune.new.integrations.sklearn as npt_utils

            rfc = RandomForestClassifier()
            rfc.fit(X_train, y_train)

            exp = neptune.init(project='my_workspace/my_project')
            exp['estimator/scores'] = npt_utils.get_scores(rfc, X, y)
    """
    assert is_regressor(estimator) or is_classifier(estimator),\
        'Estimator should be sklearn regressor or classifier.'

    scores_dict = {}

    if y_pred is None:
        y_pred = estimator.predict(X)

    if is_regressor(estimator):
        # single output
        if len(y_pred.shape) == 1:
            evs = explained_variance_score(y, y_pred)
            me = max_error(y, y_pred)
            mae = mean_absolute_error(y, y_pred)
            r2 = r2_score(y, y_pred)

            scores_dict['explained_variance_score'] = evs
            scores_dict['max_error'] = me
            scores_dict['mean_absolute_error'] = mae
            scores_dict['r2_score'] = r2

        # multi output
        if len(y_pred.shape) == 2:
            r2 = estimator.score(X, y)
            scores_dict['r2_score'] = r2

    elif is_classifier(estimator):
        precision, recall, fbeta_score, support = precision_recall_fscore_support(y, y_pred)
        for i, value in enumerate(precision):
            scores_dict['class_{}'.format(i)] = {'precision': value,
                                                 'recall': recall[i],
                                                 'fbeta_score': fbeta_score[i],
                                                 'support': support[i]}
    return scores_dict


def create_learning_curve_chart(regressor, X_train, y_train):
    """Create learning curve chart.

    Tip:
        Check Sklearn-Neptune integration
        `documentation <https://docs-beta.neptune.ai/essentials/integrations/machine-learning-frameworks/sklearn>`_
        for the full example.

    Args:
        regressor (:obj:`regressor`):
            | Fitted sklearn regressor object
        X_train (:obj:`ndarray`):
            | Training data matrix
        y_train (:obj:`ndarray`):
            | The regression target for training

    Returns:
        ``neptune.types.File`` object that you can assign to experiment ``base_namespace``.

    Examples:
        .. code:: python3

            import neptune.new.integrations.sklearn as npt_utils

            rfr = RandomForestRegressor()
            rfr.fit(X_train, y_train)

            exp = neptune.init(project='my_workspace/my_project')
            exp['visuals/learning_curve'] = npt_utils.create_learning_curve_chart(rfr, X_train, y_train)
    """
    assert is_regressor(regressor), 'regressor should be sklearn regressor.'

    chart = None

    try:
        fig, ax = plt.subplots()
        plot_learning_curve(regressor, X_train, y_train, ax=ax)

        chart = neptune.types.File.as_image(fig)
        plt.close(fig)
    except Exception as e:
        print('Did not log learning curve chart. Error: {}'.format(e))

    return chart


def create_feature_importance_chart(regressor, X_train, y_train):
    """Create feature importance chart.

    Tip:
        Check Sklearn-Neptune integration
        `documentation <https://docs-beta.neptune.ai/essentials/integrations/machine-learning-frameworks/sklearn>`_
        for the full example.

    Args:
        regressor (:obj:`regressor`):
            | Fitted sklearn regressor object
        X_train (:obj:`ndarray`):
            | Training data matrix
        y_train (:obj:`ndarray`):
            | The regression target for training

    Returns:
        ``neptune.types.File`` object that you can assign to experiment ``base_namespace``.

    Examples:
        .. code:: python3

            import neptune.new.integrations.sklearn as npt_utils

            rfr = RandomForestRegressor()
            rfr.fit(X_train, y_train)

            exp = neptune.init(project='my_workspace/my_project')
            exp['visuals/feature_importance'] = npt_utils.create_feature_importance_chart(rfr, X_train, y_train)
    """
    assert is_regressor(regressor), 'regressor should be sklearn regressor.'

    chart = None

    try:
        fig, ax = plt.subplots()
        visualizer = FeatureImportances(regressor, is_fitted=True, ax=ax)
        visualizer.fit(X_train, y_train)
        visualizer.finalize()

        chart = neptune.types.File.as_image(fig)
        plt.close(fig)
    except Exception as e:
        print('Did not log feature importance chart. Error: {}'.format(e))

    return chart


def create_residuals_chart(regressor, X_train, X_test, y_train, y_test):
    """Create residuals chart.

    Tip:
        Check Sklearn-Neptune integration
        `documentation <https://docs-beta.neptune.ai/essentials/integrations/machine-learning-frameworks/sklearn>`_
        for the full example.

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

    Returns:
        ``neptune.types.File`` object that you can assign to experiment ``base_namespace``.

    Examples:
        .. code:: python3

            import neptune.new.integrations.sklearn as npt_utils

            rfr = RandomForestRegressor()
            rfr.fit(X_train, y_train)

            exp = neptune.init(project='my_workspace/my_project')
            exp['visuals/residuals'] = npt_utils.create_residuals_chart(rfr, X_train, X_test, y_train, y_test)
    """
    assert is_regressor(regressor), 'regressor should be sklearn regressor.'

    chart = None

    try:
        fig, ax = plt.subplots()
        visualizer = ResidualsPlot(regressor, is_fitted=True, ax=ax)
        visualizer.fit(X_train, y_train)
        visualizer.score(X_test, y_test)
        visualizer.finalize()
        chart = neptune.types.File.as_image(fig)
        plt.close(fig)
    except Exception as e:
        print('Did not log residuals chart. Error: {}'.format(e))

    return chart


def create_prediction_error_chart(regressor, X_train, X_test, y_train, y_test):
    """Create prediction error chart.

    Tip:
        Check Sklearn-Neptune integration
        `documentation <https://docs-beta.neptune.ai/essentials/integrations/machine-learning-frameworks/sklearn>`_
        for the full example.

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

    Returns:
        ``neptune.types.File`` object that you can assign to experiment ``base_namespace``.

    Examples:
        .. code:: python3

            import neptune.new.integrations.sklearn as npt_utils

            rfr = RandomForestRegressor()
            rfr.fit(X_train, y_train)

            exp = neptune.init(project='my_workspace/my_project')
            exp['prediction_error'] = npt_utils.create_prediction_error_chart(rfr, X_train, X_test, y_train, y_test)
    """
    assert is_regressor(regressor), 'regressor should be sklearn regressor.'

    chart = None

    try:
        fig, ax = plt.subplots()
        visualizer = PredictionError(regressor, is_fitted=True, ax=ax)
        visualizer.fit(X_train, y_train)
        visualizer.score(X_test, y_test)
        visualizer.finalize()
        chart = neptune.types.File.as_image(fig)
        plt.close(fig)
    except Exception as e:
        print('Did not log prediction error chart. Error: {}'.format(e))

    return chart


def create_cooks_distance_chart(regressor, X_train, y_train):
    """Create cooks distance chart.

    Tip:
        Check Sklearn-Neptune integration
        `documentation <https://docs-beta.neptune.ai/essentials/integrations/machine-learning-frameworks/sklearn>`_
        for the full example.

    Args:
        regressor (:obj:`regressor`):
            | Fitted sklearn regressor object
        X_train (:obj:`ndarray`):
            | Training data matrix
        y_train (:obj:`ndarray`):
            | The regression target for training

    Returns:
        ``neptune.types.File`` object that you can assign to experiment ``base_namespace``.

    Examples:
        .. code:: python3

            import neptune.new.integrations.sklearn as npt_utils

            rfr = RandomForestRegressor()
            rfr.fit(X_train, y_train)

            exp = neptune.init(project='my_workspace/my_project')
            exp['visuals/cooks_distance'] = npt_utils.create_cooks_distance_chart(rfr, X_train, y_train)
    """
    assert is_regressor(regressor), 'regressor should be sklearn regressor.'

    chart = None

    try:
        fig, ax = plt.subplots()
        visualizer = CooksDistance(ax=ax)
        visualizer.fit(X_train, y_train)
        visualizer.finalize()
        chart = neptune.types.File.as_image(fig)
        plt.close(fig)
    except Exception as e:
        print('Did not log cooks distance chart. Error: {}'.format(e))

    return chart


def create_classification_report_chart(classifier, X_train, X_test, y_train, y_test):
    """Create classification report chart.

    Tip:
        Check Sklearn-Neptune integration
        `documentation <https://docs-beta.neptune.ai/essentials/integrations/machine-learning-frameworks/sklearn>`_
        for the full example.

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

    Returns:
        ``neptune.types.File`` object that you can assign to experiment ``base_namespace``.

    Examples:
        .. code:: python3

            import neptune.new.integrations.sklearn as npt_utils

            rfc = RandomForestClassifier()
            rfc.fit(X_train, y_train)

            exp = neptune.init(project='my_workspace/my_project')
            exp['visuals/classification_report'] = \
                npt_utils.create_classification_report_chart(rfc, X_train, X_test, y_train, y_test)
    """
    assert is_classifier(classifier), 'classifier should be sklearn classifier.'

    chart = None

    try:
        fig, ax = plt.subplots()
        visualizer = ClassificationReport(classifier, support=True, is_fitted=True, ax=ax)
        visualizer.fit(X_train, y_train)
        visualizer.score(X_test, y_test)
        visualizer.finalize()
        chart = neptune.types.File.as_image(fig)
        plt.close(fig)
    except Exception as e:
        print('Did not log Classification Report chart. Error: {}'.format(e))

    return chart


def create_confusion_matrix_chart(classifier, X_train, X_test, y_train, y_test):
    """Create confusion matrix.

    Tip:
        Check Sklearn-Neptune integration
        `documentation <https://docs-beta.neptune.ai/essentials/integrations/machine-learning-frameworks/sklearn>`_
        for the full example.

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

    Returns:
        ``neptune.types.File`` object that you can assign to experiment ``base_namespace``.

    Examples:
        .. code:: python3

            import neptune.new.integrations.sklearn as npt_utils

            rfc = RandomForestClassifier()
            rfc.fit(X_train, y_train)

            exp = neptune.init(project='my_workspace/my_project')
            exp['visuals/confusion_matrix'] = \
                npt_utils.create_confusion_matrix_chart(rfc, X_train, X_test, y_train, y_test)
    """
    assert is_classifier(classifier), 'classifier should be sklearn classifier.'

    chart = None

    try:
        fig, ax = plt.subplots()
        visualizer = ConfusionMatrix(classifier, is_fitted=True, ax=ax)
        visualizer.fit(X_train, y_train)
        visualizer.score(X_test, y_test)
        visualizer.finalize()
        chart = neptune.types.File.as_image(fig)
        plt.close(fig)
    except Exception as e:
        print('Did not log Confusion Matrix chart. Error: {}'.format(e))

    return chart


def create_roc_auc_chart(classifier, X_train, X_test, y_train, y_test):
    """Create ROC-AUC chart.

    Tip:
        Check Sklearn-Neptune integration
        `documentation <https://docs-beta.neptune.ai/essentials/integrations/machine-learning-frameworks/sklearn>`_
        for the full example.

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

    Returns:
        ``neptune.types.File`` object that you can assign to experiment ``base_namespace``.

    Examples:
        .. code:: python3

            import neptune.new.integrations.sklearn as npt_utils

            rfc = RandomForestClassifier()
            rfc.fit(X_train, y_train)

            exp = neptune.init(project='my_workspace/my_project')
            exp['visuals/roc_auc'] = npt_utils.create_roc_auc_chart(rfc, X_train, X_test, y_train, y_test)
    """
    assert is_classifier(classifier), 'classifier should be sklearn classifier.'

    chart = None

    try:
        fig, ax = plt.subplots()
        visualizer = ROCAUC(classifier, is_fitted=True, ax=ax)
        visualizer.fit(X_train, y_train)
        visualizer.score(X_test, y_test)
        visualizer.finalize()
        chart = neptune.types.File.as_image(fig)
        plt.close(fig)
    except Exception as e:
        print('Did not log ROC-AUC chart. Error {}'.format(e))

    return chart


def create_precision_recall_chart(classifier, X_test, y_test, y_pred_proba=None):
    """Create precision recall chart.

    Tip:
        Check Sklearn-Neptune integration
        `documentation <https://docs-beta.neptune.ai/essentials/integrations/machine-learning-frameworks/sklearn>`_
        for the full example.

    Args:
        classifier (:obj:`classifier`):
            | Fitted sklearn classifier object
        X_test (:obj:`ndarray`):
            | Testing data matrix
        y_test (:obj:`ndarray`):
            | The classification target for testing
        y_pred_proba (:obj:`ndarray`, optional, default is ``None``):
            | Classifier predictions probabilities on test data.

    Returns:
        ``neptune.types.File`` object that you can assign to experiment ``base_namespace``.

    Examples:
        .. code:: python3

            import neptune.new.integrations.sklearn as npt_utils

            rfc = RandomForestClassifier()
            rfc.fit(X_train, y_train)

            exp = neptune.init(project='my_workspace/my_project')
            exp['visuals/precision_recall'] = npt_utils.create_precision_recall_chart(rfc, X_test, y_test)
    """
    assert is_classifier(classifier), 'classifier should be sklearn classifier.'

    chart = None

    if y_pred_proba is None:
        try:
            y_pred_proba = classifier.predict_proba(X_test)
        except Exception as e:
            print('Did not log Precision-Recall chart: this classifier does not provide predictions probabilities.'
                  'Error {}'.format(e))
            return chart

    try:
        fig, ax = plt.subplots()
        plot_precision_recall(y_test, y_pred_proba, ax=ax)
        chart = neptune.types.File.as_image(fig)
        plt.close(fig)
    except Exception as e:
        print('Did not log Precision-Recall chart. Error {}'.format(e))

    return chart


def create_class_prediction_error_chart(classifier, X_train, X_test, y_train, y_test):
    """Create class prediction error chart.

    Tip:
        Check Sklearn-Neptune integration
        `documentation <https://docs-beta.neptune.ai/essentials/integrations/machine-learning-frameworks/sklearn>`_
        for the full example.

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

    Returns:
        ``neptune.types.File`` object that you can assign to experiment ``base_namespace``.

    Examples:
        .. code:: python3

            import neptune.new.integrations.sklearn as npt_utils

            rfc = RandomForestClassifier()
            rfc.fit(X_train, y_train)

            exp = neptune.init(project='my_workspace/my_project')
            exp['visuals/class_prediction_error'] = \
                npt_utils.create_class_prediction_error_chart(rfc, X_train, X_test, y_train, y_test)
    """
    assert is_classifier(classifier), 'classifier should be sklearn classifier.'

    chart = None

    try:
        fig, ax = plt.subplots()
        visualizer = ClassPredictionError(classifier, is_fitted=True, ax=ax)
        visualizer.fit(X_train, y_train)
        visualizer.score(X_test, y_test)
        visualizer.finalize()
        chart = neptune.types.File.as_image(fig)
        plt.close(fig)
    except Exception as e:
        print('Did not log Class Prediction Error chart. Error {}'.format(e))

    return chart


def get_cluster_labels(model, X, nrows=1000, **kwargs):
    """Log index of the cluster label each sample belongs to.

    Tip:
        Check Sklearn-Neptune integration
        `documentation <https://docs-beta.neptune.ai/essentials/integrations/machine-learning-frameworks/sklearn>`_
        for the full example.

    Args:
        model (:obj:`KMeans`):
            | KMeans object.
        X (:obj:`ndarray`):
            | Training instances to cluster.
        nrows (`int`, optional, default is 1000):
            | Number of rows to log.
        kwargs:
            KMeans parameters.

    Returns:
        ``neptune.types.File`` object that you can assign to experiment ``base_namespace``.

    Examples:
        .. code:: python3

            import neptune.new.integrations.sklearn as npt_utils

            km = KMeans(n_init=11, max_iter=270)
            X, y = make_blobs(n_samples=579, n_features=17, centers=7, random_state=28743)

            exp = neptune.init(project='my_workspace/my_project')
            exp['kmeans/cluster_labels'] = npt_utils.get_cluster_labels(km, X)
    """
    assert isinstance(model, KMeans), 'Model should be sklearn KMeans instance.'
    assert isinstance(nrows, int), 'nrows should be integer, {} was passed'.format(type(nrows))

    model.set_params(**kwargs)
    labels = model.fit_predict(X)
    df = pd.DataFrame(data={'cluster_labels': labels})
    df = df.head(n=nrows)

    return neptune.types.File.as_html(df)


def create_kelbow_chart(model, X, **kwargs):
    """Create K-elbow chart for KMeans clusterer.

    Tip:
        Check Sklearn-Neptune integration
        `documentation <https://docs-beta.neptune.ai/essentials/integrations/machine-learning-frameworks/sklearn>`_
        for the full example.

    Args:
        model (:obj:`KMeans`):
            | KMeans object.
        X (:obj:`ndarray`):
            | Training instances to cluster.
        kwargs:
            KMeans parameters.

    Returns:
        ``neptune.types.File`` object that you can assign to experiment ``base_namespace``.

    Examples:
        .. code:: python3

            import neptune.new.integrations.sklearn as npt_utils

            km = KMeans(n_init=11, max_iter=270)
            X, y = make_blobs(n_samples=579, n_features=17, centers=7, random_state=28743)

            exp = neptune.init(project='my_workspace/my_project')
            exp['kmeans/kelbow'] = npt_utils.create_kelbow_chart(km, X)
    """
    assert isinstance(model, KMeans), 'Model should be sklearn KMeans instance.'

    chart = None

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
        chart = neptune.types.File.as_image(fig)
        plt.close(fig)
    except Exception as e:
        print('Did not log KMeans elbow chart. Error {}'.format(e))

    return chart


def create_silhouette_chart(model, X, **kwargs):
    """Create silhouette coefficients charts for KMeans clusterer.

    Charts are computed for j = 2, 3, ..., n_clusters.

    Tip:
        Check Sklearn-Neptune integration
        `documentation <https://docs-beta.neptune.ai/essentials/integrations/machine-learning-frameworks/sklearn>`_
        for the full example.

    Args:
        model (:obj:`KMeans`):
            | KMeans object.
        X (:obj:`ndarray`):
            | Training instances to cluster.
        kwargs:
            KMeans parameters.

    Returns:
        ``neptune.types.FileSeries`` object that you can assign to experiment ``base_namespace``.

    Examples:
        .. code:: python3

            import neptune.new.integrations.sklearn as npt_utils

            km = KMeans(n_init=11, max_iter=270)
            X, y = make_blobs(n_samples=579, n_features=17, centers=7, random_state=28743)

            exp = neptune.init(project='my_workspace/my_project')
            exp['kmeans/silhouette'] = npt_utils.create_silhouette_chart(km, X, n_clusters=12)
    """
    assert isinstance(model, KMeans), 'Model should be sklearn KMeans instance.'

    charts = []

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
            charts.append(neptune.types.File.as_image(fig))
            plt.close(fig)
        except Exception as e:
            print('Did not log Silhouette Coefficients chart. Error {}'.format(e))

    return neptune.types.FileSeries(charts)
