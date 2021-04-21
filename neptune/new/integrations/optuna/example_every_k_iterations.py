import lightgbm as lgb
import optuna
from sklearn.datasets import load_breast_cancer
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split


def objective(trial):
    data, target = load_breast_cancer(return_X_y=True)
    train_x, test_x, train_y, test_y = train_test_split(data, target, test_size=0.25)
    dtrain = lgb.Dataset(train_x, label=train_y)

    param = {
        'verbose': -1,
        'objective': 'binary',
        'metric': 'binary_logloss',
        'num_leaves': trial.suggest_int('num_leaves', 2, 256),
        'feature_fraction': trial.suggest_uniform('feature_fraction', 0.2, 1.0),
        'bagging_fraction': trial.suggest_uniform('bagging_fraction', 0.2, 1.0),
        'min_child_samples': trial.suggest_int('min_child_samples', 3, 100),
    }

    gbm = lgb.train(param, dtrain)
    preds = gbm.predict(test_x)
    accuracy = roc_auc_score(test_y, preds)

    return accuracy

import neptune.new as neptune

run = neptune.init(api_token='ANONYMOUS', project='common/optuna-integration',
                   source_files=['example_every_k_iterations.py', 'requirements.txt'])

import optuna_wip as opt_utils
neptune_callback = opt_utils.NeptuneCallback(run, log_plots_freq=5)

study = optuna.create_study(direction='maximize')
study.optimize(objective, n_trials=20, callbacks=[neptune_callback])