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

import json
import subprocess
import warnings

import matplotlib.pyplot as plt
import xgboost as xgb

import neptune.new as neptune
from neptune.new.internal.utils import verify_type

from neptune_xgboost import __version__


class NeptuneCallback(xgb.callback.TrainingCallback):

    def __init__(self,
                 run,  # Neptune run, required
                 base_namespace='training',  # if none we apply 'training' by default
                 log_model=True,  # log model as pickled object at the end of training
                 log_importance=True,  # requires matplotlib, log feature importance chart at the end of training
                 max_num_features=None,  # requires matplotlib, number of top features on the feature importance chart
                 log_tree=None):  # requires graphviz, indices of trained trees to log as chart, i.e. [0, 1, 2]

        verify_type('run', run, neptune.Run)
        verify_type('base_namespace', base_namespace, str)
        log_model is not None and verify_type('log_model', log_model, bool)
        log_importance is not None and verify_type('log_importance', log_importance, bool)
        max_num_features is not None and verify_type('max_num_features', max_num_features, int)
        log_tree is not None and verify_type('log_tree', log_tree, list)

        self.run = run[base_namespace]
        self.log_model = log_model
        self.log_importance = log_importance
        self.max_num_features = max_num_features
        self.log_tree = log_tree
        self.cv = False

        if self.log_tree:
            try:
                subprocess.call(['dot', '-V'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            except OSError:
                self.log_tree = None
                message = "Graphviz executables not found, so trees will not be logged. " \
                          "Make sure the Graphviz executables are on your systems' PATH"
                warnings.warn(message)

    def before_training(self, model):
        if hasattr(model, 'cvfolds'):
            self.cv = True
        return model

    def after_training(self, model):
        # model structure is different for 'cv' and 'train' functions that you use to train xgb model
        if self.cv:
            for i, fold in enumerate(model.cvfolds):
                self.run[f'fold_{i}/booster_config'] = json.loads(fold.bst.save_config())
        else:
            self.run['booster_config'] = json.loads(model.save_config())
            if 'best_score' in model.attributes().keys():
                self.run['best_score'] = model.attributes()['best_score']
            if 'best_iteration' in model.attributes().keys():
                self.run['best_iteration'] = model.attributes()['best_iteration']

        if self.log_importance:
            # for 'cv' log importance chart per fold
            if self.cv:
                for i, fold in enumerate(model.cvfolds):
                    importance = xgb.plot_importance(fold.bst, max_num_features=self.max_num_features)
                    self.run[f'fold_{i}/plots/importance'].upload(neptune.types.File.as_image(importance.figure))
                plt.close('all')
            else:
                importance = xgb.plot_importance(model, max_num_features=self.max_num_features)
                self.run['plots/importance'].upload(neptune.types.File.as_image(importance.figure))
                plt.close('all')

        if self.log_tree is not None:
            # for 'cv' log trees for each cv fold (different model is trained on each fold)
            if self.cv:
                for i, fold in enumerate(model.cvfolds):
                    trees = []
                    for j in self.log_tree:
                        tree = xgb.plot_tree(fold.bst, num_trees=j)
                        trees.append(neptune.types.File.as_image(tree.figure))
                    self.run[f'fold_{i}/plots/trees'] = neptune.types.FileSeries(trees)
                    plt.close('all')
            else:
                trees = []
                for j in self.log_tree:
                    tree = xgb.plot_tree(model, num_trees=j)
                    trees.append(neptune.types.File.as_image(tree.figure))
                self.run['plots/trees'] = neptune.types.FileSeries(trees)
                plt.close('all')

        if self.log_model:
            # for 'cv' log model per fold
            if self.cv:
                for i, fold in enumerate(model.cvfolds):
                    self.run[f'fold_{i}/model_pickle'].upload(neptune.types.File.as_pickle(fold.bst))
            else:
                self.run['model_pickle'].upload(neptune.types.File.as_pickle(model))
        return model

    def before_iteration(self, model, epoch: int,
                         evals_log: xgb.callback.CallbackContainer.EvalsLog) -> bool:
        # False to indicate training should not stop.
        return False

    def after_iteration(self, model, epoch: int,
                        evals_log: xgb.callback.CallbackContainer.EvalsLog) -> bool:
        self.run['epoch'].log(epoch)

        for stage, metrics_dict in evals_log.items():
            for metric_name, metric_values in evals_log[stage].items():
                if self.cv:
                    mean, std = metric_values[-1]
                    self.run[stage][metric_name]['mean'].log(mean)
                    self.run[stage][metric_name]['std'].log(std)
                else:
                    self.run[stage][metric_name].log(metric_values[-1])

        if self.cv:
            config = json.loads(model.cvfolds[0].bst.save_config())
        else:
            config = json.loads(model.save_config())
        lr = config['learner']['gradient_booster']['updater']['grow_colmaker']['train_param']['learning_rate']
        self.run['learning_rate'].log(float(lr))
        return False
