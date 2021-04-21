import optuna

import neptune.new as neptune


class NeptuneCallback:
    def __init__(self, run,
                 log_plots_freq=1,
                 log_study_freq=1,
                 log_trials_df_freq=1,
                 last_trial_id=None,
                 vis_backend='plotly',
                 log_plot_contour=True,
                 log_plot_edf=True,
                 log_plot_parallel_coordinate=True,
                 log_plot_param_importances=True,
                 log_plot_pareto_front=True,
                 log_plot_slice=True,
                 log_plot_intermediate_values=True,
                 log_plot_optimization_history=True, ):

        self.run = run

        if any(l == 'last' for l in [log_plots_freq, log_study_freq, log_trials_df_freq]):
            assert last_trial_id, "Wne using log_{}_freq 'last' you need to specify last_trial_id"

        self._last_trial_id = last_trial_id
        self._vis_backend = vis_backend
        self._log_plots_freq = log_plots_freq
        self._log_study_freq = log_study_freq
        self._log_trials_df_freq = log_trials_df_freq
        self._log_plot_contour = log_plot_contour
        self._log_plot_edf = log_plot_edf
        self._log_plot_parallel_coordinate = log_plot_parallel_coordinate
        self._log_plot_param_importances = log_plot_param_importances
        self._log_plot_pareto_front = log_plot_pareto_front
        self._log_plot_slice = log_plot_slice
        self._log_plot_intermediate_values = log_plot_intermediate_values
        self._log_plot_optimization_history = log_plot_optimization_history

    def __call__(self, study, trial):
        self.run['trials/values'].log(trial.value)
        self.run['trials/params'].log(trial.params)
        self.run['trials/values|params'].log(f'value: {trial.value}| params: {trial.params}')

        self.run[f'trials/trials/{trial._trial_id}/datetime_start'] = trial.datetime_start
        self.run[f'trials/trials/{trial._trial_id}/datetime_complete'] = trial.datetime_complete
        self.run[f'trials/trials/{trial._trial_id}/duration'] = trial.duration
        self.run[f'trials/trials/{trial._trial_id}/distributions'] = trial.distributions
        self.run[f'trials/trials/{trial._trial_id}/intermediate_values'] = trial.intermediate_values
        self.run[f'trials/trials/{trial._trial_id}/params'] = trial.params
        self.run[f'trials/trials/{trial._trial_id}/value'] = trial.value
        self.run[f'trials/trials/{trial._trial_id}/values'] = trial.values

        self.run['best/value'] = study.best_value
        self.run['best/params'] = study.best_params
        self.run['best/value|params'].log(f'value: {study.best_value}| params: {study.best_params}')

        for _trial in study.best_trials:
            self.run[f'best/trials/{_trial._trial_id}/datetime_start'] = _trial.datetime_start
            self.run[f'best/trials/{_trial._trial_id}/datetime_complete'] = _trial.datetime_complete
            self.run[f'best/trials/{_trial._trial_id}/duration'] = _trial.duration
            self.run[f'best/trials/{_trial._trial_id}/distributions'] = _trial.distributions
            self.run[f'best/trials/{_trial._trial_id}/intermediate_values'] = _trial.intermediate_values
            self.run[f'best/trials/{_trial._trial_id}/params'] = _trial.params
            self.run[f'best/trials/{_trial._trial_id}/value'] = _trial.value
            self.run[f'best/trials/{_trial._trial_id}/values'] = _trial.values

        # import pdb; pdb.set_trace()
        if trial._trial_id == 0:
            log_study_metadata(self.run, study)

        if self._should_log_plots(study, trial):
            log_plots(self.run, study,
                      backend=self._vis_backend,
                      log_plot_contour=self._log_plot_contour,
                      log_plot_edf=self._log_plot_edf,
                      log_plot_parallel_coordinate=self._log_plot_parallel_coordinate,
                      log_plot_param_importances=self._log_plot_param_importances,
                      log_plot_pareto_front=self._log_plot_pareto_front,
                      log_plot_slice=self._log_plot_slice,
                      log_plot_optimization_history=self._log_plot_optimization_history,
                      log_plot_intermediate_values=self._log_plot_intermediate_values,

                      )

        if self._should_log_study(study, trial):
            log_study(self.run, study)

    def _should_log_plots(self, study, trial):
        if self._log_plots_freq == 'last':
            if study._stop_flag or trial._trial_id == self._last_trial_id:
                return True
        else:
            if trial._trial_id % self._log_plots_freq == 0:
                return True
        return False

    def _should_log_study(self, study, trial):
        if self._log_study_freq == 'last':
            if study._stop_flag or trial._trial_id == self._last_trial_id:
                return True
        else:
            if trial._trial_id % self._log_study_freq == 0:
                return True
        return False


def log_study_metadata(run, study):
    run['study/study_name'] = study.study_name
    run['study/direction'] = study.direction
    run['study/directions'] = study.directions
    run['study/system_attrs'] = study.system_attrs
    run['study/user_attrs'] = study.user_attrs
    run['study/_study_id'] = study._study_id
    run['study/_storage'] = study._storage


def log_study(run, study):
    if type(study._storage) is optuna.storages._in_memory.InMemoryStorage:
        """pickle and log the study object to the 'study/study.pkl' path"""
        run['study/study_name'] = study.study_name
        run['study/_storage'] = study._storage
        run['study/storage_type'] = 'InMemoryStorage'
        run['study/study'].upload(neptune.types.File.from_stream(stream=export_pickle(study), extension='pkl'))
        pass
    else:
        run['study/study_name'] = study.study_name
        run['study/_storage'] = study._storage
        run['study/storage_type'] = 'DBStorage'  # "RDBStorage", "RedisStorage",
        """LOG just the link to _storage"""


def export_pickle(obj):
    from io import BytesIO
    import pickle

    buffer = BytesIO()
    pickle.dump(obj, buffer)
    buffer.seek(0)

    return buffer


def load_study_from_run(run):
    if run['study/storage_type'].fetch() == 'InMemoryStorage':
        """download and unplickle the run['study/study.pkl']"""
        return get_pickle(path='study/study', run=run)
        pass
    else:
        optuna.load_study(study_name=run['study/study_name'], storage=run['study/_storage'])
    pass


def get_pickle(path, run):
    import os
    import tempfile
    import joblib

    with tempfile.TemporaryDirectory() as d:
        run[path].download(destination=d)
        filepath = os.listdir(d)[0]
        full_path = os.path.join(d, filepath)
        artifact = joblib.load(full_path)
    return artifact


def log_plots(run, study,
              backend='plotly',
              log_plot_contour=True,
              log_plot_edf=True,
              log_plot_parallel_coordinate=True,
              log_plot_param_importances=True,
              log_plot_pareto_front=True,
              log_plot_slice=True,
              log_plot_intermediate_values=True,
              log_plot_optimization_history=True,
              ):
    if backend == 'matplotlib':
        import optuna.visualization.matplotlib as vis
    elif backend == 'plotly':
        import optuna.visualization as vis
    else:
        raise NotImplementedError(f'{backend} backend is not implemented')

    if vis.is_available:
        if log_plot_contour:
            run['visualizations/plot_contour'] = neptune.types.File.as_html(vis.plot_contour(study))
        if log_plot_edf:
            run['visualizations/plot_edf'] = neptune.types.File.as_html(vis.plot_edf(study))
        if log_plot_parallel_coordinate:
            run['visualizations/plot_parallel_coordinate'] = neptune.types.File.as_html(
                vis.plot_parallel_coordinate(study))
        if log_plot_param_importances and len(study.trials) > 1:
            run['visualizations/plot_param_importances'] = neptune.types.File.as_html(vis.plot_param_importances(study))
        if log_plot_pareto_front and study._is_multi_objective() and backend == 'plotly':
            run['visualizations/plot_pareto_front'] = neptune.types.File.as_html(vis.plot_pareto_front(study))
        if log_plot_slice:
            run['visualizations/plot_slice'] = neptune.types.File.as_html(vis.plot_slice(study))
        if log_plot_intermediate_values:
            run['visualizations/plot_intermediate_values'] = neptune.types.File.as_html(
                vis.plot_intermediate_values(study))
        if log_plot_optimization_history:
            run['visualizations/plot_optimization_history'] = neptune.types.File.as_html(
                vis.plot_optimization_history(study))
