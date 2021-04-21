from neptune.new.types import File
from prophet.plot import plot_plotly, plot_components_plotly
from prophet.plot import plot_cross_validation_metric

def log_config(run, model):
    run['model/config/growth'] = model.growth
    run['model/config/changepoints'] = model.changepoints
    run['model/config/n_changepoints'] = model.n_changepoints
    run['model/config/changepoint_range'] = model.changepoint_range
    run['model/config/yearly_seasonality'] = model.yearly_seasonality
    run['model/config/weekly_seasonality'] = model.weekly_seasonality
    run['model/config/daily_seasonality'] = model.daily_seasonality
    run['model/config/holidays'] = model.holidays
    run['model/config/seasonality_mode'] = model.seasonality_mode
    run['model/config/seasonality_prior_scale'] = model.seasonality_prior_scale
    run['model/config/holidays_prior_scale'] = model.holidays_prior_scale
    run['model/config/changepoint_prior_scale'] = model.changepoint_prior_scale
    run['model/config/mcmc_samples'] = model.mcmc_samples
    run['model/config/interval_width'] = model.interval_width
    run['model/config/uncertainty_samples'] = model.uncertainty_samples
    run['model/config/stan_backend'] = model.stan_backend  
    
def log_model(run, model): 
    run['model/params'] = model.params


def log_forecast_plots(run, model, forecast, log_interactive=True):
    if log_interactive:

        fig1 = plot_plotly(model, forecast)
        fig2 = plot_components_plotly(model, forecast)
        run['plots/forecast'] = File.as_html(fig1)
        run['plots/forecast_components'] = File.as_html(fig2)
    else:
        fig1 = model.plot(forecast)
        fig2 = model.plot_components(forecast)
        run['plots/forecast'].upload(fig1)
        run['plots/forecast_components'].upload(fig2)


def log_plot_cross_validation_metric(run, cv, metric='mse', log_interactive=True):
    fig = plot_cross_validation_metric(cv, metric)

    if log_interactive:
        run['plots/cross_validation_metric'] = File.as_html(fig)
    else:
        run['plots/cross_validation_metric'].upload(fig)

def log_prophet_metadata(run, model, forecast=None, cv=None, metric='mse', log_interactive=True):
    log_config(run, model)
    log_model(run, model)

    if forecast:
        log_forecast_plots(run, model, forecast, log_interactive=log_interactive)

    if cv:
        log_plot_cross_validation_metric(run, cv, metric=metric, log_interactive=log_interactive)