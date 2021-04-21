import pandas as pd
from prophet import Prophet
from prophet.diagnostics import cross_validation

import neptune.new as neptune
import neptune_prophet as prophet_utils

# create a run, log code and requirements,
# your hardware consumption will be logged automatically if you have psutil installed
run = neptune.init(api_token='ANONYMOUS', project='common/prophet-integration',
                   source_files=['requirements.txt', 'example_granular.py'])

df = pd.read_csv('example_wp_log_peyton_manning.csv')

# log model config (hyperparameters)
m = Prophet()
prophet_utils.log_config(run, m)

# log model (params)
m.fit(df)
prophet_utils.log_model(run, m)

# log forecast plots (can be interactive)
future = m.make_future_dataframe(periods=365)
forecast = m.predict(future)
prophet_utils.log_forecast_plots(run, m, forecast, log_interactive=True)

# log cross validation plots (can be interactive)
df_cv = cross_validation(m, initial="730 days", period="180 days", horizon="365 days")
prophet_utils.log_plot_cross_validation_metric(run, df_cv, log_interactive=True)
