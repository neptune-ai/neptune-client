import pandas as pd
from prophet import Prophet
from prophet.diagnostics import cross_validation

import neptune.new as neptune
import neptune_prophet as prophet_utils

# create a run, log code and requirements
# your hardware consumption will be logged automatically if you have psutil installed
run = neptune.init(api_token='ANONYMOUS', project='common/prophet-integration',
                   source_files=['requirements.txt', 'example_one_liner.py'])

df = pd.read_csv('example_wp_log_peyton_manning.csv')

m = Prophet()
m.fit(df)

future = m.make_future_dataframe(periods=365)
forecast = m.predict(future)

df_cv = cross_validation(m, initial="730 days", period="180 days", horizon="365 days")

# log model config, model params, forecast plots, and cross validation plots
prophet_utils.log_prophet_metadata(run, m, forecast, df_cv)
