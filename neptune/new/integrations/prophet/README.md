# Neptune + Prophet example

## What you will get

[Watch the video](https://www.loom.com/share/692ec75bc3034b0f8d5c27b04bd2efea)

You can see examples of Prophet training runs logged to Neptune here:

*  [See example in Neptune](https://app.neptune.ai/o/common/org/prophet-integration/e/PROP-7/all?path=plots)

This integration/example logs:
* source files for training
* environment setup (requirements.txt)
* model configuration (hyperparameters)
* model itself (params)
* forecast plots (can be both interactive or static)
* cross validation plots (can be both interactive or static)

## How to run examples

### Step 1: Download files and data
 
Download everything from the ``neptune/new/integrations/prophet`` directory`.
You can [find it here](https://github.com/neptune-ai/neptune-client/tree/integration/prophet/neptune/new/integrations/prophet).

### Step 2: Install packages

Install the basic requirements from the ``requirements.txt`` file

```bash
pip install -r requirements.txt
```

### Step 3: Run examples

To run examples simply go:

```bash
python example_granular.py
```

or 

```bash
python example_one_liner.py
```

They will result in the same thing logged to Neptune but they have a little bit different logging code:

* ``example_granular.py`` adds logging explicitly for different objects at different training stages

```python
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

```

* ``example_one_liner.py`` logs everything at once at the end of training

```python
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
```

## How to use it in your own code

All logging functions are located in the ``neptune_prophet.py``.
You can use simply copy the ``neptune_prohpet.py`` to your repository and import functions from it. 
Included functions:
* ``log_prophet_metadata``: logs all the metadata in available 
* ``log_plot_cross_validation_metric``: logs the cross validation plot (can be interactive or static)
* ``log_forecast_plots``: logs forecats plots (can be interactive or static)
* ``log_model``: logs the actual model parameters
* ``log_config``: logs model configuration (hyperparameters)
