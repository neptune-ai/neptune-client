![image](https://neptune.ai/wp-content/uploads/neptune-logo-less-margin.png)

[![PyPI version](https://badge.fury.io/py/neptune-client.svg)](https://badge.fury.io/py/neptune-client)
[![Build Status](https://travis-ci.org/neptune-ai/neptune-client.svg?branch=master)](https://travis-ci.org/neptune-ai/neptune-client)
[![Maintenance](https://img.shields.io/badge/Maintained%3F-YES-green.svg)](https://github.com/neptune-ai/neptune-client/graphs/commit-activity)

[![twitter](https://img.shields.io/twitter/follow/neptune_ai.svg?label=Follow)](https://twitter.com/neptune_ai)
![youtube](https://img.shields.io/youtube/views/9iX6DxcijO8?style=social)

# Lightweight experiment tracking tool for AI/ML individuals and teams. Fits any workflow.
Neptune is a lightweight experiment logging/tracking tool that helps you with your machine learning experiments.

Neptune is suitable for **indvidual**, **commercial** and **research** projects. It can especially help you with the following:

* [Logging experiments metadata](https://docs.neptune.ai/logging-and-managing-experiment-results/index.html)
* [Monitoring ML runs live](https://docs.neptune.ai/getting-started/quick-starts/how-to-monitor-live.html#use-cases-monitor-runs-live)
* [Organizing and exploring experiments results](https://docs.neptune.ai/organizing-and-exploring-results-in-the-ui/index.html)
* [Comparing/debugging ML experiments and models](https://docs.neptune.ai/getting-started/quick-starts/how-to-compare-experiments.html#use-cases-compare-and-debug-experiments)
* [Sharing results of experiments with your team/departament](https://docs.neptune.ai/getting-started/quick-starts/how-to-share-results.html#use-cases-share-results-with-team)

## What is neptune-client?
`neptune-client` is a Python library that serves three purposes:

* logging machine learning experiments,
* updating existing experiment with new data and visualizations,
* downloading experiment data from Neptune to local machine.

It is designed to be:

* *lightweight*: low setup effort,
* *generic*: capable of logging any kind of machine learning work
* *straightforward*: user defines what to keep track of during experiment to use.

### See how it works.
Check Neptune API Tour, for hands-on intro to Neptune:

[![github-code](https://img.shields.io/badge/GitHub-code-informational?logo=github)](https://github.com/neptune-ai/neptune-examples/blob/master/README.md)
[![jupyter-code](https://img.shields.io/badge/Jupyter-code-informational?logo=jupyter)](https://github.com/neptune-ai/neptune-examples/blob/master/README.md)
[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/neptune-ai/neptune-examples/blob/master/product-tours/how-it-works/showcase/Neptune-API-Tour.ipynb)

# Use Neptune with your favourite AI/ML libraries.
![frameworks-logos](https://docs.neptune.ai/_static/images/integrations/framework-logos.png)

Neptune comes with 25+ integrations with Python libraries popular in machine learning, deep learning and reinforcement learning.

Integrations lets you automaticaly:

* log training, validation and testing metrics, and visualize them in Neptune UI,
* log experiment hyper-parameters,
* monitor hardware usage,
* log performance charts and images,
* save model checkpoints,
* log interactive visualizations,
* log csv files, pandas Datraframes,
* [much more](https://docs.neptune.ai/logging-and-managing-experiment-results/logging-experiment-data.html#what-you-can-log).

## Use with PyTorch Lightning
PyTorch Lightning is a lightweight PyTorch wrapper for high-performance AI research. You can log PyTorch Lightning experiments to Neptune using `NeptuneLogger` (part of the pytorch-lightning library).

Integration is 
```
from pytorch_lightning.loggers.neptune import NeptuneLogger

# Create NeptuneLogger
neptune_logger = NeptuneLogger(
    api_key="ANONYMOUS",
    project_name="shared/pytorch-lightning-integration",
    params=PARAMS)

# Pass NeptuneLogger to the Trainer
trainer = pl.Trainer(max_epochs=PARAMS['max_epochs'],
                     logger=neptune_logger)

# Fit model, have everything logged automatically
model = LitModel()
trainer.fit(model, train_loader)
```



https://ui.neptune.ai/o/shared/org/pytorch-lightning-integration/e/PYTOR-137930/charts

## Use with TensorFow and Keras

## Use with Scikit-learn

## Use with XGBoost or LightGBM

## Use with Optuna or scikit-opitmize

## There is more
:arrow_right: [integrations page](https://docs.neptune.ai/integrations/index.html)

# Getting help
If you got stuck or simply want to talk to us about something here are your options:

* [documentation](https://docs.neptune.ai),
* [video tutorials](https://www.youtube.com/playlist?list=PLKePQLVx9tOd8TEGdG4PAKz0Owqdv1aaw),
* Chat! When in application click on the [blue message icon](https://docs.neptune.ai/_static/images/getting-started/intercom.gif) in the bottom-right corner and send a message. A real person will talk to you ASAP (typically very ASAP),
* You can just shoot us an email at [contact@neptune.ai](mailto:contact@neptune.ai).

# People behind Neptune
Created with :heart: by the [Neptune.ai team](https://neptune.ai/about-us):

Piotr, Michał, Jakub, Paulina, Kamil, Małgorzata, Piotr, Aleksandra, Marcin, Hubert, Adam, Szymon, Jakub, Maciej, Piotr, Paweł, Patrycja, Grzegorz, Paweł, Natalia, Marcin and [you?](https://neptune.ai/jobs)

![neptune.ai](https://neptune.ai/wp-content/uploads/2020/04/logo.png)
