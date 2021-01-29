<div align="center">
  <img src="https://neptune.ai/wp-content/uploads/neptune-logo-less-margin-e1611939742683.png" width="500" /><br><br>
</div>

[![PyPI version](https://badge.fury.io/py/neptune-client.svg)](https://badge.fury.io/py/neptune-client)
[![Build Status](https://travis-ci.org/neptune-ai/neptune-client.svg?branch=master)](https://travis-ci.org/neptune-ai/neptune-client)
[![Maintenance](https://img.shields.io/badge/Maintained%3F-YES-green.svg)](https://github.com/neptune-ai/neptune-client/graphs/commit-activity)

[![neptune-blog](https://img.shields.io/badge/Neptune-blog-informational?logo=data:image/png;base64,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)](https://neptune.ai/blog)
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

# Features to help you get the job done
* Rich logging and tracking methods
* easy to use client
* experiments compare
* dashbordign views
* team management

## What is neptune-client?
`neptune-client` is a Python library that serves three purposes:

* logging machine learning experiments,
* updating existing experiment with new data and visualizations,
* downloading experiment data from Neptune to local machine.

It is designed to be:

* **lightweight**: low setup effort,
* **generic**: capable of logging any kind of machine learning work
* **straightforward**: user defines what to keep track of during experiment to use.

### See how `neptune-client` works.
`pip install neptune-client`

`conda install -c conda-forge neptune-client`

```
import neptune

neptune.init('my_workspace/my_project')
neptune.create_experiment()

for epoch in range(epochs):
    ...
    neptune.log_metric('loss', loss)
    neptune.log_metric('metric', accuracy)

neptune.log_artifact('model_weights.pth')
```

For the hands-on intro to neptune-client check this API Tour:

[![github-code](https://img.shields.io/badge/GitHub-code-informational?logo=github)](https://github.com/neptune-ai/neptune-examples/blob/master/product-tours/how-it-works/docs/Neptune-API-Tour.py)
[![jupyter-code](https://img.shields.io/badge/Jupyter-code-informational?logo=jupyter)](https://github.com/neptune-ai/neptune-examples/blob/master/product-tours/how-it-works/showcase/Neptune-API-Tour.ipynb)
[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/neptune-ai/neptune-examples/blob/master/product-tours/how-it-works/showcase/Neptune-API-Tour.ipynb)

# Use Neptune with your favourite AI/ML libraries.
![frameworks-logos](https://docs.neptune.ai/_static/images/integrations/framework-logos.png)

Neptune comes with 25+ integrations with Python libraries popular in machine learning, deep learning and reinforcement learning.

Integrations lets you automatically:

* log training, validation and testing metrics, and visualize them in Neptune UI,
* log experiment hyper-parameters,
* monitor hardware usage,
* log performance charts and images,
* save model checkpoints,
* log interactive visualizations,
* log csv files, pandas Datraframes,
* [much more](https://docs.neptune.ai/logging-and-managing-experiment-results/logging-experiment-data.html#what-you-can-log).

## Use with PyTorch Lightning
PyTorch Lightning is a lightweight PyTorch wrapper for high-performance AI research. You can automatically log PyTorch Lightning experiments to Neptune using `NeptuneLogger` (part of the pytorch-lightning library).

The integration looks like this:
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
[![neptune-pl](https://img.shields.io/badge/PytorchLightning-experiment-success?logo=data:image/png;base64,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)](https://ui.neptune.ai/o/shared/org/pytorch-lightning-integration/e/PYTOR-137930/charts)

Check full code example (pick your favourite medium):

[![github-code](https://img.shields.io/badge/GitHub-code-informational?logo=github)](https://github.com/neptune-ai/neptune-examples/blob/master/integrations/pytorch-lightning/docs/Neptune-PyTorch-Lightning-advanced.py)
[![jupyter-code](https://img.shields.io/badge/Jupyter-code-informational?logo=jupyter)](https://github.com/neptune-ai/neptune-examples/blob/master/integrations/pytorch-lightning/showcase/Neptune-PyTorch-Lightning-advanced.ipynb)
[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/neptune-ai/neptune-examples/blob/master/integrations/pytorch-lightning/showcase/Neptune-PyTorch-Lightning-advanced.ipynb)

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

# Neptune.ai is trusted by great companies
![Roche](https://neptune.ai/wp-content/uploads/Roche-logo.png)
![nnaisense](https://neptune.ai/wp-content/uploads/2020/07/NNAISENSE.png)
![senseye](https://neptune.ai/wp-content/uploads/2020/06/Senseye-1.png)

![zestyai](https://neptune.ai/wp-content/uploads/2020/07/Zesty.png)
![newyorker](https://neptune.ai/wp-content/uploads/2020/07/NewYorker-2.png)
![intive](https://neptune.ai/wp-content/uploads/2020/07/Intive-1.png)

# People behind Neptune
Created with :heart: by the [Neptune.ai team](https://neptune.ai/about-us):

Piotr, Michał, Jakub, Paulina, Kamil, Małgorzata, Piotr, Aleksandra, Marcin, Hubert, Adam, Szymon, Jakub, Maciej, Piotr, Paweł, Patrycja, Grzegorz, Paweł, Natalia, Marcin and [you?](https://neptune.ai/jobs)

![neptune.ai](https://neptune.ai/wp-content/uploads/2020/04/logo.png)
