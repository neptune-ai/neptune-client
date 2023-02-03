<div align="center">
    <img src="https://raw.githubusercontent.com/neptune-ai/neptune-client/assets/readme/github-banner.jpeg" width="1500" />
    &nbsp;
 <h1>neptune.ai</h1>
</div>

<div align="center">
  <a href="https://docs.neptune.ai/usage/quickstart/">Quickstart</a>
  <span>&nbsp;&nbsp;•&nbsp;&nbsp;</span>
  <a href="https://neptune.ai/">Website</a>
  <span>&nbsp;&nbsp;•&nbsp;&nbsp;</span>
  <a href="https://docs.neptune.ai/">Docs</a>
  <span>&nbsp;&nbsp;•&nbsp;&nbsp;</span>
  <a href="https://github.com/neptune-ai/examples">Examples</a>
  <span>&nbsp;&nbsp;•&nbsp;&nbsp;</span>
  <a href="https://neptune.ai/resources">Resource center</a>
  <span>&nbsp;&nbsp;•&nbsp;&nbsp;</span>
  <a href="https://neptune.ai/blog">Blog</a>
  <span>&nbsp;&nbsp;•&nbsp;&nbsp;</span>
  <a href="https://neptune.ai/events">Podcast</a>
&nbsp;
  <hr />
</div>

## What is neptune.ai?

neptune.ai makes it easy to log, store, organize, compare, register, and share <b>all your ML model metadata in a single place</b>.

* Automate and standardize as your modeling team grows.
* Collaborate on models and results with your team and across the org.
* Use hosted, deploy on-premises, or in a private cloud. Integrate with any MLOps stack.
&nbsp;

&nbsp;
<div align="center">
    <a href="https://youtu.be/mv9jxexmbBk">
      <img border="0" alt="neptune.ai explainer video" src="https://raw.githubusercontent.com/neptune-ai/neptune-client/assets/readme/github-explainer-video.png" width="600">
    </a>
</div>
&nbsp;

&nbsp;
<a href="https://app.neptune.ai/showcase/example-project-tensorflow-keras/experiments?split=tbl&dash=charts&viewId=eccd5adf-42b3-497e-9cc2-9fa2655429b3"><b>Play with a live neptune.ai app →</b></a>
&nbsp;

&nbsp;
## Getting started

**Step 1:** Create a **[free account](https://neptune.ai/register)**

**Step 2:** Install Neptune client library

```bash
pip install neptune-client
```

**Step 3:** Add experiment tracking snippet to your code

```python
import neptune.new as neptune

run = neptune.init_run("Me/MyProject")
run["parameters"] = {"lr": 0.1, "dropout": 0.4}
run["test_accuracy"] = 0.84

```

[![Open in Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/neptune-ai/examples/blob/master/how-to-guides/hello-neptune/notebooks/hello_neptune.ipynb)
&nbsp;

&nbsp;
## Core features

**Log and display**

Add a snippet to any step of your ML pipeline once. Decide what and how you want to log. Run a million times.

* <a href="https://docs.neptune.ai/integrations/"><b>Any framework:</b></a> any code, PyTorch, PyTorch Lightning, TensorFlow/Keras, scikit-learn, LightGBM, XGBoost, Optuna, Kedro.

* <a href="https://docs.neptune.ai/logging/what_you_can_log/"><b>Any metadata type:</b></a> metrics, parameters, dataset and model versions, images, interactive plots, videos, hardware (GPU, CPU, memory), code state.

* <a href="https://docs.neptune.ai/usage/pipelines/"><b>From anywhere in your ML pipeline:</b></a> multinode pipelines, distributed computing, log during or after execution, log offline, and sync when you are back online.
&nbsp;

&nbsp;
<div align="center">
      <img border="0" alt="all metadata metrics" src="https://raw.githubusercontent.com/neptune-ai/neptune-client/assets/readme/all-metadata-metrics.gif" width="600">
    </a>
</div>
&nbsp;

&nbsp;

**Organize experiments**

Organize logs in a fully customizable nested structure. Display model metadata in user-defined dashboard templates.

* <a href="https://app.neptune.ai/o/showcase/org/example-project-tensorflow-keras/experiments?split=tbl&dash=charts&viewId=4a21e7e9-abab-4f9f-8723-41f5ae4f71e8&product_tour_id=314370"><b>Nested metadata structure:</b></a> flexible API lets you customize the metadata logging structure however you want. Talk to a dictionary at the code level. See the folder structure in the app. Organize nested parameter configs or the results on k-fold validation splits the way they should be.

* <a href="https://app.neptune.ai/o/showcase/org/example-project-tensorflow-keras/experiments?split=tbl&dash=charts&viewId=4a21e7e9-abab-4f9f-8723-41f5ae4f71e8&product_tour_id=314370"><b>Custom dashboards:</b></a> combine different metadata types in one view. Define it for one run. Use anywhere. Look at GPU, memory consumption, and load times to debug training speed. See learning curves, image predictions, and confusion matrix to debug model quality.

* <a href="https://app.neptune.ai/o/showcase/org/example-project-tensorflow-keras/experiments?split=tbl&dash=charts&viewId=4a21e7e9-abab-4f9f-8723-41f5ae4f71e8&product_tour_id=314370"><b>Table views:</b></a> create different views of the runs table and save them for later. You can have separate table views for debugging, comparing parameter sets, or best experiments.
&nbsp;

&nbsp;
<div align="center">
      <img border="0" alt="organize dashboards" src="https://raw.githubusercontent.com/neptune-ai/neptune-client/assets/readme/organize-dashboards.gif" width="600">
    </a>
</div>
&nbsp;

&nbsp;

**Compare results**

Visualize training live in the neptune.ai web app. See how different parameters and configs affect the results. Optimize models quicker.

* <a href="https://app.neptune.ai/o/showcase/org/example-project-tensorflow-keras/experiments?split=tbl&dash=charts&viewId=4a21e7e9-abab-4f9f-8723-41f5ae4f71e8&product_tour_id=314370"><b>Compare:</b></a> learning curves, parameters, images, datasets.

* <a href="https://app.neptune.ai/o/showcase/org/example-project-tensorflow-keras/experiments?split=tbl&dash=charts&viewId=4a21e7e9-abab-4f9f-8723-41f5ae4f71e8&product_tour_id=314370"><b>Search, sort, and filter:</b></a> experiments by any field you logged. Use our query language to filter runs based on parameter values, metrics, execution times, or anything else.

* <a href="https://app.neptune.ai/o/showcase/org/example-project-tensorflow-keras/experiments?split=tbl&dash=charts&viewId=4a21e7e9-abab-4f9f-8723-41f5ae4f71e8&product_tour_id=314370"><b>Visualize and display:</b></a> runs table, interactive display, folder structure, dashboards.

* <a href="https://app.neptune.ai/o/showcase/org/example-project-tensorflow-keras/experiments?split=tbl&dash=charts&viewId=4a21e7e9-abab-4f9f-8723-41f5ae4f71e8&product_tour_id=314370"><b>Monitor live:</b></a> hardware consumption metrics, GPU, CPU, memory.

* <a href="https://app.neptune.ai/o/showcase/org/example-project-tensorflow-keras/experiments?split=tbl&dash=charts&viewId=4a21e7e9-abab-4f9f-8723-41f5ae4f71e8&product_tour_id=314370"><b>Group by:</b></a> dataset versions, parameters.
&nbsp;

&nbsp;
<div align="center">
      <img border="0" alt="compare, search, filter" src="https://raw.githubusercontent.com/neptune-ai/neptune-client/assets/readme/compare-search-filter.gif" width="600">
    </a>
</div>
&nbsp;

&nbsp;

**Register models**

Version, review, and access production-ready models and metadata associated with them in a single place.

* <a href="https://docs.neptune.ai/model_registry/registering_model/"><b>Version models:</b></a> register models, create model versions, version external model artifacts.

* <a href="https://docs.neptune.ai/model_registry/managing_stage/"><b>Review and change stages:</b></a> look at the validation, test metrics and other model metadata. You can move models between None/Staging/Production/Archived.

* <a href="https://docs.neptune.ai/model_registry/overview/"><b>Access and share models:</b></a> every model and model version is accessible via the neptune.ai web app or through the API.
&nbsp;

&nbsp;
<div align="center">
      <img border="0" alt="register stages" src="https://raw.githubusercontent.com/neptune-ai/neptune-client/assets/readme/register-stages.gif" width="600">
    </a>
</div>
&nbsp;

&nbsp;

**Share results**

Have a single place where your team can see the results and access all models and experiments.

* <a href="https://docs.neptune.ai/about/collaboration/"><b>Send a link:</b></a> share every chart, dashboard, table view, or anything else you see in the neptune.ai app by copying and sending persistent URLs.

* <a href="https://docs.neptune.ai/usage/querying_metadata/"><b>Query API:</b></a> access all model metadata via neptune.ai API. Whatever you logged, you can query in a similar way.

* <a href="https://neptune.ai/pricing"><b>Manage users and projects:</b></a> create different projects, add users to them, and grant different permissions levels.

* <a href="https://neptune.ai/pricing"><b>Add your entire org:</b></a> get unlimited users on every paid plan. So you can invite your entire organization, including product managers and subject matter experts at no extra cost.
&nbsp;

&nbsp;
<div align="center">
      <img border="0" alt="share persistent link" src="https://raw.githubusercontent.com/neptune-ai/neptune-client/assets/readme/share-persistent-link.gif" width="600">
    </a>
</div>
&nbsp;

&nbsp;
## Integrate with any MLOps stack
neptune.ai integrates with <a href="https://docs.neptune.ai/integrations/"><b>25+ frameworks:</b></a> PyTorch, PyTorch Lightning, TensorFlow/Keras, LightGBM, scikit-learn, XGBoost, Optuna, Kedro, 🤗 Transformers, fastai, Prophet, and more.

#### <img src="https://raw.githubusercontent.com/neptune-ai/neptune-client/assets/readme/Pytorch-lightning-logo.png" width="60" /> <br> <br> PyTorch Lightning

Example:

```python
from pytorch_lightning import Trainer
from pytorch_lightning.loggers import NeptuneLogger

# Create NeptuneLogger instance
from neptune.new import ANONYMOUS_API_TOKEN
neptune_logger = NeptuneLogger(
    api_key=ANONYMOUS_API_TOKEN,
    project="common/pytorch-lightning-integration",
    tags=["training", "resnet"],  # optional
)

# Pass the logger to the Trainer
trainer = Trainer(max_epochs=10, logger=neptune_logger)

# Run the Trainer
trainer.fit(my_model, my_dataloader)
```

[![neptune-pl](https://img.shields.io/badge/PytorchLightning-experiment-success?logo=data:image/png;base64,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)](https://app.neptune.ai/common/pytorch-lightning-integration/experiments?split=tbl&dash=charts&viewId=faa75e77-5bd6-42b9-9379-863fe7a33227)
&nbsp;

[![github-code](https://img.shields.io/badge/GitHub-code-informational?logo=github)](https://github.com/neptune-ai/examples/tree/main/integrations-and-supported-tools/pytorch-lightning/scripts)
[![jupyter-code](https://img.shields.io/badge/Jupyter-code-informational?logo=jupyter)](https://github.com/neptune-ai/examples/blob/main/integrations-and-supported-tools/pytorch-lightning/notebooks/Neptune_PyTorch_Lightning.ipynb)
[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/neptune-ai/examples/blob/main/integrations-and-supported-tools/pytorch-lightning/notebooks/Neptune_PyTorch_Lightning.ipynb)
[<img src="https://img.shields.io/badge/docs-PyTorch%20Lightning-yellow">](https://docs.neptune.ai/integrations/lightning/)
&nbsp;

&nbsp;
## neptune.ai is trusted by great companies
&nbsp;

<div align="center">
    <img src="https://raw.githubusercontent.com/neptune-ai/neptune-client/assets/readme/github-customers.png" width="1500" />
</div>
&nbsp;

Read how various customers use Neptune to <a href="https://neptune.ai/customers">improve their workflow</a>.
&nbsp;

&nbsp;
## Support

If you get stuck or simply want to talk to us about something, here are your options:
* Check our <a href="https://docs.neptune.ai/getting_help/#frequently-asked-questions">FAQ page</a>.
* Take a look at our <a href="https://neptune.ai/resources">resource center</a>.
* Chat! In the app, click the <a href="https://docs.neptune.ai/getting_help/#chat">blue message icon</a> in the bottom-right corner and send a message. A real person will talk to you ASAP (typically very ASAP).
* You can just shoot us an email at [support@neptune.ai](mailto:support@neptune.ai).
&nbsp;

&nbsp;
## People behind

Created with :heart: by the [neptune.ai team](https://neptune.ai/about-us):

Piotr, Valentina, Paulina, Chaz, Prince, Parth, Kshitij, Siddhant, Jakub, Patrycja, Dominika, Karolina, Stephen, Artur, Aleksiej, Martyna, Małgorzata, Magdalena, Karolina, Marcin, Michał, Tymoteusz, Rafał, Aleksandra, Sabine, Tomek, Piotr, Adam, Jakub, Rafał, Piotr, Hubert, Marcin, Jakub, Paweł, Jakub, Franciszek, Bartosz, Aleksander, Dawid, Pavel, Patryk, and [you?](https://neptune.ai/jobs)
