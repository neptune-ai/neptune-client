<div align="center">
    <img src="https://raw.githubusercontent.com/neptune-ai/neptune-client/assets/readme/Github-cover.png" width="1500" />
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
  <a href="https://neptune.ai/blog">Blog</a>
&nbsp;
  <hr />
</div>

## What is neptune.ai?

Neptune is an experiment tracker purpose-built for foundation model training.<br>
<br>
With Neptune, you can monitor thousands of per-layer metrics—losses, gradients, and activations—at any scale. Visualize them with no lag and no missed spikes. Drill down into logs and debug training issues fast. Keep your model training stable while reducing wasted GPU cycles.<br>

<a href="https://youtu.be/0J4dsEq8i08"><b>Watch a 3min explainer video →</b></a>
&nbsp;

<a href="https://scale.neptune.ai/o/neptune/org/LLM-training-example/runs/compare?viewId=9d0e032a-5a78-4a0e-81d1-98e0a7c81a8f&detailsTab=metadata&dash=charts&type=run&experimentOnly=true&compare=u0MsW4a1PJIUJ75nglpjHa9XUKFfAmcBRbLhNatCHX20"><b>Play with a live example project in the Neptune app  →</b></a>
&nbsp;
## Getting started

**Step 1:** Create a **[free account](https://neptune.ai/register)**

**Step 2:** Install the Neptune client library

```bash
pip install neptune
```

**Step 3:** Add an experiment tracking snippet to your code

```python
import neptune

run = neptune.init_run(project="workspace-name/project-name")
run["parameters"] = {"lr": 0.1, "dropout": 0.4}
run["test_accuracy"] = 0.84
```

[![Open in Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/neptune-ai/examples/blob/master/how-to-guides/hello-neptune/notebooks/hello_neptune.ipynb)
&nbsp;

&nbsp;
## Integrate with any MLOps stack
neptune.ai integrates with <a href="https://docs.neptune.ai/integrations/"><b>25+ frameworks:</b></a> PyTorch, Lightning, TensorFlow/Keras, LightGBM, scikit-learn, XGBoost, Optuna, Kedro, 🤗 Transformers, fastai, Prophet, detectron2, Airflow, and more.

#### <img src="https://raw.githubusercontent.com/neptune-ai/neptune-client/assets/readme/Pytorch-lightning-logo.png" width="60" /> <br> <br> PyTorch Lightning

Example:

```python
from pytorch_lightning import Trainer
from lightning.pytorch.loggers import NeptuneLogger

# Create NeptuneLogger instance
from neptune import ANONYMOUS_API_TOKEN

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
Read how various customers use Neptune to <a href="https://neptune.ai/customers">improve their workflow</a>.
&nbsp;
## Support

If you get stuck or simply want to talk to us about something, here are your options:
* Check our <a href="https://docs.neptune.ai/getting_help/#faq">FAQ page</a>.
* Chat! In the app, click the <a href="https://docs.neptune.ai/getting_help/#chat">blue message icon</a> in the bottom-right corner and send a message. A real person will talk to you ASAP (typically very ASAP).
* You can just shoot us an email at [support@neptune.ai](mailto:support@neptune.ai).
&nbsp;

&nbsp;
## People behind

Created with :heart: by the [neptune.ai team](https://neptune.ai/jobs#team)
