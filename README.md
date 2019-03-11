# neptune-client
[![PyPI version](https://badge.fury.io/py/neptune-client.svg)](https://badge.fury.io/py/neptune-client)
[![Build Status](https://travis-ci.org/neptune-ml/neptune-client.svg?branch=master)](https://travis-ci.org/neptune-ml/neptune-client)

## Overview

Neptune is a ...

It lets you track your work, organize it and share it with others.

Y



### Invite others
Go to your project, click `Settings` and send invites!

![image](https://gist.githubusercontent.com/jakubczakon/f754769a39ea6b8fa9728ede49b9165c/raw/e3776e605fea1fd5377c3ec748ba87b71cd8ef12/invite.png)


## Getting started

### Register
Go to https://neptune.ml/ and sign up.

It is completely free for individuals and non-organizations, and you can invite others to join your team!

### Get your API token
In order to start working with Neptune you need to get the API token first.
To do that, click on the `Get API Token` button on the top left.

![image](https://gist.githubusercontent.com/jakubczakon/f754769a39ea6b8fa9728ede49b9165c/raw/e3776e605fea1fd5377c3ec748ba87b71cd8ef12/get_api_token.png)


### Create your first project
Click on `Projects` and the `New project`. Choose a name for it and whether you want it public or private.

![image](https://gist.githubusercontent.com/jakubczakon/f754769a39ea6b8fa9728ede49b9165c/raw/e3776e605fea1fd5377c3ec748ba87b71cd8ef12/new_project.png)


### Start tracking your work

### Install Neptune client

```bash
pip install neptune-client
```

### Initialize Neptune

```python
import neptune

neptune.init(api_token='YOUR_API_TOKEN',
             project_qualified_name='USERNAME/PROJECT_NAME')
```

### Create and stop the experiment
You can treat every piece of work that you want to record as an experiment.
For example when training a model you would:

**step1**

Create an experiment.

```python
neptune.create_experiment()
```
**step2**
Train models, save hyperparameters, images, model weights.

**step3**
Stop the experiment.

```python
neptune.stop()
```

**Note**
You can get rid of the `neptune.stop()` by using the `with` statements.

```python
with neptune.create_experiment():
    ...
```

### Track hyperparameters
Making sure that all your hyperparameters are recorded is very important.
With Neptune, you can do that easily by passing `params` dictionary when creating the experiment.

```python
params = {'n_estimators':10,
          'criterion': 'gini',
          'max_depth': 2,
          'min_samples_split': 100}

neptune.create_experiment(params=params)

```

![image]()

### Track metrics
It is super easy. Just log your metric to Neptune.

```python
neptune.send_metric('roc_auc', 0.82)
```

In case you want to track your metric after every step (deep learning), you
can simply send your metric to the same channel after every step and Neptune will
automatically create a chart for you.

```python
for i in range(100):
     neptune.send_metric('learning_rate_schedule', 0.01 *1.05 ** i) 
```

### Track result diagnostics
You can even log images to Neptune. Just save to the 

```python
plot_roc(y_test, y_test_pred)
plt.savefig('roc_curve.png') 
neptune.send_image('roc_curve', 'roc_curve.png')
```

![image]()

### Track artifacts
You can save model weights and any other artifact that you created during your experiment.

```python
from sklearn.externals import joblib
joblib.dump(clf, 'rf_model.pkl')
neptune.send_artifact('rf_model.pkl')
```

### Track data versions

```python
from hashlib import sha1

data_version = sha1(X).hexdigest()
neptune.send_text('data_version', data_version)
```

### Track code


You can go and check the example project [here]().
