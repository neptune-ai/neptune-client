# neptune-client
[![PyPI version](https://badge.fury.io/py/neptune-client.svg)](https://badge.fury.io/py/neptune-client)
[![Build Status](https://travis-ci.org/neptune-ai/neptune-client.svg?branch=master)](https://travis-ci.org/neptune-ai/neptune-client)

# Overview

Neptune is an experiment tracking hub that brings organization and collaboration to your data science team. 

It works with any:
* infrastructure setup
* framework
* working style

**Keep the knowledge in one place, organized and ready to be shared with anyone.**

![image](https://gist.githubusercontent.com/jakubczakon/f754769a39ea6b8fa9728ede49b9165c/raw/2f3a5577ac55595e8b9241d81a2de43a0fc663db/wiki.png)
![image](https://gist.githubusercontent.com/jakubczakon/f754769a39ea6b8fa9728ede49b9165c/raw/2a67f6ed1017d3f6a3dec6fe85d1727f3b41f533/neptune_quick_start.png)
![image](https://gist.githubusercontent.com/jakubczakon/f754769a39ea6b8fa9728ede49b9165c/raw/8aa4f35e29a2a5177e89a8ed5d1daa233b04b0b9/clf_report.png)
![image](https://gist.githubusercontent.com/jakubczakon/f754769a39ea6b8fa9728ede49b9165c/raw/8aa4f35e29a2a5177e89a8ed5d1daa233b04b0b9/ship_predictions.png)

# Getting started

### Register
Go to https://neptune.ai/ and sign up.

It is completely free for individuals and non-organizations, and you can invite others to join your team!

### Get your API token
In order to start working with Neptune you need to get the API token first.
To do that, click on the `Get API Token` button on the top left.

![image](https://gist.githubusercontent.com/jakubczakon/f754769a39ea6b8fa9728ede49b9165c/raw/e3776e605fea1fd5377c3ec748ba87b71cd8ef12/get_api_token.png)


### Create your first project
Click on `Projects` and the `New project`. Choose a name for it and whether you want it public or private.

![image](https://gist.githubusercontent.com/jakubczakon/f754769a39ea6b8fa9728ede49b9165c/raw/e3776e605fea1fd5377c3ec748ba87b71cd8ef12/new_project.png)


### Invite others
Go to your project, click `Settings` and send invites!

![image](https://gist.githubusercontent.com/jakubczakon/f754769a39ea6b8fa9728ede49b9165c/raw/e3776e605fea1fd5377c3ec748ba87b71cd8ef12/invite.png)

### Start tracking your work
Neptune let's you track any information important to your experimentation process.

#### Install Neptune client
Just run:

```bash
pip install neptune-client
```

#### Initialize Neptune
Toward the top of your script insert the following snippet.

```python
import neptune

neptune.init(api_token='YOUR_API_TOKEN',
             project_qualified_name='USERNAME/PROJECT_NAME')
```

#### Create and stop the experiment
You can treat every piece of work that you want to record as an experiment.
Just create an experiment:

```python
neptune.create_experiment()
```
Do whatever you want and record it here!
Stop the experiment.

```python
neptune.stop()
```

#### Track hyperparameters
Making sure that all your hyperparameters are recorded is very important.
With Neptune, you can do that easily by passing `params` dictionary when creating the experiment.

```python
params = {'n_estimators':10,
          'criterion': 'gini',
          'max_depth': 2,
          'min_samples_split': 100}

neptune.create_experiment(params=params)

```

#### Track metrics
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

#### Track result diagnostics
You can even log images to Neptune. Just save to the 

```python
plot_roc(y_test, y_test_pred)
plt.savefig('roc_curve.png') 
neptune.send_image('roc_curve', 'roc_curve.png')
```

#### Track artifacts
You can save model weights and any other artifact that you created during your experiment.

```python
from sklearn.externals import joblib
joblib.dump(clf, 'rf_model.pkl')
neptune.send_artifact('rf_model.pkl')
```

#### Track data versions

```python
from hashlib import sha1

data_version = sha1(X).hexdigest()
neptune.send_text('data_version', data_version)
```

#### Track code
You can track your codebase too.
Just choose the files that you want to send to Neptune.

```python

neptune.create_experiment(upload_source_files=['utils.py', 
                                               'main.py'])
```

**[Check the example project here](https://ui.neptune.ai/jakub-czakon/quick-start/experiments)**


# Getting help
If you get stuck, don't worry we are here to help.
The best order of communication is:

 * [neptune documentation](https://docs.neptune.ai)
 * [github issues](https://github.com/neptune-ai/neptune-client/issues)
 * [neptune community forum](https://community.neptune.ai/)
 * [neptune community spectrum](https://spectrum.chat/neptune-community?tab=posts)
