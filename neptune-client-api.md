

```python
from neptune.session import Session

session = Session("token=")
```


```python
project = session.get_project("neptune-ml/Ships")
```


```python
exp = project.create_experiment( # create experiment, with params and tags
    params={
    "lr__C": 1000,
    "epochs": 10,
    "shuffle": true
}, 
    tags=["solution-6", "stacking"],
    send_hardware_metrics=False, # default = True
    run_monitoring_thread=False, # default = True
    upload_sources_files=["main.py"], # default = this file
    handle_uncaught_exceptions=False # default = True
)
```


```python
exp.send_metric("ROC_AUC", 0.75)
exp.send_metric("ROC_AUC_STD", 0.002)
exp.send_text("log", "done")
exp.send_image("false", "/tmp/failed_class.png")
```


```python
exp.set_tags(exp.get_tags().append("new-tag")) # update tags
```


```python
exp.send_artifact("model.file")
```


```python
exp.stop() # end experiment, state = successful
```


```python
exp.stop(ex) # ex or exit code not 0, state = failed
```
