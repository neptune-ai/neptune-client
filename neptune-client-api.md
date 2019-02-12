

```python
import neptune
```


```python
project = neptune.init("wajcha/sandbox", api_token="fQo=")
```


```python
exp = neptune.create_experiment(
    params={
    "lr__C": 1000,
    "epochs": 10,
    "shuffle": 1
},
    properties={"prop1": "val1"},
    tags=["solution-6", "stacking"],
    upload_source_files=["neptune-client-api.ipynb"]
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
exp.send_artifact("/tmp/model.file")
```


```python
exp.stop() # end experiment, state = successful
```


```python
exp.stop(ex) # ex or exit code not 0, state = failed
```
