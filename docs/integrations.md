# Creating a new integration

Assuming that the new integration is called awesome-integration
1. Copy directory structure from an existing integration to the new repo.
2. Update `setup.py`, `setup.cfg` replacing `tensorflow-keras` with `awesome-integration`
3. [Update versioneer](https://github.com/python-versioneer/python-versioneer/blob/master/INSTALL.md)
4. Write your integration
4a. Remember to set the integration version as an attribute, e.g.

```
run['source_code/integrations/neptune-awesome-integration'] = __version__
```

5. Create the integration project once via manual upload
    ```shell script
    cd integrations/awesome-integration
    git tag awesome-integration/0.0.0
    python setup.py sdist
    cd dist
    twine upload -u XXXXXXX -p XXXXXXX awesome-integration-0.0.0.tar.gz
   ```
6. Create a pypi api token scoped to that project
7. Add this secret to `awesome-integration` repo secrets
9. Push the git tag
10. Remember to write forwarding code to neptune-client in `neptune/new/integrations/awsome-integration` and add it to `setup.py` to `extras_require`
