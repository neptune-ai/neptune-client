## [UNRELEASED] neptune 1.10.0

### Features
- Added `get_workspace_status()` method to management API ([#1662](https://github.com/neptune-ai/neptune-client/pull/1662))
- Added auto-scaling pixel values for image logging ([#1664](https://github.com/neptune-ai/neptune-client/pull/1664))
- Introduce querying capabilities to `fetch_runs_table()` ([#1660](https://github.com/neptune-ai/neptune-client/pull/1660))
- Introduce querying capabilities to `fetch_models_table()` ([#1677](https://github.com/neptune-ai/neptune-client/pull/1677))

### Fixes
- Restored support for SSL verification exception ([#1661](https://github.com/neptune-ai/neptune-client/pull/1661))
- Allow user to control logging level ([#1679](https://github.com/neptune-ai/neptune-client/pull/1679))

### Changes
- Improve dependency installation checking ([#1670](https://github.com/neptune-ai/neptune-client/pull/1670))
- Cache dependencies check ([#1675](https://github.com/neptune-ai/neptune-client/pull/1675))


## neptune 1.9.1

### Fixes
- Fixed conda package ([#1652](https://github.com/neptune-ai/neptune-client/pull/1652))
- Resource cleaning in PyTorch Dataloaders with multiple workers ([issue](https://github.com/neptune-ai/neptune-client/issues/1622)) ([#1649](https://github.com/neptune-ai/neptune-client/pull/1649))

### Changes
- Lazy initialization of operation processor when forking ([#1649](https://github.com/neptune-ai/neptune-client/pull/1649))


## neptune 1.9.0

### Features
- Add support for seaborn figures ([#1613](https://github.com/neptune-ai/neptune-client/pull/1613))
- Added fetching with iterators in `fetch_*_table()` methods ([#1585](https://github.com/neptune-ai/neptune-client/pull/1585))
- Added `limit` parameter to `fetch_*_table()` methods ([#1593](https://github.com/neptune-ai/neptune-client/pull/1593))
- Added `sort_by` parameter to `fetch_*_table()` methods ([#1595](https://github.com/neptune-ai/neptune-client/pull/1595))
- Added `ascending` parameter to `fetch_*_table()` methods ([#1602](https://github.com/neptune-ai/neptune-client/pull/1602))
- Added `progress_bar` parameter to `fetch_*_table()` methods ([#1599](https://github.com/neptune-ai/neptune-client/pull/1599))
- Added `progress_bar` parameter to `download()` method of the `Handler` class ([#1620](https://github.com/neptune-ai/neptune-client/pull/1620))
- Added `progress_bar` parameter to `fetch_values()` method of the `Handler` class ([#1633](https://github.com/neptune-ai/neptune-client/pull/1633))

### Fixes
- Add direct requirement of `typing-extensions` ([#1586](https://github.com/neptune-ai/neptune-client/pull/1586))
- Handle `None` values in distribution sorting in `InferDependeciesStrategy` ([#1612](https://github.com/neptune-ai/neptune-client/pull/1612))
- Fixed race conditions with cleaning internal files ([#1606](https://github.com/neptune-ai/neptune-client/pull/1606))
- Better value validation for `state` parameter of `fetch_*_table()` methods ([#1616](https://github.com/neptune-ai/neptune-client/pull/1616))
- Parse `datetime` attribute values in `fetch_runs_table()` ([#1634](https://github.com/neptune-ai/neptune-client/pull/1634))
- Better handle limit in `fetch_*_table()` methods ([#1644](https://github.com/neptune-ai/neptune-client/pull/1644))
- Fix pagination handling in table fetching ([#1651](https://github.com/neptune-ai/neptune-client/pull/1651))

### Changes
- Use literals instead of str for Mode typing ([#1586](https://github.com/neptune-ai/neptune-client/pull/1586))
- Flag added for cleaning internal data ([#1589](https://github.com/neptune-ai/neptune-client/pull/1589))
- Handle logging in the `AsyncOperationProcessor` with `OperationLogger` and signal queue ([#1610](https://github.com/neptune-ai/neptune-client/pull/1610))
- Stringify `Handler` paths ([#1623](https://github.com/neptune-ai/neptune-client/pull/1623))
- Added processor id to `ProcessorStopSignalData` ([#1625](https://github.com/neptune-ai/neptune-client/pull/1625))
- Use the same logger instance for logging ([#1611](https://github.com/neptune-ai/neptune-client/pull/1611))
- Changed offline directories internal path structure ([#1606](https://github.com/neptune-ai/neptune-client/pull/1606))
- Changed internal directories path structure ([#1606](https://github.com/neptune-ai/neptune-client/pull/1606))
- Changed format of warning messages ([#1635](https://github.com/neptune-ai/neptune-client/pull/1635))
- Make `trash_objects()` raise `ProjectNotFound` if project does not exist ([#1636](https://github.com/neptune-ai/neptune-client/pull/1636))
- Do not show progress bars when no data to fetch / small amount of data ([#1638](https://github.com/neptune-ai/neptune-client/pull/1638))


## 1.8.6

### Fixes
- Support for more than 10k entries when using `fetch_*_table()` methods ([#1576](https://github.com/neptune-ai/neptune-client/pull/1576))
- Docstrings visibility for Neptune objects ([#1580](https://github.com/neptune-ai/neptune-client/pull/1580))

### Changes
- Improved performance of `fetch_*_table()` methods up to 2x ([#1573])(https://github.com/neptune-ai/neptune-client/pull/1573)
- Adjusted NeptuneLimitExceedException message ([#1574](https://github.com/neptune-ai/neptune-client/pull/1574))
- Do not create monitoring namespace if all relevant flags are set to False ([#1575](https://github.com/neptune-ai/neptune-client/pull/1575))
- Updated README ([#1577](https://github.com/neptune-ai/neptune-client/pull/1577))


## neptune 1.8.5

### Fixes
- Fixed no synchronization callbacks behaviour ([#1567](https://github.com/neptune-ai/neptune-client/pull/1567))

### Changes
- Enabled hooks for internal downloading functions used by the hosted backend ([#1571](https://github.com/neptune-ai/neptune-client/pull/1571))
- Added timestamp of operation put to disk queue ([#1569](https://github.com/neptune-ai/neptune-client/pull/1569))


## neptune 1.8.4

### Changes
- Moved `prepare_nql_query` to a separate function ([#1568](https://github.com/neptune-ai/neptune-client/pull/1568))


## neptune 1.8.3

### Fixes
- Added more safe checking to last ack ([#1510](https://github.com/neptune-ai/neptune-client/pull/1510))
- Retry request in case of bravado RecursiveCallException ([#1521](https://github.com/neptune-ai/neptune-client/pull/1512))
- Fix bug in git tracking when repo was clean ([#1517](https://github.com/neptune-ai/neptune-client/pull/1517))
- Run async callback in new daemon thread ([#1521](https://github.com/neptune-ai/neptune-client/pull/1521))
- Better handle bool values of `git_ref` param in `init_run` ([#1525](https://github.com/neptune-ai/neptune-client/pull/1525))
- Updated management docstrings ([#1500](https://github.com/neptune-ai/neptune-client/pull/1500))
- Fix error message in case of NeptuneAuthTokenExpired ([#1531](https://github.com/neptune-ai/neptune-client/pull/1531))
- Updated NeptuneModelKeyAlreadyExistsError exception message ([#1536](https://github.com/neptune-ai/neptune-client/pull/1536))
- Added support for unsupported float values in `stringify_unsupported()` ([#1543](https://github.com/neptune-ai/neptune-client/pull/1543))
- Clarified message shown when nonexistent ID is passed to `with_id` argument ([#1551](https://github.com/neptune-ai/neptune-client/pull/1551))

### Changes
- Allow to disable deletion of local parent folder ([#1511](https://github.com/neptune-ai/neptune-client/pull/1511))
- Made the disk checking more reliable for env specific errors ([#1519](https://github.com/neptune-ai/neptune-client/pull/1519))
- Update Neptune object docstrings ([#1516](https://github.com/neptune-ai/neptune-client/pull/1516))
- Added metadata file that stores information about internal directory structure and platform ([#1526](https://github.com/neptune-ai/neptune-client/pull/1526))
- Minor tweaks to `neptune.cli` and cleaning leftovers after async Experiments ([#1529](https://github.com/neptune-ai/neptune-client/pull/1529))
- Added support for plugins/extensions ([#1545](https://github.com/neptune-ai/neptune-client/pull/1545))
- Skip and warn about unsupported float values (infinity, negative infinity, NaN) in logging floats ([#1542](https://github.com/neptune-ai/neptune-client/pull/1542))
- Move error handling to a separate method in `AsyncOperationProcessor` ([#1553](https://github.com/neptune-ai/neptune-client/pull/1553))
- Abstract parts of logic to separate methods for AsyncOperationProcessor ([#1557](https://github.com/neptune-ai/neptune-client/pull/1557))
- Rework disk utilization check ([#1549](https://github.com/neptune-ai/neptune-client/pull/1549))
- Introduce error handlers for disk utilization ([#1559](https://github.com/neptune-ai/neptune-client/pull/1559))
- Added support for `neptune[experimental]` extras ([#1560](https://github.com/neptune-ai/neptune-client/pull/1560))
- Disk utilization environment variables renamed ([#1565](https://github.com/neptune-ai/neptune-client/pull/1565))


## neptune 1.8.2

### Changes
- Support for disabling operation saving based on disk utilization ([#1496](https://github.com/neptune-ai/neptune-client/pull/1496))


## neptune 1.8.1

### Fixes
- Fixed SSL-related error handling ([#1490](https://github.com/neptune-ai/neptune-client/pull/1490))


## neptune 1.8.0

### Features
- Programmatically delete trashed neptune objects ([#1475](https://github.com/neptune-ai/neptune-client/pull/1475))
- Added support for callbacks that stop the synchronization if the lag or lack of progress exceeds a certain threshold ([#1478](https://github.com/neptune-ai/neptune-client/pull/1478))

### Changes
- Add support for `retry-after` header in HTTPTooManyRequests ([#1477](https://github.com/neptune-ai/neptune-client/pull/1477))
- Bump boto3 required version to speed up installation via poetry ([#1481](https://github.com/neptune-ai/neptune-client/pull/1481))

### Fixes
- Add newline at the end of generated `.patch` while tracking uncommitted changes ([#1473](https://github.com/neptune-ai/neptune-client/pull/1473))
- Clarify `NeptuneLimitExceedException` error message ([#1480](https://github.com/neptune-ai/neptune-client/pull/1480))


## neptune 1.7.0

### Features
- Added support for `airflow` integration ([#1466](https://github.com/neptune-ai/neptune-client/pull/1466))

### Changes
- Add handling of project limits ([#1456](https://github.com/neptune-ai/neptune-client/pull/1456))
- Language and style improvements ([#1465](https://github.com/neptune-ai/neptune-client/pull/1465))

### Fixes
- Fix exception handling in `ApiMethodWrapper.handle_neptune_http_errors` ([#1469](https://github.com/neptune-ai/neptune-client/pull/1469))
- Fix race condition between close and flush in disk queue ([#1470](https://github.com/neptune-ai/neptune-client/pull/1470))


## neptune 1.6.3

### Changes
- Expose metadata container state via a getter method ([#1463](https://github.com/neptune-ai/neptune-client/pull/1463))


## neptune 1.6.2

### Changes
- Identify client's artifact supported version by adding `X-Neptune-Artifact-Api-Version` header to get artifact attribute request ([#1436](https://github.com/neptune-ai/neptune-client/pull/1436))
- Import `JSONDecodeError` from `simplejson` instead of `requests` [#1451](https://github.com/neptune-ai/neptune-client/pull/1451)

### Fixes
- Cast integers outside 32 bits to float in `stringify_unsupported()` ([#1443](https://github.com/neptune-ai/neptune-client/pull/1443))


## neptune 1.6.1

### Fixes
- Fixed conda package due to improper non-required backoff requirement ([#1435](https://github.com/neptune-ai/neptune-client/pull/1435))

## neptune 1.6.0

### Features
- Added `list_fileset_files()` method to list files and directories contained in `FileSet` field ([#1412](https://github.com/neptune-ai/neptune-client/pull/1412))

### Fixes
- Fixed `stringify_unsupported` not catching a broader `MutableMapping` class ([#1427](https://github.com/neptune-ai/neptune-client/pull/1427))
- Cast keys in the resulting dictionary to string in `stringify_unsupported` ([#1427](https://github.com/neptune-ai/neptune-client/pull/1427))
- Fixed an issue where data was sometimes not uploaded in case the initial request to the Neptune servers failed ([#1429](https://github.com/neptune-ai/neptune-client/pull/1429))

## neptune 1.5.0

### Features
- Users can pass neptune data directory path by env variable ([#1409](https://github.com/neptune-ai/neptune-client/pull/1409))
- Filter S3 empty files and exclude metadata from computing of file's hash for new version of artifacts ([#1421](https://github.com/neptune-ai/neptune-client/pull/1421))

### Fixes
- Load CLI plug-ins in try..except block to avoid a failure in loading a plug-in to crash entire CLI ([#1392](https://github.com/neptune-ai/neptune-client/pull/1392))
- Fixed cleaning operation storage when using `sync` mode and forking ([#1413](https://github.com/neptune-ai/neptune-client/pull/1413))
- Fix FileDependenciesStrategy when the dependency file is in a folder ([#1411](https://github.com/neptune-ai/neptune-client/pull/1411))
- Fixed cleaning operation storage when using `async` mode and forking ([#1418](https://github.com/neptune-ai/neptune-client/pull/1418))

### Changes
- Allow disabling Git tracking by passing `git_ref=False` ([#1423](https://github.com/neptune-ai/neptune-client/pull/1423))


##  neptune 1.4.1

### Fixes
- Retry request when ChunkedEncodingError occurred. ([#1402](https://github.com/neptune-ai/neptune-client/pull/1402))
- Fixed performance issues on forking process  ([#1407](https://github.com/neptune-ai/neptune-client/pull/1407))


##  neptune 1.4.0

### Fixes
- Fixed operation processor bug if current working directory is different from the script directory ([#1391](https://github.com/neptune-ai/neptune-client/pull/1391))

### Features
- Added support for `tensorboard` integration ([#1368](https://github.com/neptune-ai/neptune-client/pull/1368))
- Added support for `mlflow` integration ([#1381](https://github.com/neptune-ai/neptune-client/pull/1381))


##  neptune 1.3.3rc0

### Changes
- Dependency tracking feature will log an error if a given file path doesn't exist ([#1389](https://github.com/neptune-ai/neptune-client/pull/1389))
- Use `importlib` instead of `pip freeze` in dependency tracking ([#1389](https://github.com/neptune-ai/neptune-client/pull/1389))
- Log both uploaded and inferred requirements to the same namespace ([#1389](https://github.com/neptune-ai/neptune-client/pull/1389))

### Fixes
- Fixed operation processor bug if current working directory is different from the script directory ([#1391](https://github.com/neptune-ai/neptune-client/pull/1391))
- Tracking uncommitted changes and dependencies will be skipped in case of any exception, to not disturb the run initialization ([#1395](https://github.com/neptune-ai/neptune-client/pull/1395))

## neptune 1.3.2

### Fixes
- Fixed GitPython `is_dirty` failing on Windows ([#1371](https://github.com/neptune-ai/neptune-client/pull/1371))
- Fix SSL errors after forking process ([#1353](https://github.com/neptune-ai/neptune-client/pull/1353))
- Fixed support of stringify value in series attributes with step ([#1373](https://github.com/neptune-ai/neptune-client/pull/1373))
- `dict`s and `Namespace`s that are written to runs and contain an empty string "" key now produce a warning and drop
  the entry with such a key instead of raising an
  exception ([#1374](https://github.com/neptune-ai/neptune-client/pull/1374))
- Fix dependency tracking by replacing `pipreqs` with `pip freeze` ([#1384](https://github.com/neptune-ai/neptune-client/pull/1384))

### Changes
- Added support of writing to archived project exception ([#1355](https://github.com/neptune-ai/neptune-client/pull/1355))

## neptune 1.3.1

### Fixes
- Fix ImportError when git is missing ([#1359](https://github.com/neptune-ai/neptune-client/pull/1359))

## neptune 1.3.0

### Features
- Added automatic tracking of dependencies ([#1345](https://github.com/neptune-ai/neptune-client/pull/1345))
- Added automatic tracking of uncommitted changes ([#1350]https://github.com/neptune-ai/neptune-client/pull/1350)

### Fixes
- Added support of project visibility exception ([#1343](https://github.com/neptune-ai/neptune-client/pull/1343))

### Changes
- Added support of active projects limit exception ([#1348](https://github.com/neptune-ai/neptune-client/pull/1348))

## neptune 1.2.0

### Changes
- Neptune objects and universal methods covered with docstrings ([#1309](https://github.com/neptune-ai/neptune-client/pull/1309))
- Added docstrings for Neptune packages and modules ([#1332](https://github.com/neptune-ai/neptune-client/pull/1332))

### Features
- Series objects accept `timestamps` and `steps` in their constructors ([#1318](https://github.com/neptune-ai/neptune-client/pull/1318))
- Users can be invited to the workspace with `management` api ([#1333](https://github.com/neptune-ai/neptune-client/pull/1333))
- Added support for `pytorch` integration ([#1337](https://github.com/neptune-ai/neptune-client/pull/1337))

### Fixes
- Print warning instead of crashing syncing thread when logging big integers ([#1336](https://github.com/neptune-ai/neptune-client/pull/1336))

## neptune 1.1.1

### Fixes
- Fixed handling errors in case of too long filenames provided with `sys.argv` ([#1305](https://github.com/neptune-ai/neptune-client/pull/1305))

## neptune 1.1.0

### Features
- Added ability to provide repository path with `GitRef` to `init_run` ([#1292](https://github.com/neptune-ai/neptune-client/pull/1292))
- Added `SupportsNamespaces` interface in `neptune.typing` for proper type annotations of Handler and Neptune objects ([#1280](https://github.com/neptune-ai/neptune-client/pull/1280))
- Added `NEPTUNE_SYNC_AFTER_STOP_TIMEOUT` environment variable ([#1260](https://github.com/neptune-ai/neptune-client/pull/1260))
- `Run`, `Model`, `ModelVersion` and `Project` could be created with constructor in addition to `init_*` functions ([#1246](https://github.com/neptune-ai/neptune-client/pull/1246))

### Fixes
- Setting request timeout to 10 minutes instead of infinite ([#1295](https://github.com/neptune-ai/neptune-client/pull/1295))

## neptune 1.0.2

### Fixes
- Properly handle expired oauth token ([#1271](https://github.com/neptune-ai/neptune-client/pull/1271))

## neptune 1.0.1

### Fixes
- Fixed `neptune-client` package setup ([#1263](https://github.com/neptune-ai/neptune-client/pull/1263))

## neptune 1.0.1rc0

### Fixes
- Fixed `neptune-client` package setup ([#1263](https://github.com/neptune-ai/neptune-client/pull/1263))

## neptune 1.0.0

### Changes
- Disabled automatic casting to strings for unsupported by Neptune types ([#1215](https://github.com/neptune-ai/neptune-client/pull/1215))
- Moved modules from `neptune.new` to `neptune` with compatibility imports and marked `neptune.new` as deprecated ([#1213](https://github.com/neptune-ai/neptune-client/pull/1213))
- Removed `neptune.*` legacy modules ([#1206](https://github.com/neptune-ai/neptune-client/pull/1206))
- Removed `get_project` function ([#1214](https://github.com/neptune-ai/neptune-client/pull/1214))
- Removed `init` function ([#1216](https://github.com/neptune-ai/neptune-client/pull/1216))
- Removed `get_last_run` function ([#1217](https://github.com/neptune-ai/neptune-client/pull/1217))
- Removed `run` parameter from `init_run` function ([#1218](https://github.com/neptune-ai/neptune-client/pull/1218))
- Removed `model` parameter from `init_model` function ([#1223](https://github.com/neptune-ai/neptune-client/pull/1223))
- Removed `version` parameter from `init_model_version` function ([#1223](https://github.com/neptune-ai/neptune-client/pull/1223))
- Monitoring is off by default for interactive Python kernels ([#1219](https://github.com/neptune-ai/neptune-client/pull/1219))
- Removed `name` parameter from `init_project` function and `management` API ([#1227](https://github.com/neptune-ai/neptune-client/pull/1227))
- Monitoring namespace based on hostname, process id and thread id ([#1222](https://github.com/neptune-ai/neptune-client/pull/1222))
- Removed deprecated `--run` option from `neptune sync` command ([#1231](https://github.com/neptune-ai/neptune-client/pull/1231))
- Update methods to have mainly keyword arguments ([#1228](https://github.com/neptune-ai/neptune-client/pull/1228))
- Removed `Run._short_id` property ([#1234](https://github.com/neptune-ai/neptune-client/pull/1234))
- Removed `get_run_url` method ([#1238](https://github.com/neptune-ai/neptune-client/pull/1238))
- Removed `neptune.new.sync` module ([#1240](https://github.com/neptune-ai/neptune-client/pull/1240))
- Change run status in the table returned by `fetch_runs_table` to Active / Inactive ([#1233](https://github.com/neptune-ai/neptune-client/pull/1233))
- Package renamed from `neptune-client` to `neptune` ([#1225](https://github.com/neptune-ai/neptune-client/pull/1225))
- Changed values used to filter runs table by state ([#1253](https://github.com/neptune-ai/neptune-client/pull/1253))
- Added warning for unsupported types ([#1255](https://github.com/neptune-ai/neptune-client/pull/1255))

### Fixes
- Fixed input value type verification for `append()` method ([#1254](https://github.com/neptune-ai/neptune-client/pull/1254))

## neptune-client 0.16.18

### Fixes
- Fix handling connection errors when refreshing oauth token ([#1204](https://github.com/neptune-ai/neptune-client/pull/1204))
- Fix syncing offline runs with file upload ([#1211](https://github.com/neptune-ai/neptune-client/pull/1211))

## neptune-client 0.16.17

### Features
- Added support for `detectron2` integration ([#1190](https://github.com/neptune-ai/neptune-client/pull/1190))
- Make neptune-aws package installable as `pip install neptune[aws]`.  ([#1176](https://github.com/neptune-ai/neptune-client/pull/1176))

### Fixes
- Added support of tuple in stringify_unsupported ([#1196](https://github.com/neptune-ai/neptune-client/pull/1196))
- Fixed lack of `__repr__` for `StringifyValue` ([#1195](https://github.com/neptune-ai/neptune-client/pull/1195))

## neptune-client 0.16.16

### Features
- Added `stringify_unsupported` function for handling backward compatibility of implicit casting ([#1177](https://github.com/neptune-ai/neptune-client/pull/1177))
- Better support for `Handler` level objects ([#1178](https://github.com/neptune-ai/neptune-client/pull/1178))

### Changes
- Docstrings and deprecation messages updated ([#1182](https://github.com/neptune-ai/neptune-client/pull/1182))
- Deprecate name parameter in init_project and management API ([#1175](https://github.com/neptune-ai/neptune-client/pull/1175))

### Fixes
- Fixed deprecation warnings for implicit casting to string ([#1177](https://github.com/neptune-ai/neptune-client/pull/1177))
- Disabled info about stopping when using read-only mode ([#1166](https://github.com/neptune-ai/neptune-client/pull/1166))
- Disabled "Explore the metadata" message when stopping in debug mode ([#1165](https://github.com/neptune-ai/neptune-client/pull/1165))

## neptune-client 0.16.15

### Fixes
- Correct detection of missing attributes ([#1155](https://github.com/neptune-ai/neptune-client/pull/1155))
- Fixed entrypoint upload on Windows when entrypoint and source files doesnt share same drive ([#1161](https://github.com/neptune-ai/neptune-client/pull/1161))

## neptune-client 0.16.14

### Features
- Add append and extend ([#1050](https://github.com/neptune-ai/neptune-client/pull/1050))

## neptune-client 0.16.13

### Changes
- Automatically Clean junk metadata on script runs ([#1083](https://github.com/neptune-ai/neptune-client/pull/1083), [#1093](https://github.com/neptune-ai/neptune-client/pull/1093))
- New `neptune clear` command ([#1091](https://github.com/neptune-ai/neptune-client/pull/1091), [#1094](https://github.com/neptune-ai/neptune-client/pull/1094))
- `neptune sync` removes junk metadata ([#1092](https://github.com/neptune-ai/neptune-client/pull/1092))
- Increase LOGGED_IMAGE_SIZE_LIMIT_MB to 32MB ([#1090](https://github.com/neptune-ai/neptune-client/pull/1090))

### Fixes
- Fix possible deadlock in `stop()` ([#1104](https://github.com/neptune-ai/neptune-client/pull/1104))
- Add request size limit to avoid 403 error ([#1089](https://github.com/neptune-ai/neptune-client/pull/1089))

## neptune-client 0.16.12

### Changes
- Building a package with Poetry ([#1069](https://github.com/neptune-ai/neptune-client/pull/1069))
- Automatically convert image and html like assignments to uploads  ([#1006](https://github.com/neptune-ai/neptune-client/pull/1006))
- File.from_stream does not load content into memory ([#1065](https://github.com/neptune-ai/neptune-client/pull/1065))
- Move sync and status commands to `neptune.new.cli` package [#1078](https://github.com/neptune-ai/neptune-client/pull/1078)
- `neptune status` - shows trashed containers [#1079](https://github.com/neptune-ai/neptune-client/pull/1079)
- Drop limits for in-memory Files ([#1070](https://github.com/neptune-ai/neptune-client/pull/1070))

## neptune-client 0.16.11

### Fixes
- Fixed versioneer configuration and version detection in conda package ([#1061](https://github.com/neptune-ai/neptune-client/pull/1061))

### Changes
- Upload in-memory files using copy stored on disk ([#1052](https://github.com/neptune-ai/neptune-client/pull/1052))

## neptune-client 0.16.10

### Features
- Track artifacts on S3 compatible storage ([#1053](https://github.com/neptune-ai/neptune-client/pull/1053))

### Fixes
- Update jsonschema requirement with explicit `format` specifier ([#1010](https://github.com/neptune-ai/neptune-client/pull/1010))
- Escape inputs to SQL in Artifact LocalFileHashStorage ([#1034](https://github.com/neptune-ai/neptune-client/pull/1034))
- `jsonschema` requirements unpined and patched related Bravado issue ([#1051](https://github.com/neptune-ai/neptune-client/pull/1051))
- Version checking with importlib and versioneer config update ([#1048](https://github.com/neptune-ai/neptune-client/pull/1048))

### Changes
- More consistent and strict way of git repository, source files and entrypoint detection ([#1007](https://github.com/neptune-ai/neptune-client/pull/1007))
- Moved neptune and neptune_cli to src dir ([#1027](https://github.com/neptune-ai/neptune-client/pull/1027))
- `fetch_runs_table(...)`, `fetch_models_table(...)` and `fetch_model_versions_table(...)` now queries only non-trashed ([#1033](https://github.com/neptune-ai/neptune-client/pull/1033))
- `get_last_run`, `get_run_url`, `get_project` and `neptune.init` marked as deprecated ([#1025](https://github.com/neptune-ai/neptune-client/pull/1025))
- Deprecated implicit casting of objects to strings with `log` and `assign` operations ([#1028](https://github.com/neptune-ai/neptune-client/pull/1028))
- Internally extracted legacy client to `legacy` submodule ([#1039](https://github.com/neptune-ai/neptune-client/pull/1039))
- Marked legacy client as deprecated ([#1047](https://github.com/neptune-ai/neptune-client/pull/1047))

## neptune-client 0.16.9

### Fixes

- Management docstring adjustments ([#1016](https://github.com/neptune-ai/neptune-client/pull/1016))
- Few minor fixes

## neptune-client 0.16.8

### Features
- Added support of HuggingFace integration ([#948](https://github.com/neptune-ai/neptune-client/pull/948))
- Implement trash_objects management function([#996](https://github.com/neptune-ai/neptune-client/pull/996))

### Fixes
- Fixed `with_id` deprecation message ([#1002](https://github.com/neptune-ai/neptune-client/pull/1002))
- Fix passing None as deprecated parameter to deprecated_parameter decorator ([#1001](https://github.com/neptune-ai/neptune-client/pull/1001))

## neptune-client 0.16.7

### Features
- Exposed integrations related utils ([#983](https://github.com/neptune-ai/neptune-client/pull/983))
- Add new with_id parameter to init functions ([#985](https://github.com/neptune-ai/neptune-client/pull/985))
- Introduce filtering columns when fetching run, model and model_version tables ([#986](https://github.com/neptune-ai/neptune-client/pull/986))

### Fixes
- Stop hanging indefinitely on wait when async data synchronization process is dead ([#909](https://github.com/neptune-ai/neptune-client/pull/909))
- Finish stop() faster when async data synchronization process dies ([#909](https://github.com/neptune-ai/neptune-client/pull/909))

## neptune-client 0.16.6

### Features
- Added support for Prophet integration ([#978](https://github.com/neptune-ai/neptune-client/pull/978))
- Log argparse.Namespace objects as dicts ([#984](https://github.com/neptune-ai/neptune-client/pull/984))

## neptune-client 0.16.5

### Features
- Added `NEPTUNE_MODE` environment variable ([#928](https://github.com/neptune-ai/neptune-client/pull/928))
- Added support of Service account management ([#927](https://github.com/neptune-ai/neptune-client/pull/927))
- More informational exception due to plotly and matplotlib incompatibility ([#960](https://github.com/neptune-ai/neptune-client/pull/960))
- Dedicated exceptions for collision and validation errors in `create_project()` ([#965](https://github.com/neptune-ai/neptune-client/pull/965))
- Project key is now optional in API. If it is not provided by user it is generated. ([#946](https://github.com/neptune-ai/neptune-client/pull/946))

### Breaking changes
- Former `ProjectNameCollision` exception renamed to AmbiguousProjectName ([#965](https://github.com/neptune-ai/neptune-client/pull/965))

## neptune-client 0.16.4

### Fixes
- Fix uploading in-memory files lager than 5MB ([#924](https://github.com/neptune-ai/neptune-client/pull/924))
- fetch_extension added to Handler ([#923](https://github.com/neptune-ai/neptune-client/pull/923))

### Changes
- Force jsonschema version < 4.0.0 ([#922](https://github.com/neptune-ai/neptune-client/pull/922))

- Rename and copy update for UnsupportedClientVersion and DeprecatedClientLibraryVersion ([#917](https://github.com/neptune-ai/neptune-client/pull/917))

## neptune-client 0.16.3

### Features
- Added fetching Models method to Project ([#916](https://github.com/neptune-ai/neptune-client/pull/916))

### Fixes
- Fix computing of a multipart upload chunk size ([#897](https://github.com/neptune-ai/neptune-client/pull/897))
- Matching all listed tags instead of any when calling `fetch_runs_table` ([#899](https://github.com/neptune-ai/neptune-client/pull/899))
- Fix invalid processing of delete followed by file upload in a single batch ([#880](https://github.com/neptune-ai/neptune-client/pull/880))
### Changes
- `click.echo` replaced with `logging` ([#903](https://github.com/neptune-ai/neptune-client/pull/903))

## neptune-client 0.16.2

### Features
 - Sync only offline runs inside '.neptune' directory CLI flag ([#894](https://github.com/neptune-ai/neptune-client/pull/894))

### Fixes
- Fix handling of server errors ([#896](https://github.com/neptune-ai/neptune-client/pull/896))

## neptune-client 0.16.1

### Features
- Print metadata url on stop ([#883](https://github.com/neptune-ai/neptune-client/pull/883))

### Fixes
- Fix handling Internal Server Error ([#885](https://github.com/neptune-ai/neptune-client/pull/885))

## neptune-client 0.16.0

### Features
- Added python 3.10 support ([#879](https://github.com/neptune-ai/neptune-client/pull/879))
- Dropped official support for python 3.6 ([#879](https://github.com/neptune-ai/neptune-client/pull/879))

### Fixes
- restart upload when file changes during ([#877](https://github.com/neptune-ai/neptune-client/pull/877))

## neptune-client 0.15.2

### Features
- Added support for workspace visibility in Management API ([#843](https://github.com/neptune-ai/neptune-client/pull/843))
- Exposed container with a property of Handler ([#864](https://github.com/neptune-ai/neptune-client/pull/864))

## neptune-client 0.15.1

### Fixes
- Restore __version__ in neptune.new ([#860](https://github.com/neptune-ai/neptune-client/pull/860))

## neptune-client 0.15.0

### Features
- Methods for creating and manipulating Model Registry objects ([#794](https://github.com/neptune-ai/neptune-client/pull/794))

### Changes
- Renamed --run parameter to --object in `neptune sync` (previous kept as deprecated, [#849](https://github.com/neptune-ai/neptune-client/pull/849))
- More helpful error message on SSL validation problem ([#853](https://github.com/neptune-ai/neptune-client/pull/853))
- Added names to daemon worker threads ([#851](https://github.com/neptune-ai/neptune-client/pull/851))
- Stopped forwarding every attribute from Handler to Attribute ([#815](https://github.com/neptune-ai/neptune-client/pull/815))

## neptune-client 0.14.3

### Features
- Stripping whitespaces from Neptune API Token ([#825](https://github.com/neptune-ai/neptune-client/pull/825))

### Fixes
- Raise proper exception when invalid token were provided ([#825](https://github.com/neptune-ai/neptune-client/pull/825))
- Make status error-handling in legacy client consistent with neptune.new ([#829](https://github.com/neptune-ai/neptune-client/pull/829))

## neptune-client 0.14.2

### Features
- Use new file upload API ([#789](https://github.com/neptune-ai/neptune-client/pull/789))

### Fixes
- Fixed listing available workspaces when invalid name was provided ([#818](https://github.com/neptune-ai/neptune-client/pull/818))
- Added proper docstrings for Project-Level Metadata ([#812](https://github.com/neptune-ai/neptune-client/pull/812))
- Fixed backward compatibility when syncing old offline data ([#810](https://github.com/neptune-ai/neptune-client/pull/810))
- Prevent original numpy array from modifying ([#821](https://github.com/neptune-ai/neptune-client/pull/821))
- Unpin `jsonschema<4`, pin `swagger-spec-validator>=2.7.4` until bravado releases new version ([#820](https://github.com/neptune-ai/neptune-client/pull/820))


## neptune-client 0.14.1

### Fixes
- Fixed legacy url in NVML information ([#795](https://github.com/neptune-ai/neptune-client/pull/795))
- Make init_project accepting kwargs only ([#805](https://github.com/neptune-ai/neptune-client/pull/805))

## neptune-client 0.14.0

### Features
- Interacting with project-level metadata ([#758](https://github.com/neptune-ai/neptune-client/pull/758))
- Copy feature for non-file single value attributes ([#768](https://github.com/neptune-ai/neptune-client/pull/768))

### Fixes
- Fix verifying data size limits in String Atoms and File.from_content ([#784](https://github.com/neptune-ai/neptune-client/pull/784))

## neptune-client 0.13.5

### Fixes
- Restore RunMode for backward compatibility ([#775](https://github.com/neptune-ai/neptune-client/pull/775))
- Restore imports for backward compatibility ([#777](https://github.com/neptune-ai/neptune-client/pull/777))
- Limit number of Series elements sent in single request ([#780](https://github.com/neptune-ai/neptune-client/pull/780))

## neptune-client 0.13.4

### Fixes
- Fix issue that prevented waiting for subprocesses to finish after receiving stop signal from backend ([#774](https://github.com/neptune-ai/neptune-client/pull/774))
  Timeout now overridable using environment var `NEPTUNE_SUBPROCESS_KILL_TIMEOUT`

## neptune-client 0.13.3

### Fixes
- Fixed multithreading bug with StdStreamCaptureLogger ([#762](https://github.com/neptune-ai/neptune-client/pull/762))

## neptune-client 0.13.2

### Fixes
- Fixed fetching numeric values in debug mode ([#745](https://github.com/neptune-ai/neptune-client/pull/745))
- Ensure StdStreamCaptureLogger doesn't log after .close() ([#759](https://github.com/neptune-ai/neptune-client/pull/759))

## neptune-client 0.13.1

### Features
- PyTorchLightning integration is imported directly from `pytorch-lightnig` repo ([#673](https://github.com/neptune-ai/neptune-client/pull/673))

### Fixes
- Fix issue with file upload retry buffer causing 400 bad requests ([#743](https://github.com/neptune-ai/neptune-client/pull/743))

## neptune-client 0.13.0

### Features
- Provide names of existing run attributes to IPython's suggestion mechanism ([#740](https://github.com/neptune-ai/neptune-client/pull/740))
- Add docstrings for project management API ([#738](https://github.com/neptune-ai/neptune-client/pull/738))

### Fixes
- Update MemberRoles to match values in the UI ([#738](https://github.com/neptune-ai/neptune-client/pull/738))

## neptune-client 0.12.1

### Fixes
- Support Artifacts in fetch_runs_table() ([#728](https://github.com/neptune-ai/neptune-client/pull/728))

## neptune-client 0.12.0

### Features
- Human-readable objects representation via `__repr__` ([#717](https://github.com/neptune-ai/neptune-client/pull/717))
- Added project management API ([#695](https://github.com/neptune-ai/neptune-client/pull/695),
  [#720](https://github.com/neptune-ai/neptune-client/pull/720))
- Performance improvements when creating several runs ([#695](https://github.com/neptune-ai/neptune-client/pull/695))

### Fixes
- Temporarily pin `jsonschema<4` (4.0.0 is incompatible with `bravado`; [#719](https://github.com/neptune-ai/neptune-client/pull/719))

## neptune-client 0.11.0

### Fixes
- Boto3 non-strict requirement ([#708](https://github.com/neptune-ai/neptune-client/pull/708))
- Gracefully handle backends not supporting Artifacts ([#709](https://github.com/neptune-ai/neptune-client/pull/709))

## neptune-client 0.10.10

### Features
- API for Artifacts ([#703](https://github.com/neptune-ai/neptune-client/pull/703))

## neptune-client 0.10.9

### Features
- Added psutil as a base requirement ([#675](https://github.com/neptune-ai/neptune-client/pull/675))
- Added capture_traceback in neptune.init() ([#676](https://github.com/neptune-ai/neptune-client/pull/676))

### Fixes
- Fix exception type raised on calling missing method on Handler ([#693](https://github.com/neptune-ai/neptune-client/pull/693))

## neptune-client 0.10.8

### Fixes
- Fix leaks of descriptors
- Fix possible deadlock on synchronisation in async mode

## neptune-client 0.10.7

### Fixes
- Fixed url building in Windows ([#672](https://github.com/neptune-ai/neptune-client/pull/672))

## neptune-client 0.10.6

### Fixes
- Fixed slashes in file operations url concatenation ([#666](https://github.com/neptune-ai/neptune-client/pull/666))

## neptune-client 0.10.5

### Fixes
- Only print info if exception actually occurred when using Run as context manager ([#650](https://github.com/neptune-ai/neptune-client/pull/650))

## neptune-client 0.10.4

### Features
- Added long description for PyPI ([#642](https://github.com/neptune-ai/neptune-client/pull/642))

### Fixes
- Fixed GitPython importing during package preparation ([#647](https://github.com/neptune-ai/neptune-client/pull/647))

## neptune-client 0.10.3

### Features
- Checking current working directory in addition to entrypoint when looking for git repository ([#633](https://github.com/neptune-ai/neptune-client/pull/633))
- Added support for Kedro integration ([#641](https://github.com/neptune-ai/neptune-client/pull/641))

## neptune-client 0.10.2

### Features
- Added NEPTUNE_MONITORING_NAMEPSACE environment variable ([#623](https://github.com/neptune-ai/neptune-client/pull/623))

### Fixes
- Use absolute path for operations queue([#624](https://github.com/neptune-ai/neptune-client/pull/624))
- Fix race condition in operations queue([#626](https://github.com/neptune-ai/neptune-client/pull/626))

## neptune-client 0.10.1

### Features
- Delete namespace (and all child fields and namespaces) ([#619](https://github.com/neptune-ai/neptune-client/pull/619))
- .pop() works invoked on a field ([#617](https://github.com/neptune-ai/neptune-client/pull/617))
- Logging version when using python logger integration ([#622](https://github.com/neptune-ai/neptune-client/pull/622))

## neptune-client 0.10.0

### Breaking changes
- Return path from requested prefix instead of root when fetching namespace ([#609](https://github.com/neptune-ai/neptune-client/pull/609))

### Features
- Heuristics to help users find out they're writing legacy code with new client API or vice versa ([#607](https://github.com/neptune-ai/neptune-client/pull/607))
- Lookup for projects without workspace specification and listing user projects and workspaces ([#615](https://github.com/neptune-ai/neptune-client/pull/615))
- Mechanism to prevent using legacy Experiments in new-API integrations ([#611](https://github.com/neptune-ai/neptune-client/pull/611))

## neptune-client 0.9.19

### Breaking changes
- Prevent logging into stopped runs ([#602](https://github.com/neptune-ai/neptune-client/pull/602))

### Features
- Added more informal exception for invalid API token ([#601](https://github.com/neptune-ai/neptune-client/pull/601))

### Fixes
- **Legacy client** Improved stability by adding retry on failure when uploading ([#604](https://github.com/neptune-ai/neptune-client/pull/604))

## neptune-client 0.9.18

### Fixes
- Check get_ipython() for None ([#598](https://github.com/neptune-ai/neptune-client/pull/598))

## neptune-client 0.9.17

### Features
- Remind user about stopping runs in interactive console and notebooksz ([#595](https://github.com/neptune-ai/neptune-client/pull/595))
- Updating error messages and links to docs ([#593](https://github.com/neptune-ai/neptune-client/pull/593))
- Added support for fast.ai integration ([#590](https://github.com/neptune-ai/neptune-client/pull/590))

## neptune-client 0.9.16

### Fixes
- Allow for updating an already assigned Namespace instead of failing with errors (mostly affects Optuna integration) ([#585](https://github.com/neptune-ai/neptune-client/pull/585))
