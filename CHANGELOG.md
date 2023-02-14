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
