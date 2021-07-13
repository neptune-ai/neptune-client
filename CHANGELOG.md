## neptune-client 0.10.2 [UNRELEASED]

### Features
- Added NEPTUNE_MONITORING_NAMEPSACE environment variable ([#623](https://github.com/neptune-ai/neptune-client/pull/623))

## neptune-client 0.10.1

### Features
- Logging version when using python logger integration ([#622](https://github.com/neptune-ai/neptune-client/pull/622))
- Delete namespace (and all child fields and namespaces) ([#619](https://github.com/neptune-ai/neptune-client/pull/619))
- .pop() works invoked on a field ([#617](https://github.com/neptune-ai/neptune-client/pull/617))

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
