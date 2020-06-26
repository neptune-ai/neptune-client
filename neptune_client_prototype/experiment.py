class Experiment:
  
    def __init__(self):
        super().__init__()
        self._members = {}xs

    def _get_variable(self, path):
        """
        path: _Path
        """
        return self._members.get(path)

    def _set_variable(self, path, variable):
        self._members[path] = variable

    def __getitem__(self, path):
        """
        path: string
        """
        return ExperimentView(self, _Path(path))
