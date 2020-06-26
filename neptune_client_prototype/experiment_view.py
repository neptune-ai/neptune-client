class ExperimentView:
  
    def __init__(self, experiment, path):
        """
        experiment: Experiment
        path: _Path
        """
        super().__init__()
        self._experiment = experiment
        self._path = path

    def __getitem__(self, path):
        """
        key: string
        """
        return ExperimentView(self._experiment, self._path + path)

    def _get_variable(self):
        return self._experiment._get_variable(self._path)

    def _set_variable(self, var):
        self._experiment._set_variable(self._path, var)

    @property
    def value(self):
        return self._get_variable().value

    @value.setter
    def value(self, v):
        var = self._get_variable()
        if var:
            var.value = v
        else:
            var = Atom(self._experiment, self._path, v)
            self._set_variable(var)

    def log(self, value, step=None, timestamp=None):
        var = self._experiment._get_variable(self._path)
        if not var:
            var = Series(self._experiment, self._path, type_placeholder)
            self._experiment._set_variable(self._path, var)
        var.log(value, step, timestamp)

    def add(self, *values):
        var = self._get_variable()
        if not var:
            var = Set(self._experiment, self._path, type_placeholder)
            self._set_variable(var)
        self.add(*values)

    def __getattr__(self, attr):
        pass
