class _Path:

    def __init__(self, path):
        """
        path: str
        """
        super().__init__()
        # TODO validate and normalize to /path/to/variable
        self._value = path

    def __add__(self, other):
        """
        other: string or _Path
        """
        if isinstance(other, str):
            return self + _Path(other)
        elif isinstance(other, _Path):
            return _Path(self._value + other._value)
        else:
            raise ValueError()

    def __eq__(self, other):
        return self._value == other._value

    def __hash__(self):
        return hash(self._value)

    def __str__(self):
        return self._value

    def __repr__(self):
        return f"_Path({self._value})"