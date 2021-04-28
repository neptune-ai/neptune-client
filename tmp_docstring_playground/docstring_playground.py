from typing import Optional

from numpy import np


class GoogleStyleDocstring:
    def __init__(self, param1: Optional[str] = None, param2: Optional[str] = None):
        """GoogleStyleDocstring text

        Some additional description of class

        Args:
            param1:(str, optional): Short description of `param1`. Defaults to `None`.
                And additional param1 information.


                This text will be quite long.
                And will contain other paragraphs...

                ...separated by two newlines.

            param2: (str, optional): Simply `param2`. Defaults to `None`.

        Examples:
            >>> # Basic usage example:
            ... gsd = NumpyStyleDocstring()

            >>> # But you can pass whatever you like
            ... gsd = NumpyStyleDocstring(1, 2)
        """

        self._param1 = param1
        self._param2 = param2


class NumpyStyleDocstring:
    def __init__(self, param1: Optional[str] = None, param2: Optional[str] = None):
        """NumpyStyleDocstring text

        Some additional description of class

        Parameters
        ----------
        param1 : str, optional
            Short description of `param1`. Defaults to `None`.
            And additional param1 information.


            This text will be quite long.
            And will contain other paragraphs...

            ...separated by two newlines.

        param2 : str, optional
            Simply `param2`. Defaults to `None`.
            (str, optional): Simply `param2`. Defaults to `None`.

        Examples
        --------
            >>> # Basic usage example:
            ... gsd = NumpyStyleDocstring()

            >>> # But you can pass whatever you like
            >>> gsd = NumpyStyleDocstring(1, 2)
            >>> foo = 'bar'
        """

        self._param1 = param1
        self._param2 = param2


class ReStyleDocstring:
    def __init__(self, param1: Optional[str] = None, param2: Optional[str] = None):
        """ReStyleDocstring text

        Some additional description of class

        :param param1:  str, optional
            Short description of `param1`. Defaults to `None`.
            And additional param1 information.

            This text will be quite long.
            And will contain other paragraphs...

            ...separated by two newlines.
        :param param2: Simply `param2`. Defaults to `None`.

        .. doctest::
            >>> # Basic usage example:
            ... gsd = ReStyleDocstring()

            >>> # But you can pass whatever you like
            ... gsd = ReStyleDocstring(1, 2)
        """

        self._param1 = param1
        self._param2 = param2


class EpytextStyleDocstring:
    def __init__(self, param1: Optional[str] = None, param2: Optional[str] = None):
        """

        @param param1: str, optional
            Short description of `param1`. Defaults to `None`.
            And additional param1 information.

            This text will be quite long.
            And will contain other paragraphs...

            ...separated by two newlines.

        @param param2: Simply `param2`. Defaults to `None`.

        >>> # Basic usage example:
        ... gsd = ReStyleDocstring()

        >>> # But you can pass whatever you like
        ... gsd = ReStyleDocstring(1, 2)
        """

        self._param1 = param1
        self._param2 = param2


if __name__ == '__main__':
    gsd = GoogleStyleDocstring()
    nsd = NumpyStyleDocstring()
    rsd = ReStyleDocstring()
    esd = EpytextStyleDocstring()

    np.npv
