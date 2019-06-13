.. _limits-top:

Limits
======

Storage limit
-------------
According to the `pricing <https://neptune.ml/#pricing>`_, storage is set per **project**:

* individual users - 5GB,
* teams - 50GB.

If you hit the limit, you can: start new project, or contact us directly at `contact@neptune.ml <contact@neptune.ml>`_.

Number of experiments limit
---------------------------
According to the `pricing <https://neptune.ml/#pricing>`_, there is such limit per **project**:

* individual users - 5k,
* teams - 50k.

If you hit the limit, you can: start new project, or contact us directly at `contact@neptune.ml <contact@neptune.ml>`_.

Experiment's logs limit
-----------------------
Each log type in Neptune (metric, text, image) is limited to 10k data points.

API calls rate limits
---------------------
`Neptune-client <https://neptune.ml>`_ uses Python API to communicate with Neptune servers. Users are restricted to 1k requests per minute. If more requests are being placed, neptune-client will retry sending the data in the future (when usage does not approach the limit). In such case, Users may notice some delay between the actual state of the process that executes an experiment and data displayed in Neptune Web application. Extent of this effect is proportional to the number of API calls over the 1k limit.

.. note::

    Our experiences suggests that only few AI research groups hit those limits.
