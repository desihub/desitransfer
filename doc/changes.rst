==========
Change Log
==========

0.3.4 (2020-01-10)
------------------

* Guard against corrupted status JSON files; restore transfer status;
  additional daily transfers (PR `#10`_).

.. _`#10`: https://github.com/desihub/desitransfer/pull/10

0.3.3 (2019-12-18)
------------------

* Additional daily transfers; make sure daily transfers are readable by
  apache/www (PR `#8`_).

.. _`#8`: https://github.com/desihub/desitransfer/pull/8

0.3.2 (2019-10-15)
------------------

* Inhibit transfers when checksums are being computed at KPNO (PR `#7`_).

.. _`#7`: https://github.com/desihub/desitransfer/pull/7


0.3.1 (2019-09-12)
------------------

* Report version string in logs and on command line (PR `#6`_).
* Only a ``desi`` file is needed to trigger the pipeline (PR `#5`_).

.. _`#5`: https://github.com/desihub/desitransfer/pull/5
.. _`#6`: https://github.com/desihub/desitransfer/pull/6

0.3.0 (2019-09-04)
------------------

* Unified configuration file (PR `#3`_).
  - Simplified passing of command-line options, configuration, etc.
  - Store state data in a first-class object.

.. _`#3`: https://github.com/desihub/desitransfer/pull/3

0.2.2 (2019-08-29)
------------------

* Improvements based on operational testing (PR `#2`_).
  - Catch unexpected exceptions.
  - Update status reporting and display.
  - Don't include exposure number in "last" pipeline runs.
  - Make sure other similarly-named processes don't interfere with daemon startup.

.. _`#2`: https://github.com/desihub/desitransfer/pull/2

0.2.1 (2019-08-27)
------------------

* Removed obsolete shell script :command:`desi_daily_transfer.sh`.
* Refactor code for increased test coverage (PR `#1`_).

.. _`#1`: https://github.com/desihub/desitransfer/pull/1

0.2.0 (2019-08-22)
------------------

* Working (Python) version of :command:`desi_daily_transfer`.
* Increased test coverage.

0.1.0 (2019-08-08)
------------------

* First operational tag.
