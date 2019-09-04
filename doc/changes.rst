==========
Change Log
==========

0.3.0 (unreleased)
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
