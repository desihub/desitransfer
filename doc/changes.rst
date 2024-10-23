==========
Change Log
==========

1.0.4 (2024-10-23)
------------------

* Add ``engineering/focalplane/endofnight`` to morning engineering transfers (PR `#63`_).

.. _`#63`: https://github.com/desihub/desitransfer/pull/63

1.0.3 (2024-09-26)
------------------

* Convert Tucson transfer to parallel operation (PR `#61`_).

.. _`#61`: https://github.com/desihub/desitransfer/pull/61

1.0.2 (2024-06-21)
------------------

* Only sync the most recent year for nightwatch data (PR `#60`_).

.. _`#60`: https://github.com/desihub/desitransfer/pull/60

1.0.1 (2024-05-01)
------------------

* Add scripts to support data transfers to Utah (PR `#59`_).

.. _`#59`: https://github.com/desihub/desitransfer/pull/59

1.0.0 (2023-11-30)
------------------

* Refactor package to deprecate ``setup.py`` (PR `#58`_).
* Remove deprecated code such as ``nightlog.py`` (PR `#58`_).
* Add Spacewatch image download (PR `#58`_).
* Download nightlog data earlier in the day (PR `#58`_).
* Update engineering data transferred to Tucson (PR `#58`_).
* Better logging of and resilience to nightwatch transfer errors (PR `#58`_).

.. _`#58`: https://github.com/desihub/desitransfer/pull/58

0.9.2 (2023-05-31)
------------------

* Eliminate references to cori and :envvar:`CSCRATCH` (PR `#52`_).

.. _`#52`: https://github.com/desihub/desitransfer/pull/52

0.9.1 (2023-04-24)
------------------

* Exclude ``2022*`` from raw data transfers to Tucson; add API documentation
  completeness test; other minor changes to Tucson transfers (PR `#51`_).

.. _`#51`: https://github.com/desihub/desitransfer/pull/51

0.9.0 (2022-12-16)
------------------

* Nightlog data is now included in the daily transfer, instead of a
  separate process (PR `#50`_).
* Add annual transfer statistics script.
* Tweak mail handling for Tucson transfer.

.. _`#50`: https://github.com/desihub/desitransfer/pull/50

0.8.0 (2022-09-21)
------------------

* Use a more compact format for raw data transfer status (PR `#49`_).

.. _`#49`: https://github.com/desihub/desitransfer/pull/49

0.7.2 (2022-08-11)
------------------

* Adjusted Tucson transfer configuration in light of upcoming public data
  releases (PR `#48`_).

.. _`#48`: https://github.com/desihub/desitransfer/pull/48

0.7.1 (2022-03-22)
------------------

* Minor fix to wait times for nightwatch transfer.  No other changes.

0.7.0 (2022-03-22)
------------------

* Update daily transfer data sets; migrate Tucson transfer script to
  Python (PR `#47`_).

.. _`#47`: https://github.com/desihub/desitransfer/pull/47

0.6.6 (2022-02-04)
------------------

* Variable-frequency nightwatch transfers; other bug fixes (PR `#46`_).

.. _`#46`: https://github.com/desihub/desitransfer/pull/46

0.6.5 (2021-09-16)
------------------

* Update the NightLog directory structure (PR `#43`_).

.. _`#43`: https://github.com/desihub/desitransfer/pull/43

0.6.4 (2021-09-10)
------------------

* Use NERSC API to check for HPSS availability; update Tucson transfers for everest (PR `#41`_).
* Test removing user-write permission from raw data *directories* (Issue `#28`_)

.. _`#41`: https://github.com/desihub/desitransfer/pull/41
.. _`#28`: https://github.com/desihub/desitransfer/issues/28

0.6.3 (2021-06-28)
------------------

* Add more information about the types of checksum failures; also more detailed
  timing of daily transfers, mirror certain software to Tucson (PR `#39`_).

.. _`#39`: https://github.com/desihub/desitransfer/pull/39

0.6.2 (2021-05-24)
------------------

* Exclude ``preproc`` files from daily reductions when transferring to Tucson;
  update name of ``NightSummaryYYYYMMDD.html`` files (PR `#37`_).

.. _`#37`: https://github.com/desihub/desitransfer/pull/37

0.6.1 (2021-04-27)
------------------

* Sync ``NightSummaryYYYYMMDD`` files; adjust HPSS utility path (PR `#35`_).

.. _`#35`: https://github.com/desihub/desitransfer/pull/35

0.6.0 (2021-04-06)
------------------

* Renamed ``master`` branch to ``main``.
* Add nightlog transfers (PR `#32`_).

.. _`#32`: https://github.com/desihub/desitransfer/pull/32

0.5.1 (2021-02-09)
------------------

* Fix nightwatch transfer night offset (PR `#31`_).

.. _`#31`: https://github.com/desihub/desitransfer/pull/31


0.5.0 (2021-01-18)
------------------

* Moderate refactor of :command:`desi_transfer_daemon` (PR `#27`_):

  - Remove vestigial pipeline activation code.
  - More visible warnings of rsync and checksum errors in raw data transfers.
  - Move all raw data to :envvar:`DESI_SPECTRO_DATA`, even if errors detected.
  - Redo checksum on "catch-up" data.

.. _`#27`: https://github.com/desihub/desitransfer/pull/27

0.4.0 (2020-12-23)
------------------

* Migrated from Travis CI to GitHub Actions.
* Improve real-time nightwatch transfer for use when NERSC is unavailable;
  better synchronization between daily engineering transfer and Tucson
  mirror transfer (PR `#24`_).

.. _`#24`: https://github.com/desihub/desitransfer/issues/24

0.3.9 (2020-12-03)
------------------

* Deprecate continuous nightwatch transfers; nightwatch is now part of the
  daily engineering transfer (PR `#21`_).
* Allow alternate scratch directory to be chosen if :envvar:`CSCRATCH` is
  unavailable (PR `#21`_).
* Ignore malformed symlinks in the raw data staging area (Issue `#22`_).

.. _`#21`: https://github.com/desihub/desitransfer/pull/21
.. _`#22`: https://github.com/desihub/desitransfer/issues/22

0.3.8 (2020-10-26)
------------------

* Better logging and error notification for NERSC - Tucson transfers (PR `#18`_).
* Change wait time between raw data transfer to one minute (PR `#19`_).

.. _`#18`: https://github.com/desihub/desitransfer/pull/18
.. _`#19`: https://github.com/desihub/desitransfer/pull/19

0.3.7 (2020-06-11)
------------------

* Updates to Tucson transfer script (PR `#14`_).
* Remove Apache ACL option (PR `#15`_).

.. _`#14`: https://github.com/desihub/desitransfer/pull/14
.. _`#15`: https://github.com/desihub/desitransfer/pull/15

0.3.6 (2020-03-19)
------------------

* Support ICS-generated checksum files (PR `#13`_).
* Add Tucson transfer script.
* Improvements to transfer status report.

.. _`#13`: https://github.com/desihub/desitransfer/pull/13

0.3.5 (2020-03-03)
------------------

* Support direct KPNO to Tucson transfers when NERSC is shut down (PR `#12`_).
* Move nightwatch transfer script into this package.

.. _`#12`: https://github.com/desihub/desitransfer/pull/12

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
