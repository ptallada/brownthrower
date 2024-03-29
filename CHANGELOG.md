# Change Log (http://keepachangelog.com/)
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).


## [3.2.0] - 2019-09-12
### Changed
- Changes for Python 3 compatibility. (Santiago Serrano)
- Randomize job selection to avoid queue contention. (Santiago Serrano)
- Use $EDITOR by default, fallback to `vi`. (Pau Tallada)


## [3.0.1] - 2016-10-20
### Added
- Implement job profiling. (Pau Tallada)

### Removed
- Removed unused index on `Tag` (`name`, `value`). (Pau Tallada)


## [3.0.0a1] - 2016-02-26
### Added
- New `job edit description` command for documentation purposes. (Pau Tallada)
- Columns and tables may have comments stored at the database level. (Pau Tallada)

### Changed
- Bump required version of `sqlalchemy` to 1.0. (Pau Tallada)

### Fixed
- Fix bug when recovering `Table` documentation. (Pau Tallada)

### Removed
- Dispatchers are no longer shipped nor supported. (Pau Tallada)


## [2.5.0] - 2015-12-17
### Added
- Store a copy of stdout and stderr of each job. (Pau Tallada)

## Fixed
- Do not crash editing a dataset of an unavailable task. (Pau Tallada)


## [2.4.1] - 2015-11-20
### Fixed
- Require `glite` version compatible with delelgation_id argument. (Pau Tallada)


## [2.4.0] - 2015-11-10
### Changed
- Add an option to specify a custom delegation_id for gLite. (Pau Tallada)
- Add timestamp to log messages. (Pau Tallada)


## [2.3.0] - 2015-08-21
### Fixed
- Job new state might not have output in case of error. (Pau Tallada)
- Transactional session helper may execute the commit twice. (Pau Tallada)
- Allow dots ('.') when filtering in `job list`. (Pau Tallada)


## [2.2.0] - 2015-05-15
### Changed
- Use packaged version of `pydevd`.


## [2.1.0] - 2015-04-29
### Changed
- Remove outdated data from `README.md`. (Pau Tallada)
- Upgrade dependencies on `glite`, `pyparsing` and `SQLAlchemy`. (Pau Tallada)


## [2.0.1] - 2015-04-21
### Added
- Add `CHANGELOG.md`. (Pau Tallada)
- Add `MANIFEST.in`. (Pau Tallada)

### Changed
- Rename `README.txt` to `README.md`. (Pau Tallada)

### Fixed
- Do not upgrade to `pyparsing` ~2.0. (Pau Tallada)
- Reversed logic in looping code for `runner.serial`. (Pau Tallada)
- Wrong syntax for code blocks in `CHANGELOG.md`. (Pau Tallada)


## [2.0.0] - 2014-11-21
