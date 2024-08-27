# FIT Change Detector 

[![Lifecycle:Experimental](https://img.shields.io/badge/Lifecycle-Experimental-339999)](https://github.com/bcgov/repomountie/blob/master/doc/lifecycle-badges.md)

GeoBC Foundational Information and Technology (FIT) Section tool for monitoring open data and reporting on any detected changes.

## Workflow

1. Based on sources and schedule defined in a provided config file, download spatial data from the internet to object storage
2. Compare downloaded data to previous version
3. If changes are detected to schema or data, generate a diff and/or report and alert data administrators


## Installation

Using `pip` managed by the target Python environment:

	git clone git@github.com:bcgov/FIT_changedetector.git
	cd FIT_changedetector
	pip install .


## Usage

A CLI is provided for typical tasks:

1. Validate a sources configuration file:
	
		changedetector download sources_example.json --dry-run -v

2. Download data defined in `sources_example.json` configuration file to individual `parks.gdb.zip` files per municipality, 
   saving to `/path/to/Change_Detection/` on the local filesystem:

		changedetector download sources_example.json -v -p /path/to/Change_Detection -o parks.gdb -nln parks


## Configuration

Sources/layers to be downloaded are defined as json. 
For examples, see the automated download configurations (one per feature type) in the [`config`](config) folder.
For the full schema definition, [`source.schema.json`](source.schema.json).

| key                                  | description                                                                          |
|------------------------------------- |--------------------------------------------------------------------------------------|
| `admin_area_abbreviation`            | Abbreviated name of admin area, taken from `WHSE_LEGAL_ADMIN_BOUNDARIES.ABMS_MUNICIPALITIES_SP` (required) |
| `admin_area_group_name_abbreviation` | Abbreviated Regional District initials, as used by DRA program (required)            |
| `metadata_url`                       | Link to source metadata (optional)                                                   |
| `source`                             | url or file path to file based source, format readable by GDAL/OGR (required)        |
| `layer`                              | Name of layer to use within source (optional, defaults to first layer in file)       |
| `query`                              | Query to subset data in source/layer (OGR SQL) (optional)                            |
| `fields`                             | List of source field(s) to retain in the download (required)                         |
| `primary_key`                        | List of source field(s) used as primary key (optional, must be a subset of `fields`) |
| `schedule   `                        | Download frequency (required, must be one of: [`D, W, M, Q, A`] - daily/weekly/monthly/quarterly/annual) |



## Local development and testing

### virtual environment

Using your system GDAL:

	$ git clone git@github.com:bcgov/FIT_changedetector.git
	$ cd FIT_changedetector
	$ python -m venv .venv
	$ source .venv/bin/activate
	$ pip install -e .[test]
	(.venv) $ py.test

### Dockerized gdal

GDAL 3.7.0 is the latest available in a BCGov GTS Python environment.
A Dockerfile is provided to create a similar testing environment.

To build:

	$ git clone git@github.com:bcgov/FIT_changedetector.git
	$ cd FIT_changedetector
	$ docker build -t fit_changedetector .

Drop in to a bash session:

	$ docker run --rm -it -v ./:/home/fit_changedetector fit_changedetector  bash

Note that Python 3.9 is not available via the [gdal ubuntu docker images](https://github.com/OSGeo/gdal/tree/master/docker#small-ghcrioosgeogdalubuntu-small-latest), testing against 3.10 should be fine for purposes of this tool.