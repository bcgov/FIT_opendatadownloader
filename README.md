# FIT Change Detector 

[![Lifecycle:Experimental](https://img.shields.io/badge/Lifecycle-Experimental-339999)](https://github.com/bcgov/repomountie/blob/master/doc/lifecycle-badges.md)

GeoBC Foundational Information and Technology (FIT) Section tool for monitoring open data and reporting on any detected changes.

## Workflow

1. Based on sources and schedule defined in a provided config file, download spatial data from the internet
2. Compare downloaded data to cached version on object storage
3. If changes are detected, write the latest download to object storage along with a change report


## Installation

Using `pip` managed by the target Python environment:

	git clone git@github.com:bcgov/FIT_changedetector.git
	cd FIT_changedetector
	pip install .


## Usage

A CLI is provided for typical tasks:

1. Validate a configuration file for a given source:
	
		changedetector process source_example.json --dry-run -v

2. Download layers defined in `source_example.json` configuration file to zipped gdb (one file per layer),
   saving to `/path/to/Change_Detection/` on the local filesystem:

		changedetector download sources_example.json -v -p /path/to/Change_Detection


## Configuration

Layers for downloaded are configured per jusrisdiction in [sources](sources). 
Each config .json file has several tag defining how to handle data for the given jurisdiciton:

| tag            | required              | description                                                                          |
|----------------| --------------------- |--------------------------------------------------------------------------------------|
| `out_layer`    |  Y                    | Name of target file/layer (`parks`, `roads`, etc)                                    |
| `source`       |  Y                    | url or file path to file based source, format readable by GDAL/OGR (required)        |
| `protocol`     |  Y                    | Type of download (`http` - file via http, `esri` - dESRI REST API endpoint)          |
| `fields`       |  Y                    | List of source field(s) to retain in the download (required)                         |
| `schedule   `  |  Y                    | Download frequency (required, must be one of: [`D, W, M, Q, A`] - daily/weekly/monthly/quarterly/annual) |
| `source_layer` |  N                    | Name of layer to use within source (optional, defaults to first layer in file)       |
| `query`        |  N                    | Query to subset data in source/layer (OGR SQL) (optional, currently only supported for sources where `protocol` is `http`) | 
| `primary_key`  |  N                    | List of source field(s) used as primary key (optional, must be a subset of `fields`) |
| `metadata_url` |  N                    | Link to source metadata                                                    |


For the full schema definition, see [`source.schema.json`](source.schema.json).

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