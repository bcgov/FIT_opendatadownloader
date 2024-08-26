# FIT Change Detector 

[![Lifecycle:Experimental](https://img.shields.io/badge/Lifecycle-Experimental-339999)](https://github.com/bcgov/repomountie/blob/master/doc/lifecycle-badges.md)

GeoBC Foundational Information and Technology (FIT) Section tool for monitoring open data and reporting on any detected changes.

## Method

1. Based on sources and schedule in provided config file, download spatial data from the internet to BC object storage
2. Compare downloaded data to previous version
3. If changes are detected to schema or data, generate a diff and report to data administrators responsible for ingesting data to Provincial databases

## Usage

1. Validate a sources configuration file:
	
		python download.py sources_example.json --dry-run -v

2. Download data defined in configuration file:

		python download.py sources_example.json -v


## Configuration

Sources/layers to be downloaded are defined as json. See `sources_example.json` for an example and `source.schema.json` for the full schema definition.

| key                                | description |
|--------------                      |-------------|
| `admin_area_abbreviation`            | Abbreviated name of admin area, taken from `WHSE_LEGAL_ADMIN_BOUNDARIES.ABMS_MUNICIPALITIES_SP` |
| `admin_area_group_name_abbreviation` | Abbreviated Regional District initials, as used by DRA program |
| `metadata_url`                       | Link to source metadata, where available |
| `source`                             | url or file path to file based source, format readable by GDAL/OGR |
| `layer`                              | Name of layer to use within source, default is first layer in file |
| `query`                              | Query to subset data in source/layer (OGR SQL) |
| `fields`                             | List of source field(s) to retain in the download |
| `primary_key`                        | List of source field(s) used as primary key (must be a subset of `fields`)|


## Local development and testing

Scripts must be capable of running on BCGov GTS infrastructure, using the available Python environment (3.9.18).
For local development and testing, a Dockerfile is provided to create a similar environment (equivalent versions of required dependencies).

To build:

	$ docker build -t fit_changedetector .

Drop in to a bash session:

	$ docker run --rm -it -v ./:/home/fit_changedetector fit_changedetector  bash	

Note that Python 3.9 is not available via the [gdal ubuntu docker images](https://github.com/OSGeo/gdal/tree/master/docker#small-ghcrioosgeogdalubuntu-small-latest), testing against 3.10 should be fine for purposes of this tool.