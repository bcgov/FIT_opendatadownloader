# FIT Open Data Downloader

[![Lifecycle:Experimental](https://img.shields.io/badge/Lifecycle-Experimental-339999)](https://github.com/bcgov/repomountie/blob/master/doc/lifecycle-badges.md)

GeoBC Foundational Information and Technology (FIT) Section tool for downloading open data and reporting on changes since last download.

## Workflow

1. Based on sources and schedule defined in a provided config file, download spatial data from the internet
2. Compare downloaded data to cached version on object storage
3. If changes are detected, write the latest download to object storage along with a change report


## Installation

Using `pip` managed by the target Python environment:

	git clone git@github.com:bcgov/FIT_opendatadownloader.git
	cd FIT_opendatadownloader
	pip install .


## Usage

A command line interface is provided:

```
$ fit_downloader process --help
Usage: fit_downloader process [OPTIONS] CONFIG_FILE

  For each configured layer - download latest, detect changes, write to file

Options:
  -l, --layer TEXT            Layer to process in provided config.
  -p, --prefix                S3 prefix.
  -f, --force                 Force download to out-path without running
                              change detection.
  -s, --schedule [D|W|M|Q|A]  Process only sources with given schedule tag.
  -V, --validate              Validate configuration
  -v, --verbose               Increase verbosity.
  -q, --quiet                 Decrease verbosity.
  --help                      Show this message and exit.

```

Examples:

1. Validate a configuration file for a given source:
	
		fit_downloader process -vV example_config.json

2. Process data defined in `sources/CAPRD/victoria.json` configuration file, saving to `s3://$BUCKET/CAPRD/victoria`:

		fit_downloader process -v \
		  --prefix s3://$BUCKET/Change_Detection/CAPRD/victoria \
		  sources/CAPRD/victoria.json 


## Configuration

Layers for downloaded are configured per jusrisdiction in [sources](sources). 
Each config .json file has several tag defining how to handle data for the given jurisdiciton:

| tag            | required              | description                                                                          |
|----------------| --------------------- |--------------------------------------------------------------------------------------|
| `out_layer`    |  Y                    | Name of target file/layer (`parks`, `roads`, etc)                                    |
| `source`       |  Y                    | url or file path to file based source (required). For `http` protocol sources, data must be of format readable by GDAL/OGR        |
| `protocol`     |  Y                    | Type of download (`http` - file via http/curl, `esri` - ESRI REST API endpoint, `bcgw` - download BCGW table via WFS/`bcdata`)          |
| `fields`       |  Y                    | List of source field(s) to retain in the download (required)                         |
| `schedule   `  |  Y                    | Download frequency (required, must be one of: [`D, W, M, Q, A`] - daily/weekly/monthly/quarterly/annual) |
| `source_layer` |  N                    | Name of layer to use within source (optional, defaults to first layer in file)       |
| `query`        |  N                    | Query to subset data in source/layer (OGR SQL) (optional, currently only supported for sources where `protocol` is `http`) | 
| `primary_key`  |  N                    | List of source field(s) used as primary key (optional, must be a subset of `fields`) |
| `hash_fields`  |  N                    | List of additional source field(s) to add to a synthetic geometry hash based primary key (optional, must be a subset of fields) |
| `metadata_url` |  N                    | Link to source metadata                                                    |


For the full schema definition, see [`source.schema.json`](source_schema.json).

### Adding/editing config files

To add data sources:

1. Create (or edit) a config file with location/name corresponding to the admin area. For example:

	`/sources/CAPRD/central_saanich.json`

2. If adding config files, consider validating the file names. A simple validation script is provided to check that file names correspond to values in [`sources/valid_sources.csv`](sources/valid_sources.csv).
To use the script:

		$ cd sources
		$ python validate_source_filenames.py .
		Names of all 15 json files in . are valid

3. Add sources to the config as needed. As a guide, see other files present in `/sources` and the configuration notes above.
  Note that only two `out_layer` values are supported at this time, `parks` and `roads`

### Tips and tricks

As noted above, the `source` tag in the config file is the url or file path. For sources of protocol `http`, the data must be stored in a format readable by GDAL/OGR.
Steps to determine this will vary by data source, but the general sequence is:

- navigate to the data source's public web page and find the open data page/portal/etc (eg https://opendata.victoria.ca/)
- find the best link to the data of interest, where the general preference (in descending order) is:
	+ direct links to data files (eg https://www.nanaimo.ca/GISFiles/shp/Parks.zip)
	+ ArcGIS REST API endpoints (eg https://maps.victoria.ca/server/rest/services/OpenData/OpenData_Transportation/MapServer/25)
	+ links that auto re-direct to data files (eg https://governmentofbc.maps.arcgis.com/sharing/rest/content/items/4bba119c2e9042d683cc9378fb1e836e/data)
- generally, any format that is [readable by OGR](https://gdal.org/en/stable/drivers/vector/index.html) is acceptable, but (with all else being equal) the order of preference would be:
	+ GDB/GPKG
	+ geojson
	+ shp
- while the script may handle `source` urls without modification, prefixing sources of protocol `http` with `/vsicurl/` (or `/vsizip//vsicurl` if the data is zipped) will generally be more reliable

To test/debug sources of protocol `http`, use `ogr2ogr` in debug and read-only mode, with the curl debug set to verbose:

	ogrinfo -ro \
    /vsizip//vsicurl/https://opendata.chilliwack.com/shp/Parks_SHP.zip \
    --debug ON \
    --config CPL_CURL_VERBOSE=TRUE

The resulting output is very verbose. If a given source cannot be read by `ogrinfo`, look through the output for things like:

- any network errors reported
- redirects from endpoints to static files (if this is the case, replace the endpoint url with direct file url)

If problems continue, try downloading the file with a web browser and reading the result.
In some cases, the name of the zipfile downloaded does not correspond with the .gdb within, or zipfiles may be nested.
For example, Langley (City) packages an arbitrary .gdb into a file called `transport.gdb.zip`, that can be handled like this:
`"source": "/vsizip/{/vsicurl/https://governmentofbc.maps.arcgis.com/sharing/rest/content/items/4bba119c2e9042d683cc9378fb1e836e/data}/CoL_TransportationNetwork September 25 2024.gdb"` - see the /vsizip/ link below for how to handle zipfile complications. 

When debugging connection to a quirky server/file combination, see these ogr2ogr/gdal references:

- [configuration options](https://gdal.org/en/stable/user/configoptions.html#global-configuration-options)
- [vsicurl](https://gdal.org/en/stable/user/virtual_file_systems.html#vsicurl-http-https-ftp-files-random-access)
- [vsizip](https://gdal.org/en/stable/user/virtual_file_systems.html#read-capabilities)

## Development and testing

### virtual environment

Using GDAL on your system:

	$ git clone git@github.com:bcgov/FIT_opendatadownloader.git
	$ cd FIT_opendatadownloader
	$ python -m venv .venv
	$ source .venv/bin/activate
	$ pip install -e .[test]
	(.venv) $ py.test

### Dockerized environment

Using GDAL on a docker image:

To build:

	$ git clone git@github.com:bcgov/FIT_opendatadownloader.git
	$ cd FIT_opendatadownlaoder
	$ docker build -t fit_opendatadownloader .

Drop in to a bash session:

	$ docker run --rm -it -v ./:/home/fit_opendatadownloader fit_opendatadownloader bash
