# downloader

Download spatial data from the internet, save to file.

## Usage

1. Validate:
	
		python download.py sources.json --dry-run -v

2. Download:

		python download.py sources.json -v


## sources.json

Sources/layers to be downloaded are defined as json. See `sources_example.json` for an example and `source.schema.json` for the full schema definition.

| key                                | description |
|--------------                      |-------------|
| admin_area_abbreviation            | Abbreviated name of admin area, taken from `WHSE_LEGAL_ADMIN_BOUNDARIES.ABMS_MUNICIPALITIES_SP` |
| admin_area_group_name_abbreviation | Abbreviated Regional District initials, as used by DRA program |
| metadata_url                       | Link to source metadata, where available |
| source                             | url or file path to file based source, format readable by GDAL/OGR |
| layer                              | Name of layer to use within source when source_type=FILE, default is first layer in file |
| query                              | Query to subset data in source/layer (OGR SQL) |
| fields                             | List of fields to retain in the download |
| primary_key                        | List of source field(s) used as primary key |
