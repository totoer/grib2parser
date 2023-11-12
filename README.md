# GRIB2 -> [WGF4, PNG]

## Requirements
* aiohttp
* Pillow

## Config

### Section [base]
* workers - count of workers for blocked IO operations
* url_template - URL template for GRIB file
    * idx - number of item in dataset from 0 to (`--range`)
    * Any datetime template variables

See for example `fixture/config.ini`

## Run
### Help
    -h, --help, show this help message and exit
    -c CONFIG, --config CONFIG, path to config file
    -d DATE, --date DATE, date of source dataset
    -r RANGE, --range RANGE, count datasets by date
    -t TARGET, --target TARGET, result type: WGF4 or picture

### Examples
Example for: `https://opendata.dwd.de/weather/nwp/icon-d2/grib/12/tot_prec/`
Template URL: `https://opendata.dwd.de/weather/nwp/icon-d2/grib/12/tot_prec/icon-d2_germany_regular-lat-lon_single-level_%%Y%%m%%d%%H_%%(idx)02d_2d_tot_prec.grib2.bz2`

    main.py -d 2023-11-11:12 -r 48 -t WGF4

* `-d` - date of source dataset
* `-r` - count datasets by date
* `-t` - save to WGF4

Example for: `https://nomads.ncep.noaa.gov/pub/data/nccf/com/cfs/prod/cdas.20231111/`
Template URL: `https://nomads.ncep.noaa.gov/pub/data/nccf/com/cfs/prod/cdas.%%Y%%m%%d/cdas1.t12z.pgrbh%%(idx)02d.grib2`

    main.py -d 2023-11-11 -r 9 -t picture

* `-d` - date of source dataset
* `-r` - count datasets by date
* `-t` - save to PNG
