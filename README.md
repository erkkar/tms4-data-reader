TMS-4 Data Reader
=================

The class `TMSDataReader` can be used to parse data produced by the 
[TOMST Lolly software](https://tomst.com/web/en/systems/tms/software/).

Installation
------------

```
pip install git+https://github.com/erkkar/tms4-data-reader
```

Alternatively, download the repository as ZIP archive and run

``` 
pip install tms4-data-reader-main.zip
``` 


Usage
-----

Set up a reader to a directory

    >>> from tms4_data_reader import TMSDataReader
    >>> reader = TMSDataReader('data/')

Get number of files 

    >>> reader.filecount
    1

Check if there are missing files by passing a list of logger id’s:

    >>> reader.check_missing([94226401])
    set()

Read the data as a pandas `DataFrame`:

    >>> loggerdata = reader.read()
    >>> loggerdata.filter(like='T').head()
                                   T1      T2      T3
    logger_id measurement_id                         
    94226401  1               11.3125  8.3750  6.5000
              2               11.2500  8.2500  6.3750
              3               11.2500  8.1250  6.2500
              4               11.1875  8.0000  6.1875
              5               11.1250  8.0625  6.3750

Tests
-----

Run tests with

```
python -m doctest -v README.md
```
