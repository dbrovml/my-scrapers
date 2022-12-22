# Sreality scraper
This folder contains two scripts to be run in the following order:
1. `get_apartments.py` - scrapes the list of urls of apartments for sale in Prague from the search results page. Visits each item page and scrapes its metadata (price, area, location, amenities, etc.). Dumps individual apartment data as json to local disk.
2. `postprocess.py` - cleans and parses json files into a single csv containing the features of all apartments. <br>

As of Dec 2022, the resulting dataset contained data for **5 817** apartments.
