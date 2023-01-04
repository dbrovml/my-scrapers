# Rohlik scraper
Warning: this is a stale script that will most probably fail due to continuous changes on the web.<br><br>
This folder contains two scripts to be run in the following order:
1. `scrape.py` - scrapes product and category trees, e.g. (pecivo -> slane -> rohlik). For each product, scapes its price, quantity, text description, contents, allergens, and image.
2. `index.py` - cleans and parses json files into a single csv containing the features of all products. <br>

Note: these scripts use Selenium and Google Chrome + geckodriver stack.<br>
As of Dec 2020, the resulting dataset contained metadata and images for **10 500** products.