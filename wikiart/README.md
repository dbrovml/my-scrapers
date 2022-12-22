# Wikiart scraper
This folder contains three scripts to be run in the following order:
1. `get_works_list.py` - scrapes the list of all artists presented at wikiart, visits each artist's detail page and scrapes the list of their works.
2. `get_works.py` - visits each item (artwork) url in the list and scrapes its metadata and .jpeg image.
3. `get_works_index.py` - cleans the scraped data, drops corrupted artworks, and collects all metadata into a single .csv file - index. This file connects each artwork's metadata to their image and can be used for slicing by genre, style, author, etc. <br>

As of Aug 2022, the resulting dataset contained **187 451** works by **3 536** authors in **61** genres.