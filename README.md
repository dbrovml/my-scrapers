# MY-SCRAPERS
This repo contains a couple of data scraping scripts to be used for machine learning and data science pet projects. <br>
Currently there are two scrapers:
- [www.wikiart.org]: ~ 180 000 images of artworks along with available metadata, e.g. author, genre, style, etc.
- [www.sreality.cz]: ~ 5 000 for sale local property profiles along with detailed apartment metadata and price.
The scraping approach consists of:
1. Finding a page containing the list of all pages we want to scrape
2. Scraping the list of urls from this page
3. Visiting each page on the list and scraping its contents