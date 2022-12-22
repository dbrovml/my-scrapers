# MY-SCRAPERS
This repo contains a couple of data scraping scripts to be used for machine learning and data science pet projects. <br>
Currently there are two scrapers:
- [www.wikiart.org]: ~ 180 000 images of artworks along with available metadata, e.g. author, genre, style, etc.
- [www.sreality.cz]: ~ 5 500 for sale local property profiles along with detailed apartment metadata and price.<br>

The scraping approach taken:
1. Find a page containing the list of all pages we want to scrape
2. Scrape the list of item urls from this page
3. Visit each page on the list and scrape its contents