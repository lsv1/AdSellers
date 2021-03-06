# AdSellers

## Description

An ads.txt scraper which uses sellers.json as the source for domain scraping.

The eventual goal is to generate static HTML files with visualizations of ads.txt usage and SSP distribution across domains.

## Status

Still under development.

### Language

Python 3.8

### Dependencies

* Pandas
* Requests
* BeautifulSoup4
* Multiprocess
* tqdm
* sqlalchemy
* sqlite

### Instructions

* Clone the repository.
* Install any missing dependencies to your system or venv.
* Run main.py

As of 2021-01-17 this will only populate the database with all ads.txt entries.

## Authors

Luis Sastre Verzun

## Shout outs

* Peter Brendan (https://sellersjsons.com/)
* Lei Mao (https://leimao.github.io/blog/Python-tqdm-Multiprocessing/)

## License

This project is licensed under the The MIT License (MIT) - see the LICENSE.md file for details.