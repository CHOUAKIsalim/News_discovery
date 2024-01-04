# News_discovery

## 1. Analysis directory

This directory contains Jupyter Notebook files for in-depth analysis of discovered news pages. The files are as follows:

- 'Metrics of activity.ipynb': Analyzes creation dates and posting behaviors of the discovered news pages.
- 'Metrics of reputation.ipynb': Analyzes affiliations, verification statuses, and follower counts of discovered pages.
- 'Local news vs general news.ipynb': Identifies and analyzes the fraction of pages focused on local news (state or city level).
- 'News aggregators vs news creators.ipynb':  Identifies and analyzes the fraction of pages dedicated to news aggregation versus news creation.
- 'Validation of the method.ipynb': Analyzes the fraction of MBFC/NG pages that were discovered.

## 2. Data directory 
This directory contains two CSV files: 

- 'All_discovered_pages.csv': Complete list of discovered U.S.-based self-proclaimed news providers pages.
- 'Analyzed_pages.csv':  List of analyzed discovered U.S.-based self-proclaimed news providers pages (those detected at least 5 times).

## 3. Relevancy analysis directory
This directory contains randomly sampled discovered pages that underwent manual inspection to evaluate their relevancy.


## 4. Pages discovery directory

This directory contains a Python project for the news page discovery described in the paper.

- 'crawling_scripts': Contains scripts responsible for crawling from the GNews API, CrowdTangle, and the Facebook Ad Library.
- 'database_scripts': Contains scripts responsible for storing crawling results into a MySQL database. 
- 'params.py': Contains the CrowdTangle token used for discovery, directories to store results, and other global variables.
- `main.py`: The primary script for executing the news page discovery process. Accepts up to four optional arguments: language, country, start_date, and end_date for consideration in the discovery. Execute æs follow for discovering US-based pages posting in English.
```bash
$ python main.py en US
```
