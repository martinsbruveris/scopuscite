scopuscite
==========

What is it?
-----------

A python library to query the Elsevier [Scopus API](https://dev.elsevier.com/).

Dependecies
-----------

The following python packages are required

* `pandas`
* `requests`
* `humanize`
  
See `requirements.txt` for details.

The code was tested with Python 3.6.6 on Ubuntu 18.04.

Usage
-----

### API key

To query Scopus you need your own API key. You can either pass the API key
directly when constructing the Scopus object:
```python
scopus_object = scopus.Scopus(apikey='xxxxx', ...)
```
or create a file `.config` in your working directory as follows:
```ini
[Authentication]
APIKey = xxxxxx
```
The key can then be loaded via `utils.load_api_key()`.

### Local caching

The library caches the `json` responses from scopus to avoid exhausting the API
limits set by Elsevier. The cache location is set when constructing the Scopus
object:
```python
scopus_object = scopus.Scopus(cache_name='xxx', cache_dir='some_dir')
```
This will save the Scopus responses in files
```
some_dir/xxx_author.pkl
some_dir/xxx_pub.pkl
```
and others. Different values for `cache_name` can be used to avoid files
becoming too big.

### Calling scopus

The basic usage of the library is as follows.

```python
from scopuscite.scopus import Scopus
from scopuscite.utils import load_api_key
from scopuscite.aggregate import aggregate_author_info

scopus = Scopus(apikey=load_api_key)

# Author ids for all authors who published in Annals of Mathematics in 2016.
author_ids = scopus.get_authors_from_journal_year(year=2016, 
    journal='Annals of Mathematics', issn='0003486X')

# Scopus profiles of these authors as a dataframe
authors = scopus.get_author_info(author_ids)

# Scopus ids of all their publications
scopus_ids = scopus.get_author_publications(author_ids)

# Citation information about these publications in for the years 1980-2019
pubs = scopus.get_publication_info(scopus_ids, (1980, 2019))

# Aggregate citation information for each author
authors_agg = aggregate_author_info(authors, pubs)
```
For more details see `example.ipynb`.

Licence
-------

This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with this program. If not, see http://www.gnu.org/licenses/.

Contacts
--------

Martins Bruveris  
Email: martins.bruveris@gmx.at  
Web: [www.brunel.ac.uk/~mastmmb](http://www.brunel.ac.uk/~mastmmb)
