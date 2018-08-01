import os

import numpy as np
import pandas as pd

from scopuscite.scopus import Scopus
from scopuscite.aggregate import aggregate_author_info
from scopuscite.utils import load_api_key

def write_author_to_csv(output_file, authors, 
                        cites_per_year=False, year_range=None):

    # if cites_per_year:
    #     for idx, year in enumerate(range(*year_range)):
    #         authors[str(year)] = authors['cites_by_year'].map(lambda x: x[idx])

    #     authors.drop(columns='cites_by_year', inplace=True)

    authors.to_csv(output_file, sep=';')

def download_journal_year_data(year, journal, issn, output_dir, params):
    '''
    Downloads the publications for all authors that have published in a given
    journal in a given year.

    Parameters
    ----------
    year : int
        Year of publishing.
    journal : str
        Journal name. Note that journal also matches substrings, i.e. ``Nature``
        will also find publications in ``Nature Physics``.
    issn : str (optional)
        ISSN of journal. Either journal or issn can be omitted.
    output_dir : str
        Where to save the downloaded files.
    '''
    
    # Construct output name
    if not os.path.isdir(output_dir):
        os.mkdir(output_dir)

    if 'operation_name' in params:
        operation_name = params['operation_name']
    else:
        operation_name = \
            (journal if journal is not None else issn) + '_' + year
        
    output_name = os.path.join(output_dir, operation_name)

    # Create Scopus object
    APIKEY = load_api_key()
    cache_name = params['cache_name'] if 'cache_name' in params else None
    cache_dir = params['cache_dir'] if 'cache_dir' in params else None
    scopus =  Scopus(APIKEY, cache_name=cache_name, cache_dir=cache_dir)
    
    # Download list of authors
    reload_author_list = params['reload_author_list'] \
                        if 'reload_author_list' in params else False
    author_ids = scopus.get_authors_from_journal_year(year, journal, issn,
                            force_reload=reload_author_list)
    if author_ids is None:
        print('Aborting download. No author_ids found.')
        return None
        
    # Get basic information about authors
    reload_author_info = params['reload_author_info'] \
                            if 'reload_author_info' in params else False
    authors = scopus.get_author_info(author_ids, reload_author_info)

    print('Saving author information to file.')
    authors.to_pickle(output_name+'_pubs.pkl')
    print('Exporting author information to csv.')
    write_author_to_csv(output_name + '_no_cites.csv', authors, 
                        cites_per_year=False)
    print('')

    print('Total authors: {}'.format(len(authors)))
    print('Total publications: {}'.format(authors['npubs'].sum()))
    print('')
    
    # Make selection by applying condition
    condition = params['condition'] if 'condition' in params else None
    if condition is not None:
        print('Apply selection to author list.')
        selection = authors.apply(condition, axis=1)
        authors = authors[selection]
        author_ids = authors.index

        print('Authors satisfying condition: {}'.format(len(authors)))
        print('Total publications of selection: {}' \
                .format(authors['npubs'].sum()))
        print('')

     # Get list of publications that need to be downloaded
    reload_author_pub = params['reload_author_pub'] \
                        if 'reload_author_pub' in params else False
    scopus_ids = scopus.get_author_publications(author_ids, 
                    force_reload=reload_author_pub)
    
    reload_pub_info = params['reload_pub_info'] \
                        if 'reload_pub_info' in params else False
    pubs = scopus.get_publication_info(scopus_ids, params['year_range'], 
            params['cite_type'], reload_pub_info)
    pubs.to_pickle(output_name+'_pubs.pkl')

    print('Aggregate cite-per-year info for authors.')
    authors = aggregate_author_info(authors, pubs, year_range=None)
    authors.to_pickle(output_name+'_auth.pkl')
    
    print('Export authors+cites to csv.')
    write_author_to_csv(output_name + '_export.csv', authors, \
                        cites_per_year=True, year_range=params['year_range'])

    return None