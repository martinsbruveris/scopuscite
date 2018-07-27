import math
import os, sys
import pickle

import numpy as np
import pandas as pd

import requests
from humanize import naturalsize

from scopus_utils import chunks, scopus_id_to_eid, eid_to_scopus_id

URI_SEARCH = 'https://api.elsevier.com/content/search/scopus'
URI_AUTHOR = 'https://api.elsevier.com/content/author'
URI_CITATION = 'https://api.elsevier.com/content/abstract/citations'
URI_ABSTRACT = 'https://api.elsevier.com/content/abstract/scopus_id/'

class Scopus(object):

    def __init__(self, apikey, cache_name=None, cache_dir=None):
        self.CACHE_DIR_DEFAULT = 'local_cache'
        self.CACHE_NAME_DEFAULT = 'cache'
        self.CACHE_AUTHOR_PUB_SUFFIX = '_author_pub.pkl'
        self.CACHE_PUB_INFO_SUFFIX = '_pub.pkl'
        self.CACHE_AUTHOR_INFO_SUFFIX = '_author.pkl'
        self.CACHE_SEARCH_QUERY_NAME = 'cache_search_query.pkl'

        self.apikey = apikey
        self.cache_name = cache_name if cache_name is not None else \
                            self.CACHE_NAME_DEFAULT
        self.cache_dir = cache_dir if cache_dir is not None else \
                            self.CACHE_DIR_DEFAULT

        # Check if cache directory exists
        if not os.path.isdir(self.cache_dir):
            os.mkdir(self.cache_dir)

    def load_search_query_cache(self):
        """
        Loads the cache containing author ids from search queries

        Output:
            Function saves cache in self.cache_search_query
        """

        filename = os.path.join(self.cache_dir, self.CACHE_SEARCH_QUERY_NAME)
        if os.path.exists(filename):
            with open(filename, 'rb') as fp:
                self.cache_search_query = pickle.load(fp)
        else:
            self.cache_search_query = {}

    def save_search_query_cache(self):
        '''
        Saves the cache in self.cache_search_query to file.
        '''

        filename = os.path.join(self.cache_dir, self.CACHE_SEARCH_QUERY_NAME)
        with open(filename, 'wb') as fp:
                pickle.dump(self.cache_search_query, fp)   

    def load_author_pub_cache(self):
        '''
        Loads the cache containing the list of publication eids for a given
        author id.

        Output:
            Function saves cache in self.cache_author_pub
        '''

        filename = os.path.join(self.cache_dir, 
            self.cache_name + self.CACHE_AUTHOR_PUB_SUFFIX)
        if os.path.exists(filename):
            with open(filename, 'rb') as fp:
                self.cache_author_pub = pickle.load(fp)
        else:
            self.cache_author_pub = {}

    def save_author_pub_cache(self):
        '''
        Saves the cache in self.cache_author_pub to file.
        '''

        filename = os.path.join(self.cache_dir, 
            self.cache_name + self.CACHE_AUTHOR_PUB_SUFFIX)
        with open(filename, 'wb') as fp:
                pickle.dump(self.cache_author_pub, fp)

    def load_pub_info_cache(self):
        '''
        Loads the cache containing publication information.

        Output:
            Function saves cache in self.cache_pub_info
        '''

        filename = os.path.join(self.cache_dir, 
            self.cache_name + self.CACHE_PUB_INFO_SUFFIX)
        if os.path.exists(filename):
            with open(filename, 'rb') as fp:
                self.cache_pub_info = pickle.load(fp)
        else:
            self.cache_pub_info = {}

    def save_pub_info_cache(self):
        '''
        Saves the cache in self.cache_pub_info to file.
        '''

        filename = os.path.join(self.cache_dir, 
            self.cache_name + self.CACHE_PUB_INFO_SUFFIX)
        with open(filename, 'wb') as fp:
                pickle.dump(self.cache_pub_info, fp, pickle.HIGHEST_PROTOCOL)

    def load_author_info_cache(self):
        '''
        Loads the cache containing author information.

        Output:
            Function saves cache in self.cache_author_info
        '''

        filename = os.path.join(self.cache_dir, 
            self.cache_name + self.CACHE_AUTHOR_INFO_SUFFIX)
        if os.path.exists(filename):
            with open(filename, 'rb') as fp:
                self.cache_author_info = pickle.load(fp)
        else:
            self.cache_author_info = {}

    def save_author_info_cache(self):
        '''
        Saves the cache in self.cache_author_info to file.
        '''

        filename = os.path.join(self.cache_dir, 
            self.cache_name + self.CACHE_AUTHOR_INFO_SUFFIX)
        with open(filename, 'wb') as fp:
                pickle.dump(self.cache_author_info, fp)

    def call_api(self, url, params):
        for _ in range(10):
            r = requests.get(url, params=params)

            if r.status_code == 200:
                js = r.json()
                return js
            elif not r.status_code in {503, 504}:
                print(r)
                print(r.headers)
                return None

        print('Number of consecutive failed to Scopus exceeds 10.')
        print(r)
        print(r.headers)
        return None

    def check_api_response(self, r, js):
        '''
        Checks the response from scopus for errors

        Input
        r       Object returned by the requests library
        js      Parsed json object

        Output
        code     0 Something went wrong
                 1 Ressource not found
                 2 No problems
        '''
        if 'service-error' in js:
            if 'status' in js['service-error'] and \
                'statusCode' in js['service-error']['status'] and \
                js['service-error']['status']['statusCode'] \
                    == 'RESOURCE_NOT_FOUND':
                return 1
                
            print('Something went wrong when calling Scopus API.')
            print('Last response headers.')
            print(r.headers)
            return 0
        return 2

    def get_authors_from_journal_year(self, year, journal=None, issn=None,
                                    force_reload=False):
        """Retrieves author ids for a given journal and year.

        The function retrieves from Scopus the author ids of all authors that
        have published in a given journal and a given year.

        Parameters
        ----------
        year : int
            Year of publication.
        journal : str
            Name of journal.
        issn : str
            Issn of journal.
            
            It is enough to provide only one of ``journal`` and ``issn``, but
            since ``journal`` also finds partial matches, e.g. `Nature` will
            also match `Nature Physics`, using ``issn`` is recommended.
        force_reload : bool, optional
            If ``True`` then function ignores the local cache and queries 
            Scopus.

        Returns
        -------
        set
            Set of author ids.
        """

        print('Querying Scopus to retrieve list of authors.')

        search_query = 'PUBYEAR+IS+' + str(year)
        if journal is not None:
            search_query += ' AND SRCTITLE(' + journal + ')'
        if issn is not None:
            search_query += ' AND ISSN(' + issn + ')'

        self.load_search_query_cache()

        if not force_reload and search_query in self.cache_search_query:
            authors = self.cache_search_query[search_query]
            print('Authors retrieved from cache.')
        else:
            
            par = {'apikey': self.apikey, 
                'query': search_query,
                'httpAccept': 'application/json',
                'field': 'eid,author',
                'count': 200,
                'start': 0}

            r = requests.get(URI_SEARCH, params=par)
            js = r.json()
            
            num_results = int(js['search-results']['opensearch:totalResults'])
            retrieved = 0

            authors = set()
            while retrieved < num_results:

                entries = js['search-results']['entry']
                for entry in entries:
                    if 'author' in entry:
                        authors |= { a['authid'] for a in entry['author'] }

                par['start'] += len(entries)
                retrieved += len(entries)

                if retrieved >= num_results:
                    break

                r = requests.get(URI_SEARCH, params=par)
                js = r.json()

            print('Api calls remaining: {} / {}' \
                    .format(r.headers['X-RateLimit-Remaining'],
                            r.headers['X-RateLimit-Limit']))

            # Save result to cache                
            self.cache_search_query[search_query] = authors            
            self.save_search_query_cache()

        print('Authors found: {}'.format(len(authors)))
        print('')

        return authors

    def get_single_author_publications(self, author_id):
        '''
        Retrieves the scopus_ids of publications of a single author

        Input
        author_id       paper to query

        Output
        scopus_ids      set of scopus_ids
        '''

        scopus_ids = set()
        
        par = {'apikey': self.apikey, 
            'httpAccept': 'application/json',
            'query': 'AU-ID(' + author_id + ')',
            'field': 'eid,author',
            'count': 200,
            'start': 0}

        js = self.call_api(URI_SEARCH, params=par)
        if js is None:
            return None
        
        num_results = int(js['search-results']['opensearch:totalResults'])
        retrieved = 0
    
        while retrieved < num_results:
            entries = js['search-results']['entry']
            
            # Need to update this before manipulating entries
            par['start'] += len(entries)
            retrieved += len(entries)

            # Some entries don't have an eid entry...
            entries = [ entry for entry in entries if 'eid' in entry ]

            # Add to list of things to be donwloaded
            scopus_ids |= {eid_to_scopus_id(entry['eid']) \
                                for entry in entries}

            if retrieved >= num_results:
                break

            js = self.call_api(URI_SEARCH, params=par)
            if js is None:
                return None
        
        return scopus_ids
            

    def get_author_publications(self, author_ids, force_reload=False):
        '''
        Retrieves set of scopus_ids with all publications from given author ids.

        Input:
        author_ids      List of author ids to be queried.
        force_reload    If True cache is ignored.

        Output:
        scopus_ids      Set of eids with all publications from the authors.
        '''

        print('Querying Scopus to retrieve list of publication scopus ids.')
        print('Number of authors: {}'.format(len(author_ids)))

        author_ids = list(author_ids)
        author_ids_new = []
        scopus_ids = set()

        print('Loading cache.')
        self.load_author_pub_cache()

        # Start by retrieving cached authors
        if not force_reload:
            num_read_cache = 0
            for author_id in author_ids:
                if author_id in self.cache_author_pub:
                    scopus_ids |= self.cache_author_pub[author_id]
    
                    num_read_cache += 1
                    if num_read_cache % 100 == 0:
                        print('Read from cache: {}'.format(num_read_cache))
                else:
                    author_ids_new.append(author_id)
            print('Read from cache: {}'.format(num_read_cache))
        else:
            print('Ignoring cache, reloading all results.')
            author_ids_new = author_ids
        
        chunk_size = 10 # Limit set by Scopus API
        num_chunks = math.ceil(len(author_ids_new) / chunk_size)

        # Papers with more than 100 authors
        num_many_authors = 0
        
        print('Authors to query Scopus: {}'.format(len(author_ids_new)))
        r = None
        for idx, chunk in enumerate(chunks(author_ids_new, chunk_size)):
            search_query = ' OR '.join(['AU-ID(' + au_id + ')' \
                                            for au_id in chunk])

            par = {'apikey': self.apikey, 
                'query': search_query,
                'httpAccept': 'application/json',
                'field': 'eid,author',
                'count': 200,
                'start': 0}

            js = self.call_api(URI_SEARCH, params=par)
            if js is None:
                return
            
            num_results = int(js['search-results']['opensearch:totalResults'])
            retrieved = 0
            
            print('Chunk {} / {}: {} results found' \
                    .format(idx+1, num_chunks, num_results))

            # This dict will be added to the cache
            author_pub = {author_id : set() for author_id in chunk}
            exists_long_author_list = False
            
            while retrieved < num_results:
                entries = js['search-results']['entry']
                
                # Need to update this before manipulating entries
                par['start'] += len(entries)
                retrieved += len(entries)

                # Some entries don't have an eid entry...
                entries = [ entry for entry in entries if 'eid' in entry ]

                # Add to list of things to be donwloaded
                scopus_ids |= {eid_to_scopus_id(entry['eid']) \
                                    for entry in entries}

                # Add entries to author_pub dict
                for entry in entries:
                    current_id = eid_to_scopus_id(entry['eid'])
                    if 'message' in entry and 'truncated' in entry['message']:
                        exists_long_author_list = True
                        num_many_authors += 1
                    elif not 'author' in entry:
                        continue
                    else:
                        authors = {a['authid'] for a in entry['author']}

                    for a in authors & set(chunk):
                        author_pub[a].add(current_id)

                if retrieved >= num_results:
                    break

                js = self.call_api(URI_SEARCH, params=par)
                if js is None:
                    return

            # If some papers have more than 100 authors, we need to query the
            # chunk one-by-one
            if exists_long_author_list:
                print('Query authors one-by-one.')
                for a in chunk:
                    pubs = self.get_single_author_publications(a)
                    if pubs is not None:
                        author_pub[a] = pubs

            # Update cache
            self.cache_author_pub.update(author_pub)
            self.save_author_pub_cache()

        # Save cache (just to be sure)
        self.save_author_pub_cache()

        if r is not None:
            print('Api calls remaining: {} / {}' \
                    .format(r.headers['X-RateLimit-Remaining'],
                            r.headers['X-RateLimit-Limit']))
        print('Publications found: {}'.format(len(scopus_ids)))
        print('Publications with more than 100 authors: {}' \
                    .format(num_many_authors))
        print('')
        
        return scopus_ids

    def decode_cite_info(self, cite_info, start_year, cite_type):
        '''
        Extracts information about a publication and its citation profile from 
        the json object returned by the scopus api.

        Input:
        cite_info       Dict containing the parsed json.
        start_year      When does citation data begin.
        cite_type       'all', 'exclude-self' or 'exclude-books'

        Output
        info            Dict with the collected information.
        '''

        info = {}
        
        # scopus_id = 'SCOPUS_ID:xxx'
        scopus_id = cite_info['dc:identifier']
        info['scopus_id'] = scopus_id[10:]
        
        info['title'] = cite_info['dc:title'] if 'dc:title' in cite_info else ''
        info['journal'] = cite_info['prism:publicationName'] \
                            if 'prism:publicationName' in cite_info else ''
        info['year'] = int(cite_info['sort-year']) \
                            if 'sort-year' in cite_info else 0
        
        if 'author' in cite_info and isinstance(cite_info['author'], list):
            info['authors'] = [author['authid'] \
                                for author in cite_info['author']]
        else:
            info['authors'] = []

        cite_names = {'all' : 'cites_by_year',
                      'exclude-self' : 'cites_by_year_excl_self',
                      'exclude-books' : 'cites_by_year_excl_books' }
        col_name = cite_names[cite_type]
        if 'cc' in cite_info and isinstance(cite_info['cc'], list):
            info[col_name] = \
                np.array([ int(x['$']) for x in cite_info['cc'] ])
        else:
            info[col_name] = np.ndarray((0,))
            
        info['pcc'] = int(cite_info['pcc']) if 'pcc' in cite_info else 0
        info['lcc'] = int(cite_info['lcc']) if 'lcc' in cite_info else 0
        info['cites_start_year'] = start_year
        info['ncites'] = sum(info['cites_by_year']) + info['pcc'] + info['lcc']
        
        return info

    def get_publication_info(self, scopus_ids, year_range, cite_type='all',
                             force_reload=False):
        '''
        Retrieves detailed information about publications with given scopus ids
        from Scopus and collects information in a dataframe.

        Input
        scopus_ids      List of scopus ids to be queried.
        year_range      Tuple (start, end) of years with per-year citation info.
                        Following python convention we return citation data for
                        the years
                            start, start+1, ..., end-1
        cite_type       'all', 'exclude-self', 'exclude-books'
        force_reload    If True, cache is ignored

        Output
        pubs            Dataframe with the information
        '''

        print('Retrieving publication info for {} ids.'.format(len(scopus_ids)))
        
        scopus_id_list = list(scopus_ids)
        scopus_id_list_new = []
        pubs_list = []
        
        # Load cache file
        print('Loading cache file.')
        self.load_pub_info_cache()
        print('Cache size: {}' \
                .format(naturalsize(sys.getsizeof(self.cache_pub_info, 0))))
        
        # Load publications from cache
        if not force_reload:
            num_read_cache = 0
            for scopus_id in scopus_id_list:
                cache_key = (scopus_id, year_range, cite_type)
                if cache_key in self.cache_pub_info:
                    entry = self.cache_pub_info[cache_key]
                    pub = self.decode_cite_info(entry, year_range[0], cite_type)
                    pubs_list.append(pub)
                    
                    num_read_cache += 1
                    if num_read_cache % 10000 == 0:
                        print('Read from cache: {}'.format(num_read_cache))
                else:
                    scopus_id_list_new.append(scopus_id)
            print('Total read from cache: {}'.format(num_read_cache))
        else:
            scopus_id_list_new = scopus_id_list
            print('Ignoring cache, reloading all info.')
        
        par = {'apikey': self.apikey, 
            'scopus_id': '',
            'httpAccept':'application/json', 
            'date': '%i-%i' % (year_range[0], year_range[1]-1),
            'count' : '25',
            'view' : 'STANDARD'}
        # Default is all citation, no parameter needed in this case
        if cite_type in {'exclude-self', 'exclude-books'}:
            par['citation'] = cite_type
        
        print('To be retrieved from Scopus: {}'.format(len(scopus_id_list_new)))
        
        chunk_size = 25 # Limit set by Scopus API
        num_chunks = math.ceil(len(scopus_id_list_new) / chunk_size)
        
        r = None
        res_not_found = 0
        for idx, chunk in enumerate(chunks(scopus_id_list_new, chunk_size)):
            if (idx+1) % 20 == 0:
                print('Chunk {} / {}.'.format(idx+1, num_chunks))
            par['scopus_id'] = ','.join(chunk)
            
            r = requests.get(URI_CITATION, params=par)
            js = r.json()
            
            # Something went wrong
            if 'service-error' in js:
                if 'status' in js['service-error'] and \
                    'statusCode' in js['service-error']['status'] and \
                    js['service-error']['status']['statusCode'] \
                        == 'RESOURCE_NOT_FOUND':
                    res_not_found += 1
                    continue
                    
                print('Something went wrong when calling Scopus API.')
                print('Last response headers.')
                print(r.headers)
                break
            
            cite_info = js['abstract-citations-response'] \
                        ['citeInfoMatrix']['citeInfoMatrixXML'] \
                        ['citationMatrix']['citeInfo']
            
            for entry in cite_info:
                # Save result to cache
                scopus_id = entry['dc:identifier'][10:]
                cache_key = (scopus_id, year_range, cite_type)
                self.cache_pub_info[cache_key] = entry
                
                # Decode result
                pub = self.decode_cite_info(entry, year_range[0], cite_type)
                pubs_list.append(pub)
                
            if (idx+1) % 200 == 0:
                print('Saving cache file.')
                self.save_pub_info_cache()
        
        # Save cache file
        print('Saving cache file.')
        self.save_pub_info_cache()
        
        if res_not_found > 0:
            print('Ressources not found: {}.'.format(res_not_found))
        
        if r is not None:
            print('{} / {} api calls remaining.' \
                    .format(r.headers['X-RateLimit-Remaining'],
                            r.headers['X-RateLimit-Limit']))
        else:
            print('Scopus api was not called.')
        
        pubs = pd.DataFrame(pubs_list).set_index('scopus_id')
        print('Publication info retrieved.')
        print('')
    
        return pubs

    def decode_author_response(self, author):
        '''
        Extracts information about an author from the json object returned 
        by the scopus api.

        Input:
        author      Dict containing the parsed json.
        
        Output
        info        Dict with the collected information.
        '''

        # The author id has changed. We ignore this for now.
        if 'alias' in author['author-profile'] and \
            author['author-profile']['alias']['@current-status'] \
                == 'tombstone':
            return None
        
        info = {}

        coredata = author['coredata']
        info['author_id'] = coredata['dc:identifier'][10:]
        info['npubs'] = coredata['document-count']
        info['ncites'] = coredata['citation-count']
        info['ncited_by'] = coredata['cited-by-count']

        author_profile = author['author-profile']
        info['name'] = author_profile['preferred-name']['indexed-name']
        info['first_name'] = author_profile['preferred-name']['given-name']
        info['last_name'] = author_profile['preferred-name']['surname']
        info['first_pub'] = author_profile['publication-range']['@start']
        info['last_pub'] = author_profile['publication-range']['@end']
        
        tmp_1 = author['author-profile']
        if 'affiliation-current' in tmp_1:
            tmp_2 = tmp_1['affiliation-current']['affiliation']
            if isinstance(tmp_2, list):
                tmp_2 = tmp_2[0]
            tmp_3 = tmp_2['ip-doc']
            if 'afdispname' in tmp_3:
                info['affiliation'] = tmp_3['afdispname']
            else:
                info['affiliation'] = ''
        else:
            info['affiliation'] = ''
        
        info['ncoauthors'] = author['coauthor-count']
        info['hindex'] = author['h-index']

        return info

    def get_author_info(self, author_ids, force_reload=False):
        '''
        Retrieves detailed information about authors with given author ids from
        Scopus and collects information in a dataframe.

        Input
        author_ids      List of eids to be queried.
        force_reload    If True, cache is ignored

        Output
        authors         Dataframe with the information
        '''

        print('Retrieving info for {} authors.'.format(len(author_ids)))

        author_id_list = list(author_ids)
        author_id_list_new = []
        author_list = []

        # Load cache file
        print('Loading cache file.')
        self.load_author_info_cache()
        print('Cache size: {}' \
                .format(naturalsize(sys.getsizeof(self.cache_author_info, 0))))
        
        # Load those that have already been cached
        if not force_reload:
            num_read_cache = 0
            for author_id in author_id_list:
                if author_id in self.cache_author_info:
                    entry = self.cache_author_info[author_id]
                    author = self.decode_author_response(entry)
                    if author is not None:
                        author_list.append(author)
                        
                    num_read_cache += 1
                    if num_read_cache % 100 == 0:
                        print('Read from cache: {}'.format(num_read_cache))
                else:
                    author_id_list_new.append(author_id)
            print('Read from cache: {}'.format(num_read_cache))
        else:
            author_id_list_new = author_id_list
            print('Ignoring cache, reloading all data.')
        
        par = {'apikey': self.apikey, 
               'author_id' : '',
               'httpAccept' : 'application/json',
               'view' : 'ENHANCED'}
        r = None

        print('To be read from Scopus: {}'.format(len(author_id_list_new)))

        chunk_size = 25 # Limit set by Scopus API
        num_chunks = math.ceil(len(author_id_list_new) / chunk_size)
        
        for idx, chunk in enumerate(chunks(author_id_list_new, chunk_size)):
            print('Chunk {} / {}.'.format(idx+1, num_chunks))
            par['author_id'] = ','.join(chunk)
            
            r = requests.get(URI_AUTHOR, params=par)
            js = r.json()
            
            # Something went wrong
            if 'service-error' in js:
                print('Something went wrong when calling Scopus API.')
                print('Last response headers.')
                print(r.headers)
                break
            
            response_list = js['author-retrieval-response-list'] \
                              ['author-retrieval-response']
                
            for entry in response_list:
                # Save result to cache
                author_id = entry['coredata']['dc:identifier'][10:]
                self.cache_author_info[author_id] = entry
    
                # Decode result
                author = self.decode_author_response(entry)
                if author is not None:
                    author_list.append(author)

            if (idx+1) % 20 == 0:
                print('Saving cache file.')
                self.save_author_info_cache()

        print('Saving cache file.')
        self.save_author_info_cache()

        if r is not None:
            print('Api call remaining: {} / {}' \
                    .format(r.headers['X-RateLimit-Remaining'],
                            r.headers['X-RateLimit-Limit']))
        else:
            print('Scopus api was not called.')
        
        authors = pd.DataFrame(author_list)
        
        int_cols = ['npubs', 'ncites', 'ncited_by', 'ncoauthors',
                    'hindex', 'first_pub', 'last_pub']
        for col in int_cols:
            authors[col] = pd.to_numeric(authors[col], errors='coerce') \
                            .fillna(0).astype(np.int64)
        
        authors.set_index('author_id', inplace=True)

        authors = authors.reindex(['name', 'first_name', 'last_name', 
            'affiliation', 'first_pub', 'last_pub', 'npubs', 'ncites', 
            'ncited_by', 'ncoauthors', 'hindex', 'pcc', 'lcc', 
            'cites_by_year'], axis=1)

        print('Author info retrieved.')
        print('')
            
        return authors