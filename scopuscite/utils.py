import configparser

import numpy as np

def chunks(l, n):
    '''Yields successive n-sized chunks from list l.'''
    for i in range(0, len(l), n):
        yield l[i:i + n]

def eid_to_scopus_id(eid):
    '''
    Transforms an eid to a scopus_id
        eid = '2-s2.0-XXXXXX' and scopus_id='XXXXXX'
    '''
    return eid[7:]

def scopus_id_to_eid(scopus_id):
    '''
    Transforms a scopus_id to an eid
        eid = '2-s2.0-XXXXXX' and scopus_id='XXXXXX'
    '''
    return '2-s2.0-' + scopus_id

def valid_config(conf):
    '''Test if config contains necessary sections.'''
    return conf.has_option('Authentication', 'APIKey') \
            and conf.has_option('Authentication', 'InstToken')

def load_api_key():

    config = configparser.ConfigParser()
    config.read('.config')

    if (not config.has_section('Authentication')) or \
        (not valid_config(config)):
        # Authentication with Key from config file
        msg = ("config file misspecified. It must contain an "
               "Authentication section with entry APIKey.")
        raise ValueError(msg)

    return config['Authentication']['APIKey']

def set_union(sets):
    '''Computes the union of sets in an iterator.

    Parameters
    ----------
    sets : iterable
        Sets to compute union over (or types that can be cast to set)

    Returns
    -------
    set
        The union of all sets in ``sets``.

    Examples
    --------
    >>> set_union([{1, 2}, {1, 3}, {2, 3}])
    {1, 2, 3}
    '''

    union = set()
    for s in sets:
        union |= set(s)
    return union

def hindex(citations):
    '''Computes the hindex of a list of citations.

    Given an array of citations (each citation is a non-negative integer)
    of a researcher, write a function to compute the researcher's h-index.

    According to the definition of h-index on Wikipedia: "A scientist has 
    index h if h of his/her N papers have at least h citations each, and the 
    other N − h papers have no more than h citations each."

    Note: If there are several possible values for h, the maximum one is taken 
    as the h-index.

    Parameters
    ----------
    citations : list of int
        Number of citations for each papers
    
    Returns
    -------
    int
        h-index

    Examples
    --------
    >>> hindex([3, 0, 6, 1, 5])
    3
    '''

    n = len(citations);
    count = np.zeros((n+1,))
    for x in citations:
        # Put all x >= n in the same bucket.
        if x >= n:
            count[n] += 1
        else:
            count[x] += 1

    h = 0
    for i in reversed(range(0, n + 1)):
        h += count[i]
        if h >= i:
            return i
            
    return h