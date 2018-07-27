import pandas as pd

def pubs_by_author(pubs):
    '''
    Given a dataframe with publication information returns a dict of pairs
    (author_id, P) where P is the set of publications authored by author_id.

    Parameters
    ----------
    pubs : pandas.DataFrame
        Dataframe indexed by scopus_id

    Returns
    -------
    dict
        Dictionary indexed by author_id.
    '''
    
    author_pub = {}

    for scopus_id, authors in pubs['authors'].iteritems():
        for author in authors:
            if author in author_pub:
                author_pub[author].add(scopus_id)
            else:
                author_pub[author] = {scopus_id}

    return author_pub