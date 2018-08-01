import numpy as np
import pandas as pd

from scopuscite.utils import set_union
from scopuscite import utils

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

def aggregate_author_info(authors, pubs, year_range=None):
    '''
    Function aggregates author level citation information from data about
    individual publications.

    Parameters
    ----------
    authors : pandas.DataFrame
        Dataframe with author information. In fact only the index is needed to
        create the index of the return.
    pubs : pandas.DataFrame
        Dataframe with publication information

    Returns
    -------
    pandas.DataFrame
        Dataframe with same index as authors with computed columns
    '''
    
    if year_range is None:
        # Extract citation range from publication dataframe
        # We assume that all rows have same range
        start_year = pubs['cites_start_year'].iloc[0]
        end_year = start_year + len(pubs['cites_by_year'].iloc[0])
        year_range = range(start_year, end_year)

    # Organise publications by author
    author_pubs = {k : {} for k in authors.index}
    author_pubs.update({k : v for k, v in pubs_by_author(pubs).items() \
                            if k in author_pubs})


    res = pd.DataFrame(index=authors.index)

    # We will recompute these statistics
    authors = authors.drop(['npubs', 'first_pub', 'last_pub',
        'ncites', 'ncoauthors', 'hindex'], axis=1)

    # Recomupute scopus statistics using publication list
    # We don't have data to recompute ncited_by
    res['npubs'] = npubs(authors, author_pubs)
    res['first_pub'] = first_pub(authors, author_pubs, pubs)
    res['last_pub'] = last_pub(authors, author_pubs, pubs)
    res['ncites'] = ncites(authors, author_pubs, pubs)
    res['ncoauthors'] = ncoauthors(authors, author_pubs, pubs)
    res['hindex'] = hindex(authors, author_pubs, pubs)

    # Citations per year
    sum_cols = ['pcc', 'cites_by_year', 'lcc']
    for col in sum_cols:
        res[col] = authors.index.map( \
            lambda a : pubs[col].loc[author_pubs[a]].sum())

    
    res['pubs_by_year'] = pubs_by_year(authors, author_pubs, pubs, year_range)
    res['ncoauthors_mean'] = ncoauthors_mean(authors, author_pubs, pubs)
    res['ncoauthors_acc'] = ncoauthors_acc( \
        authors, author_pubs, pubs, year_range)

    # TODO: Accumulated citations per paper
    
    # print(res.columns)

    # Add results to authors dataframe
    res = authors.join(res)
    # res = authors

    return res

def npubs(authors, author_pubs):
    '''Counts number of publications per author.

    Parameters
    ----------
    authors : pandas.DataFrame
        Dataframe with author information.
    author_pubs : dict
        Dictionary with set of publication ids for each author.

    Returns
    -------
    pandas.Series
        Series indexed like authors.
    '''
    
    return authors.index.map(lambda a : len(author_pubs[a]))

def first_pub(authors, author_pubs, pubs):
    '''Year of first publication for an author

    Parameters
    ----------
    authors : pandas.DataFrame
        Dataframe with author information.
    author_pubs : dict
        Dictionary with set of publication ids for each author.
    pubs : pandas.DataFrame
        Dataframe with publication information.

    Returns
    -------
    pandas.Series
        Series indexed like authors
    '''

    get_first_pub = lambda a : pubs['year'].loc[author_pubs[a]].min()

    return authors.index.map(get_first_pub)

def last_pub(authors, author_pubs, pubs):
    '''Year of last publication for an author

    Parameters
    ----------
    authors : pandas.DataFrame
        Dataframe with author information.
    author_pubs : dict
        Dictionary with set of publication ids for each author.
    pubs : pandas.DataFrame
        Dataframe with publication information.

    Returns
    -------
    pandas.Series
        Series indexed like authors
    '''

    get_last_pub = lambda a : pubs['year'].loc[author_pubs[a]].max()

    return authors.index.map(get_last_pub)

def ncites(authors, author_pubs, pubs):
    '''Total number of citations for an author.

    Parameters
    ----------
    authors : pandas.DataFrame
        Dataframe with author information.
    author_pubs : dict
        Dictionary with set of publication ids for each author.
    pubs : pandas.DataFrame
        Dataframe with publication information.

    Returns
    -------
    pandas.Series
        Series indexed like authors
    '''

    return authors.index.map(
        lambda a : pubs['ncites'].loc[author_pubs[a]].sum() )

def ncoauthors(authors, author_pubs, pubs):
    '''Total number of coauthors for an author.

    Parameters
    ----------
    authors : pandas.DataFrame
        Dataframe with author information.
    author_pubs : dict
        Dictionary with set of publication ids for each author.
    pubs : pandas.DataFrame
        Dataframe with publication information.

    Returns
    -------
    pandas.Series
        Series indexed like authors
    '''

    return authors.index.map(
        lambda a : len(set_union(pubs['authors'].loc[author_pubs[a]])) - 1 )

def hindex(authors, author_pubs, pubs):
    '''H-index of an author.

    Parameters
    ----------
    authors : pandas.DataFrame
        Dataframe with author information.
    author_pubs : dict
        Dictionary with set of publication ids for each author.
    pubs : pandas.DataFrame
        Dataframe with publication information.

    Returns
    -------
    pandas.Series
        Series indexed like authors
    '''

    return authors.index.map(
        lambda a : utils.hindex(pubs['ncites'].loc[author_pubs[a]]) )

def ncoauthors_mean(authors, author_pubs, pubs):
    '''Average number of authors per paper

    Parameters
    ----------
    authors : pandas.DataFrame
        Dataframe with author information.
    author_pubs : dict
        Dictionary with set of publication ids for each author.
    pubs : pandas.DataFrame
        Dataframe with publication information.

    Returns
    -------
    pandas.Series
        Series indexed like authors
    '''

    return authors.index.map( \
        lambda a : (pubs['authors'].loc[author_pubs[a]].map(len)-1).mean() )

def ncoauthors_acc(authors, author_pubs, pubs, year_range):
    '''Number of accumulated coauthors until each year.

    Parameters
    ----------
    authors : pandas.DataFrame
        Dataframe with author information.
    author_pubs : dict
        Dictionary with set of publication ids for each author.
    pubs : pandas.DataFrame
        Dataframe with publication information.
    year_range : range

    Returns
    -------
    pandas.Series
        Series indexed like authors
    '''

    return authors.index.map( \
        lambda a : _ncoauthors_acc(pubs.loc[author_pubs[a]], year_range))

def _ncoauthors_acc(pubs, year_range):
    num_years = len(year_range)
    res = np.zeros((num_years,), dtype='int')

    coauthors = pubs['authors'] \
                    .groupby(pubs['year']) \
                    .agg(set_union) \
                    .reindex(year_range)

    # Fill NaNs with empty sets
    isnull = coauthors.isnull()
    coauthors[isnull] = [set(),] * isnull.sum()

    # Coauthors before selected years
    coauthors_now = set_union(pubs['authors'][pubs['year'] < year_range.start])

    # Coauthors during selected years
    for idx, year in enumerate(year_range):
        coauthors_now |= coauthors[year]
        res[idx] = len(coauthors_now)

    return res

def pubs_by_year(authors, author_pubs, pubs, year_range):
    '''Number of publications written each year.

    Parameters
    ----------
    authors : pandas.DataFrame
        Dataframe with author information.
    author_pubs : dict
        Dictionary with set of publication ids for each author.
    pubs : pandas.DataFrame
        Dataframe with publication information.
    year_range : range

    Returns
    -------
    pandas.Series
        Series indexed like authors
    '''

    return authors.index.map( \
        lambda a : _pubs_by_year(pubs.loc[author_pubs[a]], year_range))

def _pubs_by_year(pubs, year_range):
    num_years = len(year_range)
    res = np.zeros((num_years,), dtype='int')

    for _, year in pubs['year'].iteritems():
        if not year in year_range:
            continue
        res[year_range.index(year)] += 1

    return res