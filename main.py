import os

import pandas as pd

from scopuscite.download_data import \
    download_journal_year_data, write_author_to_csv

def published_before_year(row, first_pub):
    return row['first_pub'] <= first_pub

def download_math():
    params = {'operation_name' : None,
              'year_range' : (1960, 2019),
              'cite_type' : 'all',
              'condition' : lambda row : published_before_year(row, 1998),
              'cache_dir' : 'data/local_cache',
              'cache_name' : 'math_2016',
              'reload_author_list' : False,
              'reload_author_info' : False,
              'reload_author_pub' : False,
              'reload_pub_info' : False}

    params['operation_name'] = 'annals_2016'
    download_journal_year_data(2016, 'Annals of Mathematics', '0003486X', 
            output_dir='data/output', params=params)
    params['operation_name'] = 'duke_2016'
    download_journal_year_data(2016, 'Duke Mathematical Journal', '00127094', 
            output_dir='data/output', params=params)
    params['operation_name'] = 'inventiones_2016'
    download_journal_year_data(2016, 'Inventiones Mathematicae', '00209910', 
            output_dir='data/output', params=params)

    output_dir = 'data/output'
    math_names = ['annals_2016', 'duke_2016', 'inventiones_2016']

    # Join math author files
    math_author_list = []
    for math_journal in math_names:
        author_file = os.path.join(output_dir, math_journal + '_auth.pkl')
        math_author_list.append(pd.read_pickle(author_file))
    math_authors = pd.concat(math_author_list)
    math_authors = math_authors[~math_authors.index.duplicated()]

    output_name = os.path.join(output_dir, 'math_2016')
    math_authors.to_pickle(output_name + '_auth.pkl')
    write_author_to_csv(output_name + '_export.csv', math_authors, \
                        cites_per_year=True, year_range=params['year_range'])

    # Join math publication files
    math_pub_list = []
    for math_journal in math_names:
        pub_file = os.path.join(output_dir, math_journal + '_pubs.pkl')
        math_pub_list.append(pd.read_pickle(pub_file))
    math_pubs = pd.concat(math_pub_list)
    math_pubs = math_pubs[~math_pubs.index.duplicated()]

    output_name = os.path.join(output_dir, 'math_2016')
    math_pubs.to_pickle(output_name + '_pubs.pkl')

def download_pami():
    params = {'operation_name' : 'pami_2016',
              'year_range' : (1960, 2019),
              'cite_type' : 'all',
              'condition' : lambda row : published_before_year(row, 1998),
              'cache_dir' : 'data/local_cache',
              'cache_name' : 'pami_2016',
              'reload_author_list' : False,
              'reload_author_info' : False,
              'reload_author_pub' : False,
              'reload_pub_info' : False}

    download_journal_year_data(2016, 
        'IEEE Transactions on Pattern Analysis and Machine Intelligence', 
        '01628828', output_dir='data/output', params=params)

def download_physics():

    params = {'operation_name' : 'physics_2016',
              'year_range' : (1960, 2019),
              'cite_type' : 'all',
              'condition' : lambda row : published_before_year(row, 1998),
              'cache_dir' : 'data/local_cache',
              'cache_name' : 'physics_2016',
              'reload_author_list' : False,
              'reload_author_info' : False,
              'reload_author_pub' : False,
              'reload_pub_info' : False}

    download_journal_year_data(2016, 'Nature Physics', '17452473', 
        output_dir='data/output', params=params)

def download_biology():

    params = {'operation_name' : 'biology_2016',
              'year_range' : (1960, 2019),
              'cite_type' : 'all',
              'condition' : lambda row : published_before_year(row, 1998),
              'cache_dir' : 'data/local_cache',
              'cache_name' : 'biology_2016',
              'reload_author_list' : False,
              'reload_author_info' : False,
              'reload_author_pub' : False,
              'reload_pub_info' : False}

    download_journal_year_data(2016, 'PLoS Biology', '15449173', 
        output_dir='data/output', params=params)

def main():

    # download_math()

    download_pami()

    # download_physics()

    # download_biology()

if __name__ == '__main__':
    main()