import configparser

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
        # Authentication with InstToken and Key from config file
        msg = ("config file misspecified. It must contain an "
               "Authentication section with two entries: APIKey "
               "and InstToken. Please correct.")
        raise ValueError(msg)

    return config['Authentication']['APIKey']