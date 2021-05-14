from ming.config import configure_from_nested_dict

def mongo_init(session_name, mongo_params):
    mongo_uri = 'mongodb://{username}:{password}@{host}/{database}'.format(**mongo_params)
    configure_from_nested_dict({session_name: {'uri': mongo_uri}})
