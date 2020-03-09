import yaml, fasttext
from elasticsearch import Elasticsearch
import logging

def logger_factory():
    return logging.getLogger(__name__)
    
def load_yaml_confg(yaml_file):
    with open(yaml_file) as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    return config

def connect_elasticserach(host, port):
    conn = Elasticsearch([{"host": host, "port": port}])
    return conn


def load_fasttext_model(path):
    classifier = fasttext.load_model(path)
    return classifier
