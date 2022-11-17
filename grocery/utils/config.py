import os
import yaml
import grocery


def get_config():
    return yaml.safe_load(open(os.path.join(grocery.__path__[0], './config/develop.yml')))