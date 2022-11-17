import os
import yaml
import pgsrc


def get_config():
    return yaml.safe_load(open(os.path.join(pgsrc.__path__[0], './config/develop.yml')))