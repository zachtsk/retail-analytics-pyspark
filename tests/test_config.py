import pgsrc
import os
import yaml


def test_config_import():
    config_name = 'develop.yml'
    try:
        yaml.safe_load(open(os.path.join(pgsrc.__path__[0], f'./config/{config_name}')))
    except:
        print(f"Could not import config [{config_name}]")
