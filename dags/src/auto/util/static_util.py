import configparser
import os
import re
from pathlib import Path


def sort_array(arr):
    s = [item.replace("\'", " ").replace("\"", " ").strip() for item in arr]
    return sorted(list(set(s)))


def get_levels_up(up):
    current_path = Path(__file__).resolve()
    return current_path.parents[up]


def get_config(items):
    root_dir = get_levels_up(3)  # Project root
    config_path = os.path.join(root_dir, "conf.conf")

    config = configparser.ConfigParser()
    config.read(config_path)
    return config.items(items)


def extract_between_capitals(s):
    capitals = [match.start() for match in re.finditer(r'[A-Z]', s)]
    if len(capitals) >= 2:
        return s[capitals[0]:capitals[1]]
    else:
        return "?"

def to_camel(snake_str):
    parts = snake_str.strip('_').split('_')
    return ''.join(word.capitalize() for word in parts)

def convert_to_tuple(result):
    if not isinstance(result, tuple):
        return (result,)
    return result


spark_pandas_type_cast = {
    "timestamp": "datetime64[ns]",
    "int": "int32",
    "long": "int64",
    "float": "double",
    "double": "double",
    "string": "object",
    "boolean": "bool"
}
