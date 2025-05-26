import json
from types import NoneType

from src.auto.util.enum_util import PropTypeEnum


def to_return(value):
    return value


def to_json(value):
    if isinstance(value, dict):
        return value
    return json.loads(value)


def to_str(value):
    if value is None:
        return None
    return str(value)


def to_int(value):
    if value is None:
        return None
    return int(value)


def to_bool(value):
    if value is None:
        return None
    return bool(value)


def to_float(value):
    if value is None:
        return None
    return float(value)


TypeConvert = {
    None: to_return,
    NoneType: to_return,
    PropTypeEnum.str: to_str,
    PropTypeEnum.int: to_int,
    PropTypeEnum.bool: to_bool,
    PropTypeEnum.float: to_float,
    PropTypeEnum.json: to_json,
    PropTypeEnum.array: to_return,
    PropTypeEnum.object: to_return,
}
