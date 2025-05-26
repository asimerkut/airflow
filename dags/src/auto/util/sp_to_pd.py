import base64
import hashlib
import json
import math
import os
import re
import time
import uuid
import zlib
from collections import OrderedDict
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_EVEN
from io import StringIO

import Levenshtein
import numpy as np
import pandas as pd
import xxhash
from sklearn.preprocessing import LabelEncoder

df_current = None


# NORMAL FUNC #
def col(column_name):
    return df_current[column_name]


def column(column_name):
    return df_current[column_name]


def lit(value):
    return value


def broadcast():
    raise Exception('Not implemented!')


def coalesce(column, default):
    return df_current[column].fillna(default)


def input_file_name(file_path):
    return os.path.basename(file_path)


def isnan(column_name):
    return df_current[column_name].isna()


def isnull(column_name):
    return df_current[column_name].isnull()


def monotonically_increasing_id():
    return range(len(df_current))


def nanvl(column_name, column_name_2):
    return df_current[column_name].combine_first(df_current[column_name_2])


def rand(range):
    return np.random.rand(range)


def spark_partition_id():
    raise Exception('Not implemented!')


def randn(range):
    return np.random.randn(range)


def when(condition, value, otherwise):
    return np.where(condition, value, otherwise)


def bitwise_not(column_name):
    return ~df_current[column_name]


def bitwiseNOT(column_name):
    return ~df_current[column_name]


def expr(str):
    return df_current.eval(str)


def greatest(cols):
    return df_current[cols].max(axis=1)


def least(cols):
    return df_current[cols].min(axis=1)


# MATH FUNC #

def sqrt(column_name):
    return np.sqrt(df_current[column_name])


def abs(column_name):
    return df_current[column_name].abs()


def acos(column_name):
    return np.arccos(df_current[column_name])


def acosh(column_name):
    return np.arccosh(df_current[column_name])


def asin(column_name):
    return np.arcsin(df_current[column_name])


def asinh(column_name):
    return np.arcsinh(df_current[column_name])


def atan(column_name):
    return np.arctan(df_current[column_name])


def atanh(column_name):
    return np.arctanh(df_current[column_name])


def atan2(column_name, column_name_2):
    return np.arctan2(df_current[column_name], df_current[column_name_2])


def bin_func(column_name):
    return df_current[column_name].apply(lambda x: bin(x))


def cbrt(column_name):
    return np.cbrt(df_current[column_name])


def ceil(column_name):
    return np.ceil(df_current[column_name])


def conv(column_name, fbase, tbase):
    def convert_number(x):
        try:
            decimal_number = int(x, fbase)
            if tbase == 10:
                return decimal_number
            elif tbase == 2:
                return format(decimal_number, 'b')
            elif tbase == 8:
                return format(decimal_number, 'o')
            elif tbase == 16:
                return format(decimal_number, 'X')
            else:
                return f"Unsupported target base: {tbase}"
        except ValueError:
            return f"Invalid number: {x}"

    return df_current[column_name].apply(convert_number)


def cos(column_name):
    return np.cos(df_current[column_name])


def cosh(column_name):
    return np.cosh(df_current[column_name])


def cot(column_name):
    return 1 / np.tan(df_current[column_name])


def csc(column_name):
    return 1 / np.sin(df_current[column_name])


def exp(column_name):
    return df_current[column_name].apply(np.exp)
    # return df_current[column_name].exp()


def expm1(column_name):
    return df_current[column_name].apply(np.expm1)
    # return df_current[column_name].expm1()


def factorial(column_name):
    return df_current[column_name].apply(math.factorial)


def floor(column_name):
    return df_current[column_name].floordiv(1)


def to_hex(column_name):
    return df_current[column_name].apply(hex)


def unhex(column_name):
    # return df_current[column_name].apply(lambda x: np.int64(x))
    return df_current[column_name].apply(int, base=16)


def hypot(column_name, column_name_2):
    return np.hypot(df_current[column_name], df_current[column_name_2])


def log(column_name, value=None):
    # return df_current[column_name].apply(lambda v: np.log(value))
    if value is None:
        return df_current[column_name].apply(lambda x: math.log(x))
    else:
        return df_current[column_name].apply(lambda x: math.log(x, value))


def log10(column_name):
    return df_current[column_name].apply(np.log10)


def log1p(column_name):
    return df_current[column_name].apply(np.log1p)


def log2(column_name):
    return df_current[column_name].apply(np.log2)


def pmod(p1, p2):
    if type(p1) == str and type(p2) == str:
        return np.mod(df_current[p1], df_current[p2])
    elif type(p1) == str and type(p2) in [int, float]:
        return np.mod(df_current[p1], p2)
    elif type(p1) in [int, float] and type(p2) in [int, float]:
        new_column = str(uuid.uuid4())
        df_current[new_column] = np.mod(p1, p2)
        return df_current[new_column]
    else:
        raise Exception('Mod alınamaz.')


def pow(p1, p2):
    if type(p1) == str and type(p2) == str:
        return df_current[p1] ** df_current[p2]
    elif type(p1) == str and type(p2) in [int, float]:
        return df_current[p1] ** p2
    elif type(p1) in [int, float] and type(p2) in [int, float]:
        new_column = str(uuid.uuid4())
        df_current[new_column] = p1 ** p2
        return df_current[new_column]
    else:
        raise Exception('Kuvvet alınamaz.')

    # return df_current[column_name] ** df_current[column_name_2]


def rint(column_name):
    return df_current[column_name].apply(np.rint)


def round(column_name, scale):
    return df_current[column_name].round(scale)


def bround(column_name, scale):
    '''
    https://www.scaler.com/topics/round-function-in-python/
    '''

    # return df_current[column_name].apply(lambda x: decimal.Decimal(x).quantize(decimal.Decimal(scale), rounding=decimal.ROUND_HALF_EVEN))
    def halfevenround(row, scale):
        # If scale is non-negative, round to the specified number of decimal places
        if scale >= 0:
            return Decimal(str(row)).quantize(Decimal('1e-{0}'.format(scale)), rounding=ROUND_HALF_EVEN)
        # If scale is negative, round to the nearest integer at the integral part
        else:
            return Decimal(str(row)).quantize(Decimal('1'), rounding=ROUND_HALF_EVEN)

    return df_current[column_name].apply(lambda x: halfevenround(x, scale))


def sec(column_name):
    return 1 / np.cos(df_current[column_name])


def shiftleft(column_name, numBitsint):
    # return df_current[column_name].shift(numBitsint)
    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    # https://linuxhint.com/pyspark-shiftleft-functions/
    # shiftleft() Function
    # It is available in the pyspark.sql.functions module. The shiftleft() function shifts the bits to the left.
    # It is evaluated to the formula, value*(2^shift).
    return df_current[column_name].apply(lambda x: x * (2 * numBitsint))


def shiftright(column_name, numBitsint):
    # return df_current[column_name].shift(numBitsint)
    # shiftright() Function
    # It is available in the pyspark.sql.functions module.The shiftright() function shifts the bits to the right.
    # It is evaluated to the formula, value / (2 ^ shift)
    return df_current[column_name].apply(lambda x: (x / (2 * numBitsint))).floordiv(1)


def shiftrightunsigned(col, numbits):
    return df_current[col].apply(lambda x: (x % (1 << 64)) >> numbits)


def signum(column_name):
    return np.sign(df_current[column_name])


def sin(column_name):
    return np.sin(df_current[column_name])


def sinh(column_name):
    return np.sinh(df_current[column_name])


def tan(column_name):
    return np.tan(df_current[column_name])


def tanh(column_name):
    return np.tanh(df_current[column_name])


def toDegrees(column_name):
    return np.degrees(df_current[column_name])


def degrees(column_name):
    return np.degrees(df_current[column_name])


def toRadians(column_name):
    return np.radians(df_current[column_name])


def radians(column_name):
    return np.radians(df_current[column_name])


# DATETIME FUNC #

def add_months(column_name, _months):
    # return pd.to_datetime(df_current[column_name]) + relativedelta(months=_months)
    # return pd.to_datetime(df_current[column_name]) + pd.DateOffset(months=_months)

    if column_name in df_current.columns:
        return pd.to_datetime(df_current[column_name]) + pd.DateOffset(months=_months)
    elif re.search(r"^\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])$", column_name):
        return pd.to_datetime(column_name) + pd.DateOffset(months=_months)
    else:
        raise ValueError("Invalid column_name or date")


def current_date():
    return datetime.now().strftime("%Y-%m-%d")
    # return pd.to_datetime(datetime.today()).date().strftime("%Y-%m-%d")


def current_timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")


def date_add(column_name, _days):
    # return pd.to_datetime(df_current[column_name]) + pd.DateOffset(days=_days)
    if column_name in df_current.columns:
        return pd.to_datetime(df_current[column_name]) + pd.DateOffset(days=_days)
    elif re.search(r"^\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])$", column_name):
        return pd.to_datetime(column_name) + pd.DateOffset(days=_days)
    else:
        raise ValueError("Invalid column_name or date")


def date_format(column_name, format):
    # return pd.to_datetime(df_current[column_name]).dt.strftime(format)

    if column_name in df_current.columns:
        return pd.to_datetime(df_current[column_name]).dt.strftime(format)
    elif re.search(r"^\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])$", column_name):
        return pd.to_datetime(column_name).dt.strftime(format)
    else:
        raise ValueError("Invalid column_name or date")


def date_sub(column_name, _days):
    # return pd.to_datetime(df_current[column_name]) - pd.DateOffset(days=_days)
    if column_name in df_current.columns:
        return pd.to_datetime(df_current[column_name]) - pd.DateOffset(days=_days)
    elif re.search(r"^\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])$", column_name):
        return pd.to_datetime(column_name) - pd.DateOffset(days=_days)
    else:
        raise ValueError("Invalid column_name or date")


def date_trunc(format, column_name):
    formats = {
        'year': '%Y-01-01',
        'yyyy': '%Y-01-01',
        'yy': '%y-01-01',
        'month': '%Y-%m-01',
        'mon': '%Y-%b-01',
        'mm': '%Y-%m-01',
        'day': '%Y-%m-%d',
        'dd': '%Y-%m-%d',
        'microsecond': '%Y-%m-%d %H:%M:%S.%f',
        'millisecond': '%Y-%m-%d %H:%M:%S.%f',
        'second': '%Y-%m-%d %H:%M:%S',
        'minute': '%Y-%m-%d %H:%M',
        'hour': '%Y-%m-%d %H:00:00',
        'week': '%Y-%U-1',
        'quarter': '%Y-Q1-01'
    }

    if format not in formats:
        raise ValueError("Invalid format parameter")

    format_string = formats[format]
    if column_name in df_current.columns:
        truncated_date = pd.to_datetime(df_current[column_name]).apply(lambda x: x.strftime(format_string))
    elif re.search(r"^\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])$", column_name):
        truncated_date = pd.to_datetime(column_name).strftime(format_string)
    else:
        raise ValueError("Invalid column_name or date")

    return pd.to_datetime(truncated_date)


def datediff(d1, d2):
    new_column = str(uuid.uuid4())

    if isinstance(d1, datetime):
        d1 = d1.strftime('%Y-%m-%d')

    if isinstance(d2, datetime):
        d2 = d2.strftime('%Y-%m-%d')

    if d1 in df_current.columns and d2 in df_current.columns:
        df_current[new_column] = (pd.to_datetime(df_current[d1]) - pd.to_datetime(df_current[d2])).dt.days
    elif d1 in df_current.columns and re.search(r"^\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])$", d2):
        df_current[new_column] = (pd.to_datetime(df_current[d1]) - pd.to_datetime(d2)).dt.days
    elif d2 in df_current.columns and re.search(r"^\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])$", d1):
        df_current[new_column] = (pd.to_datetime(d1) - pd.to_datetime(df_current[d2])).dt.days
    elif re.search(r"^\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])$", d1) and re.search(
            r"^\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])$", d2):

        df_current[new_column] = (pd.to_datetime(d1) - pd.to_datetime(d2)).dt.days
    else:
        raise Exception('Invalid date or column.')

    return df_current[new_column]
    # return (pd.to_datetime(df_current[column_name]) - pd.to_datetime(df_current[column_name_2])).dt.days
    # return (df_current[column_name] - df_current[column_name_2]).dt.days


def dayofmonth(column_name):
    if column_name in df_current.columns:
        return pd.to_datetime(df_current[column_name]).dt.day
    elif re.search(r"^\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])$", column_name):
        return pd.to_datetime(column_name).dt.day
    else:
        raise Exception('Invalid date or column.')


def dayofweek(column_name):  # Sparkın sonucunu baz alacak şekilde düzeltildi ihtiyaca göre yeniden gözden geçirilmeli
    # SPARK
    # pyspark.sql.functions.dayofweek
    # pyspark.sql.functions.dayofweek(col: ColumnOrName) → pyspark.sql.column.Column[source]
    # Extract the day of the week of a given date/timestamp as integer. Ranges from 1 for a Sunday through to 7 for a Saturday

    # PANDAS
    # pandas.Series.dt.dayofweek
    # Series.dt.dayofweek[source]
    # The day of the week with Monday=0, Sunday=6.
    if column_name in df_current.columns:
        return pd.to_datetime(df_current[column_name]).dt.dayofweek + 2
    elif re.search(r"^\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])$", column_name):
        return pd.to_datetime(column_name).dt.dayofweek + 2
    else:
        raise Exception('Invalid date or column.')


def dayofyear(column_name):
    if column_name in df_current.columns:
        return pd.to_datetime(df_current[column_name]).dt.dayofyear
    elif re.search(r"^\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])$", column_name):
        return pd.to_datetime(column_name).dt.dayofyear
    else:
        raise Exception('Invalid date or column.')


def weekofyear(column_name):
    if column_name in df_current.columns:
        return pd.to_datetime(df_current[column_name]).dt.isocalendar().week
    elif re.search(r"^\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])$", column_name):
        return pd.to_datetime(column_name).dt.isocalendar().week
    else:
        raise Exception('Invalid date or column.')


def second(column_name):
    if column_name in df_current.columns:
        return pd.to_datetime(df_current[column_name]).dt.second
    elif re.search(r"^\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])$", column_name):
        return pd.to_datetime(column_name).dt.second
    else:
        raise Exception('Invalid date or column.')


def year(column_name):
    if column_name in df_current.columns:
        return pd.to_datetime(df_current[column_name]).dt.year
    elif re.search(r"^\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])$", column_name):
        return pd.to_datetime(column_name).dt.year
    else:
        raise Exception('Invalid date or column.')


def quarter(column_name):
    if column_name in df_current.columns:
        return pd.to_datetime(df_current[column_name]).dt.quarter
    elif re.search(r"^\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])$", column_name):
        return pd.to_datetime(column_name).dt.quarter
    else:
        raise Exception('Invalid date or column.')


def month(column_name):
    if column_name in df_current.columns:
        return pd.to_datetime(df_current[column_name]).dt.month
    elif re.search(r"^\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])$", column_name):
        return pd.to_datetime(column_name).dt.month
    else:
        raise Exception('Invalid date or column.')


def last_day(column_name):
    if column_name in df_current.columns:
        return pd.to_datetime(df_current[column_name]) + pd.offsets.MonthEnd(0)
    elif re.search(r"^\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])$", column_name):
        return pd.to_datetime(column_name) + pd.offsets.MonthEnd(0)
    else:
        raise Exception('Invalid date or column.')


def localtimestamp():
    # return pd.to_datetime('now').strftime('%d.%m.%Y %H:%M:%S.%f')
    return pd.Timestamp.now().strftime('%d.%m.%Y %H:%M:%S.%f')


def minute(column_name):
    if column_name in df_current.columns:
        return pd.to_datetime(df_current[column_name]).dt.minute
    elif re.search(r"^\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])$", column_name):
        return pd.to_datetime(column_name).dt.minute
    else:
        raise Exception('Invalid date or column.')


def months_between(d1, d2):
    if isinstance(d1, datetime):
        d1 = d1.strftime('%Y-%m-%d')

    if isinstance(d2, datetime):
        d2 = d2.strftime('%Y-%m-%d')

    if d1 in df_current.columns and d2 in df_current.columns:
        return (pd.to_datetime(df_current[d1]).dt.to_period('M').astype('int64') -
                pd.to_datetime(df_current[d2]).dt.to_period('M').astype('int64'))
    elif d1 in df_current.columns and re.search(r"^\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])$", d2):
        return (pd.to_datetime(df_current[d1]).dt.to_period('M').astype('int64') -
                pd.to_datetime(d2).dt.to_period('M').astype('int64'))
    elif d2 in df_current.columns and re.search(r"^\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])$", d1):
        return (pd.to_datetime(d1).dt.to_period('M').astype('int64') -
                pd.to_datetime(df_current[d2]).dt.to_period('M').astype('int64'))
    elif re.search(r"^\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])$", d1) and re.search(
            r"^\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])$", d2):
        return (pd.to_datetime(d1).dt.to_period('M').astype('int64') -
                pd.to_datetime(d2).dt.to_period('M').astype('int64'))
    else:
        raise Exception('Invalid date or column.')


def next_day(column_name, dayOfweek):
    dowmap = {"Mon": 0, "Tue": 1, "Wed": 2, "Thu": 3, "Fri": 4, "Sat": 5, "Sun": 6}

    if dayOfweek not in dowmap.keys():
        raise Exception('Invalid Day of Week!')
    else:
        nextday = dowmap[dayOfweek]

    def get_next_day(row, nd):
        daydiff = (nd - row.weekday() + 7) % 7
        return row + timedelta(days=daydiff)

    if column_name in df_current.columns:
        return pd.to_datetime(df_current[column_name]).apply(lambda x: get_next_day(x, nextday))
    elif re.search(r"^\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])$", column_name):
        return pd.to_datetime(column_name).apply(lambda x: get_next_day(x, nextday))
    else:
        raise Exception('Invalid date or column.')


def hour(column_name):
    return pd.to_datetime(df_current[column_name]).dt.hour


def make_date(year, month, day):  # bunun df e uygulanması anlamsız geldi.
    # def make_date_transform(year, month, day):
    # return pd.to_datetime(f"{year}-{month}-{day}")

    # return df_current.apply(lambda row: make_date_transform(year, month, day), axis=1)
    return pd.to_datetime(f"{year}-{month}-{day}")


def from_unixtime(column_name, format):
    def from_unixtime_transform(unix_time):
        return datetime.fromtimestamp(unix_time).strftime(format)

    if column_name in df_current.columns:
        return df_current[column_name].apply(from_unixtime_transform)
    elif isinstance(column_name, 'int'):
        return column_name.apply(from_unixtime_transform)
    else:
        raise Exception('Invalid unixtime or column.')


def unix_timestamp(column_name):
    # return pd.to_datetime(df_current[column_name]).astype(int) // 10**9 # 10**9 ifadesi, nanosaniyeleri saniyelere donustur
    # return (df_current[column_name] - pd.Timestamp("1970-01-01")) // pd.Timedelta('1s')
    def convert_unixtime(row):
        return time.mktime(pd.to_datetime(row).timetuple())

    if column_name in df_current.columns:
        return df_current[column_name].apply(convert_unixtime)
    elif re.search(r"^\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])$", column_name):
        return time.mktime(pd.to_datetime(column_name).timetuple())
    else:
        raise Exception('Invalid date or column.')


def to_timestamp(column_name, format):
    return pd.to_datetime(df_current[column_name], format=format)


def to_date(column_name, format):
    def format_date(date_str):
        return datetime.strptime(date_str, format)

    # apply() fonksiyonunu kullanarak tarihi belirli bir formata dönüştürerek yeni bir sütun oluşturma
    return df_current[column_name].apply(format_date)
    # return pd.to_datetime(pd.to_datetime(df_current[column_name]).dt.strftime(format))
    # return pd.to_datetime(df_current[column_name]).dt.date


def trunc(column_name, format):
    formats = {
        'year': '%Y-01-01',
        'yyyy': '%Y-01-01',
        'yy': '%y-01-01',
        'month': '%Y-%m-01',
        'mon': '%Y-%b-01',
        'mm': '%Y-%m-01',
        'day': '%Y-%m-%d',
        'dd': '%Y-%m-%d',
        'week': '%Y-%U-1',
        'quarter': '%Y-Q1-01'
    }

    if format not in formats:
        raise ValueError("Invalid format parameter")

    format_string = formats[format]
    if column_name in df_current.columns:
        truncated_date = pd.to_datetime(df_current[column_name]).apply(lambda x: x.strftime(format_string))
    elif re.search(r"^\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])$", column_name):
        truncated_date = pd.to_datetime(column_name).strftime(format_string)
    else:
        raise ValueError("Invalid column_name or date")

    return pd.to_datetime(truncated_date)


def from_utc_timestamp(column_name, tz):
    def from_utc_timestamp_transform(timestamp, timezone):
        return pd.Timestamp(timestamp, tz=timezone)

    return df_current[column_name].apply(from_utc_timestamp_transform, args=(tz,))


def to_utc_timestamp(timestamp, tz):
    raise Exception('Not implemented!')


def window():
    raise Exception('Not implemented!')


def session_window():
    raise Exception('Not implemented!')


def timestamp_seconds(column_name):
    def from_unixtime_transform(unix_time):
        return datetime.fromtimestamp(unix_time)

    if column_name in df_current.columns:
        return df_current[column_name].apply(from_unixtime_transform)
    elif isinstance(column_name, 'int'):
        return column_name.apply(from_unixtime_transform)
    else:
        raise Exception('Invalid unixtime or column.')


def window_time():
    raise Exception('Not implemented!')


# COLLECTION FUNC #

def array(cols):
    return df_current[cols].apply(lambda row: list(row), axis=1)


def array_contains(column_name, value):
    def check_col_is_null_and_contains(col_value, value):
        if col_value is None:
            return None
        else:
            return value in col_value

    return df_current[column_name].apply(lambda x: check_col_is_null_and_contains(x, value))


def arrays_overlap(column_name, column_name_2):
    # return df_current.apply(lambda row: np.intersect1d(row[column_name], row[column_name_2]).size > 0, axis=1)
    return bool(set(df_current[column_name]) & set(df_current[column_name_2]))


def array_join(column_name, delimiter):
    def joinx(x):
        if isinstance(x, list):
            return delimiter.join(str(i) for i in x if i is not None)
        else:
            return x

    return df_current[column_name].apply(lambda x: joinx(x))


def create_map(cols):
    return df_current.apply(lambda row: {row[cols[0]]: row[cols[1]]}, axis=1)


def slice(column_name, start, length):
    def slice_col(row, start):
        if start > 0:
            return row[start - 1: start - 1 + length]
        else:
            return row[start: start + length]

    return df_current[column_name].apply(lambda x: slice_col(x, start))


def concat(cols):
    # return pd.concat([df_current[col] for col in cols], axis=1)
    # return df_current[cols].agg(''.join, axis=1)
    def safe_concat(row):
        if any(value is None for value in row):
            return None
        else:
            return ''.join(map(str, row))

    return df_current[cols].apply(safe_concat, axis=1)


def array_position(column_name, value):
    def find_position(row, value):
        if value in row:
            return row.index(value) + 1
        else:
            return -1

    return df_current[column_name].apply(find_position, value=value)


def element_at(column_name, extraction):
    def get_element(row, index):
        if np.abs(index) < len(row):
            if index > 0:
                return row[index - 1]
            else:
                return row[index]
        else:
            return None

    return df_current[column_name].apply(get_element, index=extraction)


"""
def array_append(column_name, value):
    def append_value(row, value):
        if isinstance(value, pd.Series):  # Check if the value is a column
            row.extend(value.tolist())
        else:
            row.append(value)
        return row

    if value in df_current.columns:
        return df_current[column_name].apply(lambda x: append_value(x,df_current[value]))
    else:
        return df_current[column_name].apply(append_value, value=value)
"""


def array_append(column_name, value):
    def append_value(row, value):
        if isinstance(value, pd.Series):  # Check if the value is a column
            return row + value.tolist()  # Append values row by row
        else:
            return row + [value]  # Append a single value

    if value in df_current.columns:
        return df_current.apply(lambda row: append_value(row[column_name], row[value]), axis=1)
    else:
        return df_current[column_name].apply(append_value, value=value)


def array_sort(column_name):
    def sort_array(row):
        def custom_sort_key(x):
            if x is None:
                return (float('inf'), str(x))
            elif isinstance(x, (int, float)):
                return (x, str(x))
            else:
                return (float('-inf'), str(x))

        return sorted(row, key=custom_sort_key)

    return df_current[column_name].apply(sort_array)


def array_insert(column_name, value, index):
    def insert_value(row, value, index):
        if index - len(row) <= 0:
            row.insert(index - 1, value)
        else:
            lenr = len(row)
            subv = index - lenr
            i = 0
            # print("lenr:%",lenr)
            # print("subv:%", subv)
            while i < subv - 1:
                row.insert(lenr + i, None)
                i = i + 1

            row.insert(index - 1, value)
        return row

    return df_current[column_name].apply(insert_value, value=value, index=index)


def array_remove(column_name, value):
    def remove_value(row, value):
        if value in row:
            row.remove(value)
        return row

    return df_current[column_name].apply(remove_value, value=value)


def array_distinct(column_name):
    def get_unique_values(row):
        seen = set()
        result = []
        for value in row:
            if value not in seen:
                seen.add(value)
                result.append(value)
        return result

    return df_current[column_name].apply(get_unique_values)


def array_intersect(column_name, column_name_2):
    def get_common_values(row):
        return list(set(row[column_name]) & set(row[column_name_2]))

    return df_current.apply(get_common_values, axis=1)


def array_union(column_name, column_name_2):
    def get_union(row):
        # return list(set(row[column_name]).union(row[column_name_2]))
        union_set = OrderedDict.fromkeys(row[column_name])
        union_set.update(OrderedDict.fromkeys(row[column_name_2]))
        return list(union_set)

    return df_current.apply(get_union, axis=1)


def array_except(column_name, column_name_2):
    def get_difference(row):
        return list(set(row[column_name]).difference(row[column_name_2]))

    return df_current.apply(get_difference, axis=1)


def array_compact(column_name):
    def remove_none_values(row):
        return [value for value in row[column_name] if value is not None]

    return df_current.apply(remove_none_values, axis=1)


def transform(column_name, custom_operation):
    # return df_current[column_name].transform(custom_operation)
    # return df_current[column_name].apply(lambda x: eval(custom_operation))
    # return df_current[column_name].apply(lambda x: [eval(custom_operation) for i in x])
    def eval_expr(lst):
        return (eval(custom_operation) for x in lst)

    return df_current[column_name].apply(lambda x: eval_expr(x))


def exists(column_name, condition):
    def check_condition(lst):
        return any(eval(condition) for x in lst)

    return df_current[column_name].apply(check_condition)


def forall(column_name, condition):
    # def check_condition(column, condition):
    #     return df_current[column].apply(lambda x: [eval(condition) for i in x])
    # return check_condition(column_name, condition).all()
    def check_condition(lst):
        return all(eval(condition) for x in lst)

    return df_current[column_name].apply(check_condition)


def filter(column_name, condition):
    def check_condition(x):
        return x[eval(condition)]
        # return df_current[column].apply(lambda x: eval(condition) if x)

    return df_current[column_name].apply(check_condition)


def aggregate():
    raise Exception('Not implemented!')


def zip_with():
    raise Exception('Not implemented!')


def transform_keys():
    raise Exception('Not implemented!')


def transform_values():
    raise Exception('Not implemented!')


def map_filter():
    raise Exception('Not implemented!')


def map_from_arrays(column_name, column_name_2):
    # return df_current[column_name].map(dict(zip(df_current[column_name], df_current[column_name_2])))
    def map_from_arrays_func(row):
        arr = row[column_name]
        arr2 = row[column_name_2]
        mapping = dict(zip(arr, arr2))
        return [mapping.get(item, item) for item in arr]

    return df_current[[column_name, column_name_2]].apply(map_from_arrays_func, axis=1)


def map_zip_with():
    raise Exception('Not implemented!')


def explode(column_name):
    return df_current.explode(column_name)


def explode_outer(column_name):
    return df_current.explode(column_name)


def posexplode(column_name):
    exploded_df = df_current.copy()

    # Explode the array column
    exploded_df[column_name] = exploded_df[column_name].apply(lambda x: [(i, v) for i, v in enumerate(x)])

    # Explode the list of tuples into separate rows
    exploded_df = exploded_df.explode(column_name)

    # Extract the position and value into separate columns
    exploded_df[['pos', 'col']] = pd.DataFrame(exploded_df[column_name].tolist(), index=exploded_df.index)

    # Drop the original array column
    exploded_df.drop(column_name, axis=1, inplace=True)

    return exploded_df


def posexplode_outer():
    raise Exception('Not implemented!')


def inline():
    raise Exception('Not implemented!')


def inline_outer():
    raise Exception('Not implemented!')


def get(column_name, index):
    # return df_current[column_name].get(index)
    # def get_func(lst):
    # return eval(lst)[index]

    # return df_current[column_name].apply(get_func)

    return df_current[column_name].apply(lambda a: a[index] if isinstance(a, list) and len(a) > index else None)


def get_json_object(column_name, path):
    raise Exception('Not implemented!')


def json_tuple(column_name, fields):
    def get_json_fields(row):
        nonlocal fields
        json_data = json.loads(row)
        return [json_data.get(field) for field in fields]

    return df_current[column_name].apply(get_json_fields)


def from_json(column_name, schema):
    df_current[column_name] = df_current[column_name].apply(json.loads)
    json_df = pd.json_normalize(df_current[column_name])
    for column, dtype in schema.items():
        if column in json_df.columns:
            json_df[column] = json_df[column].astype(dtype)
    return pd.concat([df_current, json_df], axis=1).drop(columns=[column_name])


def schema_of_json(column_name):
    schema = {}
    df_current[column_name] = df_current[column_name].apply(json.loads)
    for record in df_current[column_name]:
        for key, value in record.items():
            if key not in schema:
                if isinstance(value, int):
                    schema[key] = int
                elif isinstance(value, float):
                    schema[key] = float
                elif isinstance(value, str):
                    schema[key] = str
                elif isinstance(value, bool):
                    schema[key] = bool
                elif isinstance(value, (list, tuple)):
                    if all(isinstance(item, int) for item in value):
                        schema[key] = list[int]
                    elif all(isinstance(item, float) for item in value):
                        schema[key] = list[float]
                    elif all(isinstance(item, str) for item in value):
                        schema[key] = list[str]
                    elif all(isinstance(item, bool) for item in value):
                        schema[key] = list[bool]
                elif value is None:
                    schema[key] = type(None)

    return schema


def to_json(column_name):
    return df_current[column_name].to_json(orient='records')


def size(column_name):
    def get_element_length(element):
        if ('[' in element) & (']' in element):
            return len(eval(element))

    return df_current[column_name].apply(get_element_length)


def struct():
    raise Exception('Not implemented!')


def sort_array(column_name, ascending=True):
    def custom_sort_key(x):
        if x is None:
            return (float('inf'), str(x))
        elif isinstance(x, (int, float)):
            return (x, str(x))
        else:
            return (float('-inf'), str(x))

    return df_current[column_name].apply(lambda x: sorted(x, key=custom_sort_key, reverse=not ascending))


def array_max(column_name):
    def find_max(arr):
        if isinstance(arr, list):
            return np.max(arr)
        return np.nan

    return df_current[column_name].apply(find_max)


def array_min(column_name):
    def find_max(arr):
        if isinstance(arr, list):
            return np.min(arr)
        return np.nan

    return df_current[column_name].apply(find_max)


def shuffle(column_name):
    return df_current[column_name].apply(lambda x: np.random.permutation(x).tolist() if isinstance(x, list) else x)


def reverse(column_name):
    return df_current[column_name].apply(lambda arr: arr[::-1])


def flatten(column_name):
    def flatten_func(column):

        if isinstance(column, list):
            return np.concatenate(column).tolist()
        elif isinstance(column, str):
            return np.concatenate(eval(column)).tolist()
        elif pd.isna(column):
            return None
        else:
            raise Exception('Flaten operasyonu için uygun veri seti bulunmuyor')
        # return column

    return df_current[column_name].apply(flatten_func)
    # return df_current[column_name].apply(lambda x: None if pd.isna(x) else list(chain(*x)))


def sequence():
    raise Exception('Not implemented!')


def array_repeat(column_name, count):
    # return np.repeat(df_current[column_name].values, count)
    def repeat_list(lst, repetitions):
        return lst * repetitions

    return df_current[column_name].apply(repeat_list, repetitions=count)


def map_contains_key(column_name, value):
    # return df_current[column_name].apply(lambda x: value in x.keys())
    return df_current[column_name].apply(lambda x: value in x)


def map_keys(column_name):
    # return df_current[column_name].apply(lambda x: list(x.keys()))
    return df_current[column_name].apply(lambda x: list(json.loads(x).keys()))


def map_values(column_name):
    return df_current[column_name].apply(lambda x: list(json.loads(x).values()))


def map_entries(column_name):
    return df_current[column_name].apply(lambda x: list(json.loads(x).items()))


def map_from_entries(column_name):
    return df_current[column_name].apply(lambda x: str(dict(json.loads(x))))


def arrays_zip():
    raise Exception('Not implemented!')


def map_concat(cols):
    return df_current[cols[0]].astype(str) + df_current[cols[1]].astype(str)


def from_csv():
    raise Exception('Not implemented!')


def schema_of_csv(column_name):
    def infer_csv_schema(csv_string):
        # Read CSV string into a pandas DataFrame
        df = pd.read_csv(StringIO(csv_string))

        # Create a dictionary to store column names and their corresponding data types
        column_types = {col: str(df[col].dtype) for col in df.columns}

        # Generate DDL schema
        ddl_schema = []
        for col, dtype in column_types.items():
            ddl_schema.append(f"{col} {dtype}")

        return ', '.join(ddl_schema)

    return df_current[column_name].apply(infer_csv_schema)


def to_csv(column_name):
    def struct_to_csv(row):
        if not isinstance(row, tuple):
            raise ValueError("Unsupported type: Not a nested structure.")
        return ','.join(map(str, row))

    return df_current[column_name].apply(struct_to_csv)


# PARTITION TRANSFORM FUNC #
def years():
    raise Exception('Not implemented!')


def months():
    raise Exception('Not implemented!')


def days():
    raise Exception('Not implemented!')


def hours():
    raise Exception('Not implemented!')


def bucket():
    raise Exception('Not implemented!')


# AGGREGATE FUNC #

def approx_count_distinct(column_name):
    return df_current[column_name].nunique()


def approxCountDistinct(column_name):
    return df_current[column_name].nunique()


def avg(column_name):
    return df_current[column_name].mean()


def collect_list(column_name):
    # pyspark.sql.functions.collect_list(col: ColumnOrName) → pyspark.sql.column.Column[source]
    # Aggregate function: returns a list of objects with duplicates.
    # (cols, column_name):
    # return df_current.groupby(cols).agg({column_name: list})
    return df_current[column_name].agg(list)


def collect_set(column_name):
    # pyspark.sql.functions.collect_set(col: ColumnOrName) → pyspark.sql.column.Column[source]
    # Aggregate function: returns a set of objects with duplicate elements eliminated.
    # (cols, column_name):
    # return df_current.groupby(cols).agg({column_name: set})
    return df_current[column_name].drop_duplicates().agg(list)


def corr(column_name, column_name_2):
    return df_current[[column_name, column_name_2]].corr()


def count(column_name):
    return df_current[column_name].count()


def count_distinct(column_name):
    return df_current[column_name].nunique()


def countDistinct(column_name):
    return df_current[column_name].nunique()


def covar_pop(column_name, column_name_2):
    return df_current[[column_name, column_name_2]].cov()


def covar_samp(column_name, column_name_2):
    return df_current[[column_name, column_name_2]].cov()


def first(column_name):
    # return df_current[column_name].first()

    def first_transform(column_name, offset=0):
        return df_current[column_name].iat[offset]

    return pd.Series(df_current.index).apply(lambda x: first_transform(column_name, x))


def grouping(cols):
    return df_current.groupby(cols).ngroup().astype('int')


def kurtosis(column_name):
    return df_current[column_name].kurtosis()


def last(column_name):
    # return df_current[column_name].last()
    def last_transform(column_name, offset=0):
        return df_current[column_name].iat[-1 - offset]

    return pd.Series(df_current.index).apply(lambda x: last_transform(column_name, x))


def max(column_name):
    return df_current[column_name].max()


def max_by(column_name, ord):
    return df_current.groupby(column_name)[ord].idxmax()


def mean(column_name):
    return df_current[column_name].mean()


def median(column_name):
    return df_current[column_name].median()


def min(column_name):
    return df_current[column_name].min()


def mode(column_name):
    return df_current[column_name].mode()


def percentile_approx(column_name, percentile):
    return np.percentile(df_current[column_name], percentile)


def product(column_name):
    return df_current[column_name].product()


def skewness(column_name):
    return df_current[column_name].skew()


def stddev(column_name):
    return df_current[column_name].std()


def stddev_pop(column_name):
    return df_current[column_name].std(ddof=0)


def stddev_samp(column_name):
    return df_current[column_name].std(ddof=1)


def sum(column_name):
    return df_current[column_name].sum()


def sum_distinct(column_name):
    return df_current[column_name].drop_duplicates().sum()


def sumDistinct(column_name):
    return df_current[column_name].drop_duplicates().sum()


def var_pop(column_name):
    return df_current[column_name].var()


def var_samp(column_name):
    return df_current[column_name].var(ddof=1)


def variance(column_name):
    return df_current[column_name].var()


# WINDOW FUNC #
def cume_dist():
    return df_current.apply(lambda x: x.rank(method='max', ascending=True) / x.count())


def dense_rank():
    def calculate_dense_rank(x):
        return x.rank(method='dense', ascending=True)

    return df_current.apply(calculate_dense_rank)


def lag(sort_by, group_by, lag, offset=1):  # arti yonde
    return df_current.sort_values(by=[sort_by], ascending=True).groupby(group_by)[lag].shift(offset)
    # return df_current[column_name].shift(offset)


def lead(column_name, offset=-1):  # eksi yonde
    return df_current[column_name].shift(offset)


def nth_value(column_name, offset, ignoreNulls):
    raise Exception('Not implemented!')


def ntile(n):
    def ntile(arr, n):
        return np.ceil(arr.rank(pct=True) * n)

    return df_current.apply(lambda x: ntile(x, n))


def percent_rank():
    def percent_rank(arr):
        return (arr.rank(method='min') - 1) / (len(arr) - 1)

    return df_current.apply(percent_rank)


def rank():
    return df_current.rank(ascending=False)


def row_number():
    return df_current.reset_index().index + 1


# SORT FUNC #
def asc(column_name):
    return df_current.sort_values(column_name, ascending=True)


def asc_nulls_first(column_name):
    return df_current.sort_values(column_name, ascending=True, na_position='first')


def asc_nulls_last(column_name):
    return df_current.sort_values(column_name, ascending=True, na_position='last')


def desc(column_name):
    return df_current.sort_values(column_name, ascending=False)


def desc_nulls_first(column_name):
    return df_current.sort_values(column_name, ascending=False, na_position='first')


def desc_nulls_last(column_name):
    return df_current.sort_values(column_name, ascending=False, na_position='last')


# STRING FUNC #
def ascii(column_name):
    def get_ascii(c):
        return ' '.join(str(ord(i)) for i in c)

    # apply() fonksiyonuyla her bir elemana uygulayarak ASCII karşılıklarını alalım
    return df_current[column_name].apply(get_ascii)


def base64_func(column_name):
    def encode_string(x):
        return base64.b64encode(x.encode('utf-8')).decode('utf-8')

    return df_current[column_name].apply(encode_string)


def bit_length(column_name):
    return df_current[column_name].apply(lambda x: x.bit_length())


def concat_ws(sep, cols):
    return df_current[cols].apply(lambda row: sep.join(row.values.astype(str)), axis=1)


def decode(column_name, charset):
    return df_current[column_name].apply(lambda x: x.decode(charset))


def encode(column_name, charset):
    return df_current[column_name].apply(lambda x: x.encode(charset))


def format_number(column_name, d):
    return df_current[column_name].apply(lambda x: format(float(x), f'.{d}f' if isinstance(x, (int, float)) else x))


def format_string(f, cols):
    return df_current[cols].apply(lambda x: f.format(**x))


def initcap(column_name):
    return df_current[column_name].apply(lambda x: x.title())


def lower(column_name):
    return df_current[column_name].str.lower()


def levenshtein(column_name, column_name_2):
    def levenshtein_distance(s1, s2):
        return Levenshtein.distance(s1, s2)

    return df_current.apply(lambda x: levenshtein_distance(x[column_name], x[column_name_2]), axis=1)


def locate(substr, column_name):
    # return df_current[column_name].str.index(substr)
    index = df_current[column_name].str.find(substr)
    index[index == -1] = np.nan
    return index + 1


def lpad(column_name, len, pad):
    return df_current[column_name].str.pad(width=len, side='left', fillchar=pad)


def ltrim(column_name):
    return df_current[column_name].str.lstrip()


def octet_length(column_name):
    return df_current[column_name].apply(lambda x: len(x.encode('utf-8')))


def regexp_extract(column_name, pattern, idx):
    return df_current[column_name].str.extract(pattern, expand=False)


def regexp_replace(column_name, pattern, replacement):
    return df_current[column_name].str.replace(pattern, replacement, regex=True)


def unbase64(column_name):
    return df_current[column_name].apply(lambda x: base64.b64decode(x).decode())


def rpad(column_name, len, pad):
    return df_current[column_name].str.pad(width=len, side='right', fillchar=pad)


def repeat(column_name, n):
    return df_current[column_name].apply(lambda x: x * n)


def rtrim(column_name):
    return df_current[column_name].str.rstrip()


def soundex():
    raise Exception('Not implemented!')


def split(column_name, pattern, limit):
    df_splitted = df_current[column_name].str.split(pattern, expand=True)
    df_splitted['id_idx'] = pd.RangeIndex(start=1, stop=len(df_splitted) + 1)
    df_splitted.set_index('id_idx', inplace=True)  # 'id_idx' sütununu indeks olarak ayarla

    df_current['id_idx'] = pd.RangeIndex(start=1, stop=len(df_current) + 1)
    df_current.set_index('id_idx', inplace=True)  # 'id_idx' sütununu indeks olarak ayarla
    df_merged = pd.merge(df_current, df_splitted, left_index=True, right_index=True, how='inner')
    df_merged.reset_index(inplace=True)
    df_merged = df_merged.drop('id_idx', axis=1)

    # return df_merged
    return df_splitted


def substring(column_name, pos, len):
    pos = pos - 1
    return df_current[column_name].str[pos:pos + len]


def substring_index(column_name, delim, count):
    return df_current[column_name].apply(lambda x: delim.join(x.split(delim)[:count]))


def overlay():
    raise Exception('Not implemented!')


def sentences():
    raise Exception('Not implemented!')


def translate():
    raise Exception('Not implemented!')


def trim(column_name, trim_type='BOTH', trim_str=None):
    # return df_current[column_name].str.strip()
    """
    > SELECT trim('    SparkSQL   '); --> SparkSQL
    > SELECT trim(BOTH FROM '    SparkSQL   '); -->SparkSQL
    > SELECT trim(LEADING FROM '    SparkSQL   '); -->SparkSQL
    > SELECT trim(TRAILING FROM '    SparkSQL   '); -->    SparkSQL
    > SELECT trim('SL' FROM 'SSparkSQLS'); --> parkSQ
    > SELECT trim(BOTH 'SL' FROM 'SSparkSQLS'); --> parkSQ
    > SELECT trim(LEADING 'SL' FROM 'SSparkSQLS'); --> parkSQLS
    > SELECT trim(TRAILING 'SL' FROM 'SSparkSQLS'); --> SSparkSQ
    """
    if trim_type == 'BOTH':
        return df_current[column_name].apply(lambda x: x.strip(trim_str))
    elif trim_type == 'LEADING':
        return df_current[column_name].apply(lambda x: x.lstrip(trim_str))
    elif trim_type == 'TRAILING':
        return df_current[column_name].apply(lambda x: x.rstrip(trim_str))
    else:
        raise ValueError("Invalid trim_type. Choose from 'BOTH', 'LEADING', or 'TRAILING'.")


def upper(column_name):
    return df_current[column_name].str.upper()


def like(column_name, pattern):
    if pattern[0] != '%':
        new_pattern = pattern.split('%')[0]
        return df_current[df_current[column_name].str.startswith(new_pattern)]
    elif pattern[-1] != '%':
        return df_current[df_current[column_name].str.endswith(pattern[-1])]
    else:
        new_pattern = pattern.replace('%', '.*')
        return df_current[df_current[column_name].str.contains(new_pattern)]


def length(column_name):
    return df_current[column_name].str.len()


def instr(column_name, substr):
    return df_current[column_name].str.find(substr).apply(lambda x: 0 if x == -1 else x + 1)


# UDF FUNC #
def call_udf():
    raise Exception('Not implemented!')


def pandas_udf():
    raise Exception('Not implemented!')


def udf():
    raise Exception('Not implemented!')


def unwrap_udt():
    raise Exception('Not implemented!')


# MISC FUNC #
def md5(column_name):
    return df_current[column_name].apply(lambda x: hashlib.md5(x.encode()).hexdigest())


def sha(column_name):
    return df_current[column_name].apply(lambda x: hashlib.sha1(x.encode()).hexdigest())


def sha1(column_name):
    return df_current[column_name].apply(lambda x: hashlib.sha1(x.encode()).hexdigest())


def sha2(column_name, numBits):
    if numBits == 224:
        return df_current[column_name].apply(lambda x: hashlib.sha224(x.encode()).hexdigest())
    elif numBits == 256:
        return df_current[column_name].apply(lambda x: hashlib.sha256(x.encode()).hexdigest())
    elif numBits == 384:
        return df_current[column_name].apply(lambda x: hashlib.sha384(x.encode()).hexdigest())
    elif numBits == 512:
        return df_current[column_name].apply(lambda x: hashlib.sha512(x.encode()).hexdigest())
    else:
        raise Exception('Tanimsiz num bits')


def crc32(column_name):
    return df_current[column_name].apply(lambda x: zlib.crc32(x.encode()))


def hash_func(cols):
    return df_current[cols].apply(lambda x: hash(x))


def xxhash64(cols):
    return df_current[cols].apply(lambda x: xxhash.xxh64(x).hexdigest())


def assert_true(column_name, errMessage):
    full_condition = f'df[{column_name}]'

    if eval(full_condition, globals(), {'df': df_current}):
        return None
    else:
        raise AssertionError(errMessage)


"""
    if eval(column_name):
        return None
    else:
        raise Exception(errMessage)
"""


def raise_error(errMessage):
    raise Exception(errMessage)


def fillna_func(column_name, value):
    return df_current[column_name].fillna(value=value)


def drop_duplicates(subset):
    df_drop_duplicates = df_current.drop_duplicates(subset=subset)
    return df_drop_duplicates


def pivot_func(index, columns, values):
    # pivot_df = df_current.set_index(index).pivot(columns=columns, values=values).reset_index()
    # pivot_df.columns.name = None
    pivot_df = df_current.pivot(index=index, columns=columns, values=values)
    return pivot_df


def melt_func(var_name, value_name):
    melt_df = df_current.melt(var_name=var_name, value_name=value_name)
    return melt_df


def get_last_character_func(column_name):
    def get_last_character(value):
        if isinstance(value, str) and len(value) > 0:
            return value[-1]
        else:
            return None

    return df_current[column_name].apply(get_last_character)


def string_indexer(column_name):
    encoder = LabelEncoder()
    df_current[column_name] = encoder.fit_transform(df_current[column_name])
    return df_current
