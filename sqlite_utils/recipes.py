from dateutil import parser
import json

IGNORE = object()
SET_NULL = object()


def parsedate(value, dayfirst=False, yearfirst=False, errors=None):
    """
    Parse a date and convert it to ISO date format: yyyy-mm-dd
    \b
    - dayfirst=True: treat xx as the day in xx/yy/zz
    - yearfirst=True: treat xx as the year in xx/yy/zz
    - errors=r.IGNORE to ignore values that cannot be parsed
    - errors=r.SET_NULL to set values that cannot be parsed to null
    """
    try:
        return (
            parser.parse(value, dayfirst=dayfirst, yearfirst=yearfirst)
            .date()
            .isoformat()
        )
    except parser.ParserError:
        if errors is IGNORE:
            return value
        elif errors is SET_NULL:
            return None
        else:
            raise


def parsedatetime(value, dayfirst=False, yearfirst=False, errors=None):
    """
    Parse a datetime and convert it to ISO datetime format: yyyy-mm-ddTHH:MM:SS
    \b
    - dayfirst=True: treat xx as the day in xx/yy/zz
    - yearfirst=True: treat xx as the year in xx/yy/zz
    - errors=r.IGNORE to ignore values that cannot be parsed
    - errors=r.SET_NULL to set values that cannot be parsed to null
    """
    try:
        return parser.parse(value, dayfirst=dayfirst, yearfirst=yearfirst).isoformat()
    except parser.ParserError:
        if errors is IGNORE:
            return value
        elif errors is SET_NULL:
            return None
        else:
            raise


def jsonsplit(value, delimiter=",", type=str):
    """
    Convert a string like a,b,c into a JSON array ["a", "b", "c"]
    """
    return json.dumps([type(s.strip()) for s in value.split(delimiter)])
