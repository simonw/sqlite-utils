from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Callable, Optional

from dateutil import parser
import json

IGNORE: object = object()
SET_NULL: object = object()


def parsedate(
    value: Optional[str],
    dayfirst: bool = False,
    yearfirst: bool = False,
    errors: Optional[object] = None,
) -> Optional[str]:
    """
    Parse a date and convert it to ISO date format: yyyy-mm-dd
    \b
    - dayfirst=True: treat xx as the day in xx/yy/zz
    - yearfirst=True: treat xx as the year in xx/yy/zz
    - errors=r.IGNORE to ignore values that cannot be parsed
    - errors=r.SET_NULL to set values that cannot be parsed to null
    """
    if not value:
        return value
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


def parsedatetime(
    value: Optional[str],
    dayfirst: bool = False,
    yearfirst: bool = False,
    errors: Optional[object] = None,
) -> Optional[str]:
    """
    Parse a datetime and convert it to ISO datetime format: yyyy-mm-ddTHH:MM:SS
    \b
    - dayfirst=True: treat xx as the day in xx/yy/zz
    - yearfirst=True: treat xx as the year in xx/yy/zz
    - errors=r.IGNORE to ignore values that cannot be parsed
    - errors=r.SET_NULL to set values that cannot be parsed to null
    """
    if not value:
        return value
    try:
        return parser.parse(value, dayfirst=dayfirst, yearfirst=yearfirst).isoformat()
    except parser.ParserError:
        if errors is IGNORE:
            return value
        elif errors is SET_NULL:
            return None
        else:
            raise


def jsonsplit(
    value: str, delimiter: str = ",", type: Callable[[str], Any] = str
) -> str:
    """
    Convert a string like a,b,c into a JSON array ["a", "b", "c"]
    """
    return json.dumps([type(s.strip()) for s in value.split(delimiter)])
