from pathlib import Path
from typing import *

import parse

import amethyst_facet as fct
from amethyst_facet.windows import VariableWindowsAggregator

class VariableWindowsParserException(Exception):
    def __init__(self, arg: str, message: str):
        message = f"Problem parsing variable windows argument '{arg}'. {message}"
        super().__init__(message)

class InvalidFormat(VariableWindowsParserException):
    def __init__(self, arg: str, format: str):
        message = (
            f"Attempted to use format '{format}' but was unable to use it to parse argument. "
            "Hint: avoid using the following symbols in your name or path: {}:+ "
            "and do not surround the name or path in curly braces (i.e. use "
            "-v name=path/to/windows.tsv, not -v {name}={path/to/windows.tsv})"
        )
        super().__init__(arg, message)

class InvalidName(VariableWindowsParserException):
    def __init__(self, arg: str, format: str, name: str, stripped: str):
        message = (
            f"After using format '{format}' to parse and stripping flanking whitespace "
            f"from name '{name}', obtained '{stripped}' "
            f"which is blank. Dataset names must contain non-whitespace characters. "
            f"Note that if no name is supplied, the name defaults to the prefix of the path filename, "
            f"i.e. -v window_file.tsv (no name suppled) uses 'window_file' as the dataset name."
        )
        super().__init__(arg, message)

class VariableWindowsParser:
    def parse(self, arg: str) -> VariableWindowsAggregator:
        format = "{name}={path}" if "=" in arg else "{path}"
        try:
            parsed = parse.parse(format, arg).named
            path = Path(parsed["path"])
            default_name = str(path.name.removesuffix(path.suffix))
            name = parsed.get("name", default_name)
        except Exception as e:
            raise InvalidFormat(arg, format) from e
        
        if not (stripped := name.strip()):
            raise InvalidName(self, arg, format, name, stripped)
        else:
            name = stripped

        result = VariableWindowsAggregator(name=name, path=path)
        return result