import dataclasses as dc
import itertools
import parse

import amethyst_facet as fct
from amethyst_facet.windows import UniformWindowsAggregator

class UniformWindowsParserException(Exception):
    def __init__(self, message: str):
        # Create complete list of valid formats for error message
        name = ["{name}=", ""]
        size = ["{size}"]
        step = [":{step}", ""]
        offset = ["+{offset}", ""]
        valid_formats = itertools.product(name, size, step, offset)
        valid_formats = ", ".join(["".join(it) for it in valid_formats])
        valid_formats_msg = f"Valid formats for uniform window CLI argument: {valid_formats}. "
        message += valid_formats_msg
        super().__init__(message)

class UniformWindowsParseFailed(UniformWindowsParserException):
    def __init__(self, arg: str, message = ""):
        message = message + f"Failed to parse window CLI argument '{arg}'. "
        super().__init__(message)

class NoSize(UniformWindowsParseFailed):
    def __init__(self, arg: str):
        message = f"For uniform windows, specifying a window size is mandatory but was not present. "
        super().__init__(arg, message)

class FailedCastToInt(UniformWindowsParseFailed):
    def __init__(self, arg: str, name: str, value: str):
        message = f"For uniform windows, failed to cast {name} '{value}' to integer. "
        super().__init__(arg, message)

class FailedParseName(UniformWindowsParseFailed):
    def __init__(self, arg: str, message: str = ""):
        message = f"For uniform windows, failed to parse or set name. " + message

        super().__init__(arg, message)


@dc.dataclass
class UniformWindowsParser:
    """Service to parse CLI arguments for generating UniformWindowsAggregator object.
    """

    def parse(self, arg: str) -> UniformWindowsAggregator:
        # Get actual format to parse uniform window parameters from CLI argument
        format = "{name}={size}" if "=" in arg else "{size}"
        format = format + ":{step}" if ":" in arg else format
        format = format + "+{offset}" if "+" in arg else format

        try:
            self.parsed = parse.parse(format, arg).named
        except Exception as e:
            raise UniformWindowsParseFailed(arg) from e
        
        return UniformWindowsAggregator(self.size, self.step, self.offset, self.name)

    @property
    def size(self) -> int:
        self._size = None
        try:
            self._size = self.parsed["size"]
            self._size = int(self._size)
        except KeyError as e:
            raise NoSize(self.arg) from e
        except Exception as e:
            raise FailedCastToInt(self.arg, "size", getattr(self, "_size", None)) from e
        return self._size
    
    @property
    def step(self) -> int:
        self._step = self.parsed.get("step", self.size)
        try:
            self._step = int(self._step)
        except Exception as e:
            raise FailedCastToInt(self.arg, "step", getattr(self, "_step", None)) from e
        return self._step

    @property
    def offset(self) -> str:
        try:
            self._offset = self.parsed.get("offset", 1)
            self._offset = int(self._offset)
        except Exception as e:
            raise FailedCastToInt(self.arg, "offset", getattr(self, "_offset", None)) from e
        return self._offset
    
    @property
    def name(self) -> str:
        try:
            self._name = self.parsed.get("name", None)
        except:
            self._name = None
        return self._name