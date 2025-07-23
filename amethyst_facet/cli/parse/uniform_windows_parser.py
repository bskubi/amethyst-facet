import dataclasses as dc
import itertools
import parse

import amethyst_facet as fct
from amethyst_facet.windows import UniformWindowsAggregator

class UniformWindowsParserException(Exception):
    def __init__(self, arg: str, format, message: str):
        # Create complete list of valid formats for error message
        name = ["{name}=", ""]
        size = ["{size}"]
        step = [":{step}", ""]
        offset = ["+{offset}", ""]
        valid_formats = itertools.product(name, size, step, offset)
        valid_formats = ", ".join(["".join(it) for it in valid_formats])
        valid_formats_msg = f"Valid formats for uniform window CLI argument: {valid_formats}. "
        message = (
            f"Problem parsing uniform windows parameters from -u argument "
            f"'{arg}' detected as format '{format}'. "
            f"{message} {valid_formats_msg}"
        )
        super().__init__(message)

class UniformWindowsParseFailed(UniformWindowsParserException):
    def __init__(self, arg: str, format: str, message = ""):
        super().__init__(arg, format, message)

class NoSize(UniformWindowsParseFailed):
    def __init__(self, arg: str, format: str):
        message = f"No window size specified."
        super().__init__(arg, format, message)

class FailedCastToInt(UniformWindowsParseFailed):
    def __init__(self, arg: str, format: str, var: str, value: str):
        message = f"Failed to cast {var} '{value}' to integer."
        super().__init__(arg, format, message)

class InvalidSize(UniformWindowsParseFailed):
    def __init__(self, arg: str, format: str, size: int):
        message = f"Size must be a positive integer, but size={size}."
        super().__init__(arg, format, message)

class InvalidStep(UniformWindowsParseFailed):
    def __init__(self, arg: str, format: str, size: int, step: int):
        message = f"Step must be a positive integer divisible by size, but size={size} and step={step}."
        super().__init__(arg, format, message)

class InvalidUniformWindowsName(UniformWindowsParseFailed):
    def __init__(self, arg: str, format: str, name: str):
        message = (
            f"Name was given as '{name}', but this appears to be all whitespace. "
            f"If a name is supplied, it must contain non-whitespace characters. "
            r"Alternatively, leaving the name out uses the default name {size}:{step}+{offset}."
        )
        super().__init__(arg, format, message)


@dc.dataclass
class UniformWindowsParser:
    """Service to parse CLI arguments for generating UniformWindowsAggregator object.
    """

    def parse(self, arg: str) -> UniformWindowsAggregator:
        # Get actual format to parse uniform window parameters from CLI argument
        format = "{name}={size}" if "=" in arg else "{size}"
        format = format + ":{step}" if ":" in arg else format
        format = format + "+{offset}" if "+" in arg else format
        self._arg = arg
        self._format = format

        try:
            self.parsed = parse.parse(format, arg).named
        except Exception as e:
            raise UniformWindowsParseFailed(self._arg, self._format) from e
        
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
            raise FailedCastToInt(self._arg, self._format, "size", getattr(self, "_size", None)) from e

        if self._size <= 0:
            raise InvalidSize(self._arg, self._format, self._size)

        return self._size
    
    @property
    def step(self) -> int:
        self._step = self.parsed.get("step", self.size)
        try:
            self._step = int(self._step)
        except Exception as e:
            raise FailedCastToInt(self._arg, self._format, "step", getattr(self, "_step", None)) from e

        if self._step <= 0 or self.size % self._step != 0:
            raise InvalidStep(self._arg, self._format, self._size, self._step)

        return self._step

    @property
    def offset(self) -> str:
        try:
            self._offset = self.parsed.get("offset", 1)
            self._offset = int(self._offset)
        except Exception as e:
            raise FailedCastToInt(self._arg, self._format, "offset", getattr(self, "_offset", None)) from e
        return self._offset
    
    @property
    def name(self) -> str:
        try:
            self._name = self.parsed.get("name", None)
        except:
            self._name = None
        if self._name is not None and self._name.strip() == "":
            raise InvalidUniformWindowsName(self._arg, self._format, self._name)
        
        return self._name