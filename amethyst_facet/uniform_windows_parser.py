import dataclasses as dc
import itertools
import parse

import amethyst_facet as fct

class UniformWindowCLIProblem(Exception):
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

class UniformWindowCLIParseFailed(UniformWindowCLIProblem):
    def __init__(self, arg: str, message = ""):
        message = message + f"Failed to parse window CLI argument '{arg}'. "
        super().__init__(message)

class NoSize(UniformWindowCLIParseFailed):
    def __init__(self, arg: str):
        message = f"For uniform windows, specifying a window size is mandatory but was not present. "
        super().__init__(arg, message)

class FailedCastToInt(UniformWindowCLIParseFailed):
    def __init__(self, arg: str, name: str, value: str):
        message = f"For uniform windows, failed to cast {name} '{value}' to integer. "
        super().__init__(arg, message)

class FailedParseName(UniformWindowCLIParseFailed):
    def __init__(self, arg: str, message: str = ""):
        message = f"For uniform windows, failed to parse or set name. " + message

        super().__init__(arg, message)


@dc.dataclass
class UniformWindowsParser:
    def __init__(self, arg: str):
        self.arg = arg
        self.parse()

    def parse(self) -> str:
        # Get actual format to parse uniform window parameters from CLI argument
        format = "{name}={size}" if "=" in self.arg else "{size}"
        format = format + ":{step}" if ":" in self.arg else format
        format = format + "+{offset}" if "+" in self.arg else format

        try:
            self.parsed = parse.parse(format, self.arg).named
        except Exception as e:
            raise UniformWindowCLIParseFailed(self.arg) from e

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
            default_name = str(self.size) + "_by_" + str(self.step) if self.step != self.size else str(self.size)
            self._name = self.parsed.get("name", default_name)
        except Exception as e:
            name = getattr(self, "_name", None)
            message = f"Name was '{name}'" if name is not None else ""
            raise FailedParseName(self.arg, message) from e
        
        try:
            assert self._name.strip()
        except Exception as e:
            message = f"Window name must be a string with non-whitespace characters, but was '{self._name}'. "
            raise FailedParseName(self.arg, message) from e
        return self._name
    
    @property
    def windows(self) -> fct.UniformWindows:
        return fct.UniformWindows(self.size, self.step, self.offset)
