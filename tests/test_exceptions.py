import dataclasses as dc
import itertools
import parse

import amethyst_facet as fct

def test_exceptions():
    parser = fct.UniformWindowsParser(arg="test=10:5+1")
    windows = fct.UniformWindows(parser.size, parser.step, parser.offset)
    name = parser.name
    
