import click

from .agg import agg
from .calls2h5 import calls2h5
from .convert import convert
from .delete import delete
from .version import version

@click.group()
def facet():
    pass

facet.add_command(agg, name="agg")
facet.add_command(calls2h5, name="calls2h5")
facet.add_command(convert, name="convert")
facet.add_command(delete, name="delete")
facet.add_command(version, name="version")