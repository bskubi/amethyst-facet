import click

import amethyst_facet as fct

@click.group()
def facet():
    pass

facet.add_command(fct.agg, name="agg")
facet.add_command(fct.calls2h5, name="calls2h5")
facet.add_command(fct.convert, name="convert")
facet.add_command(fct.delete, name="delete")
facet.add_command(fct.dump, name="dump")
facet.add_command(fct.version, name="version")

if __name__ == "__main__":
    facet()