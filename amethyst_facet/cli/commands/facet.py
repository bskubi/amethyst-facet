import click
import amethyst_facet.cli.commands as cmd

@click.group()
def facet():
    pass

facet.add_command(cmd.agg, name="agg")
# facet.add_command(cmd.calls2h5, name="calls2h5")
facet.add_command(cmd.convert, name="convert")
# facet.add_command(cmd.delete, name="delete")
# facet.add_command(cmd.dump, name="dump")
# facet.add_command(cmd.version, name="version")