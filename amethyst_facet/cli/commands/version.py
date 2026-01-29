import click
from importlib.metadata import version as package_version, PackageNotFoundError
from loguru import logger

@logger.catch(reraise=True)
def get_version():
    try:
        __version__ = package_version("amethyst-facet")
    except PackageNotFoundError:
        __version__ = "unknown"
    return __version__

@click.command
def version():
    """
    Print version and exit
    """
    print(get_version())
