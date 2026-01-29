import sys
from loguru import logger
import amethyst_facet
import amethyst_facet.errors

def main():
    amethyst_facet.cli.commands.facet()


if __name__ == "__main__":
    main()
    