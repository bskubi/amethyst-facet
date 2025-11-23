import sys
from loguru import logger
import amethyst_facet
import amethyst_facet.errors

def main():
    with logger.catch(onerror=lambda _: sys.exit(1)):
        amethyst_facet.cli.commands.facet(standalone_mode=False)


if __name__ == "__main__":
    main()
    