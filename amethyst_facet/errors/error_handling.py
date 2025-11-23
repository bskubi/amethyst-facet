import sys
from loguru import logger
from rich.logging import RichHandler
import shlex

# 1. Clear default handler
logger.remove()

# 2. Add the "Cluster File" Sink (Clean, detailed, extensive history)
logger.add(
    "amethyst_facet.log", 
    rotation="100 MB", 
    backtrace=True,  # Extend stack up
    diagnose=True,   # Show variables (cautious)
    format="{time} {level} {message}"
)

# 3. Add the "User Terminal" Sink (Visual, pretty)
# We only add this if we are in an interactive terminal
if sys.stderr.isatty():
    logger.add(
        RichHandler(rich_tracebacks=True, tracebacks_show_locals=True), 
        format="{message}", 
        level="INFO"
    )
else:
    # Fallback for non-interactive stderr (e.g., SLURM .err files)
    logger.add(sys.stderr, format="{time} {level} {message}")

full_cmd = [sys.executable] + sys.argv
command_str = shlex.join(full_cmd)
logger.info(f"Command invocation: {command_str}")
logger.info("When raising a Github issue or reporting an error, please include the complete error message and the contents of amethyst_facet.log.")