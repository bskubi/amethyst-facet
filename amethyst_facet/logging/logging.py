from datetime import datetime
import platform
import logging
import sys
import warnings

import amethyst_facet as fct

def config(verbosity, logfile):
    if verbosity != "silent":
        logging.captureWarnings(True)
        root = logging.getLogger()
        levels = {
            "debug": logging.DEBUG,
            "info": logging.INFO,
            "warning": logging.WARNING,
            "error": logging.ERROR,
            "critical": logging.CRITICAL,
            "all": logging.NOTSET
        }
        root.setLevel(levels[verbosity])
        if logfile is None:
            handler = logging.StreamHandler(sys.stdout)
        else:
            handler = logging.FileHandler(logfile)

        handler.setLevel(levels[verbosity])
        root.addHandler(handler)
        configstr = f"{datetime.now()} Configured logger with verbosity: {verbosity} logfile: {logfile}"
        platformstr = "\n".join([
            f"Machine: {platform.machine()}",
            f"Version: {platform.version()}",
            f"Platform: {platform.platform()}",
            f"Uname: {platform.uname()}",
            f"System: {platform.system()}",
            f"Processor: {platform.processor()}",
            f"Amethyst version: {fct.facet_version}"
        ])
        seplength = len(configstr)
        sep = "-"*seplength
        config_msg = (
f"""
{sep}
{configstr}
{platformstr}
{sep}\n
""")
        logging.info(config_msg)
    else:
        warnings.filterwarnings("ignore")

# --- The fix: Define and set the exception hook ---
def handle_exception(exc_type, exc_value, exc_traceback):
    """Log any unhandled exception instead of letting it crash the program."""
    
    # First, log the exception. The 'exception' method automatically includes traceback info.
    logging.critical("Unhandled exception", exc_info=(exc_type, exc_value, exc_traceback))
    
    # Then, let the default sys.excepthook print to stderr so you still see the crash.
    sys.__excepthook__(exc_type, exc_value, exc_traceback)

# Set our custom function as the new exception hook
sys.excepthook = handle_exception