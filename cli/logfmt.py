import click
import logging
import re
import time

# Track script start time
SCRIPT_START_TIME = time.time()

class ColorFormatter(logging.Formatter):

    def __init__(self, fmt):
        super().__init__(fmt)

        self._colors = {
            logging.CRITICAL: "red",
            logging.ERROR:    "red",
            logging.WARNING:  "yellow",
            logging.INFO:     "green",
            logging.DEBUG:    "magenta",
            15:              "blue",    # VERBOSE level
        }

        self._marks = {
            logging.CRITICAL: "!",
            logging.ERROR:    "E",
            logging.WARNING:  "W",
            logging.INFO:     "*",
            logging.DEBUG:    "D",
            15:              "V",       # VERBOSE level
        }

        self.packet_re = re.compile(r'(RX <--|TX -->)\s+(\w+)\((.*?)\)')

    def format(self, rec):
        marks, colors = self._marks, self._colors
        
        # Handle packet logging differently based on level
        if rec.levelno <= logging.DEBUG and isinstance(rec.msg, str):
            match = self.packet_re.match(rec.msg)
            if match:
                direction, pkt_type, details = match.groups()
                # In verbose mode (-v), show simplified packet info
                if rec.levelno == 15:  # VERBOSE level
                    rec.msg = f"{direction} {pkt_type}"
                # In debug mode (-vv), show full packet details
                # (original message is kept)

        # Add elapsed time
        elapsed = time.time() - SCRIPT_START_TIME
        elapsed_str = f"{elapsed:.1f}s"

        return "".join([
            click.style("[",                fg="blue",              bold=True),
            click.style(marks[rec.levelno], fg=colors[rec.levelno], bold=True),
            click.style("]",                fg="blue",              bold=True),
            " ",
            click.style(f"[{elapsed_str}]", fg="cyan"),
            " ",
            super().format(rec),
        ])


class ExitOnExceptionHandler(logging.StreamHandler):

    def emit(self, record):
        super().emit(record)
        if record.levelno == logging.CRITICAL:
            raise SystemExit(127)


def setup_logging(level=logging.INFO):
    # Add VERBOSE level
    if not hasattr(logging, 'VERBOSE'):
        logging.VERBOSE = 15
        logging.addLevelName(logging.VERBOSE, 'VERBOSE')

    logging.basicConfig(handlers=[ExitOnExceptionHandler()])
    log = logging.getLogger()
    log.setLevel(level)
    handler = log.handlers[0]
    handler.setFormatter(ColorFormatter("%(message)s"))
    return log
