import sys
import time
import logging
import colorama


class ColoredFormatter(logging.Formatter):
    COLORS = {
            'DEBUG': colorama.Fore.BLACK + colorama.Style.BRIGHT,
            'INFO': '',
            'WARNING': colorama.Fore.YELLOW,
            'ERROR': colorama.Fore.RED,
            'CRITICAL': colorama.Back.RED + colorama.Fore.WHITE
            }

    def __init__(self, fmt=None, datefmt=None):
        super(ColoredFormatter, self).__init__(fmt, datefmt)

    def format(self, record):
        color = self.COLORS.get(record.levelname)
        res = super(ColoredFormatter, self).format(record)
        if color:
            res = color + res + colorama.Style.RESET_ALL
        return res


def setup_logging(quiet=False, debug=False, log_file=None, log_debug=False,
                  prefix=None):
    prefix = prefix or ''

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    if debug or log_debug:
        root_logger.setLevel(logging.DEBUG)

    root_logger.handlers = []

    log_handler = logging.StreamHandler(sys.stdout)
    if debug:
        log_handler.setLevel(logging.DEBUG)
        log_handler.setFormatter(
                ColoredFormatter(prefix + "[%(name)s] %(message)s"))
    else:
        if quiet:
            log_handler.setLevel(logging.WARNING)
        else:
            log_handler.setLevel(logging.INFO)
        log_handler.setFormatter(
                ColoredFormatter(prefix + "%(message)s"))
    root_logger.addHandler(log_handler)

    if log_file:
        file_handler = logging.FileHandler(log_file, mode='w')
        root_logger.addHandler(file_handler)
        if log_debug:
            file_handler.setLevel(logging.DEBUG)


def format_timed(start_time, message, indent_level=0, colored=True):
    end_time = time.clock()
    indent = indent_level * '  '
    time_str = '%8.1f ms' % ((end_time - start_time) * 1000.0)
    if colored:
        return '[%s%s%s] %s' % (colorama.Fore.GREEN, time_str,
                                colorama.Fore.RESET, message)
    return '%s[%s] %s' % (indent, time_str, message)


def log_friendly_exception(logger, ex):
    indent = ''
    while ex:
        logger.error('%s%s' % (indent, str(ex)))
        indent += '  '
        ex = ex.__cause__


def print_help_item(s, title, description, margin=4, align=25):
    s.write(margin * ' ')
    s.write(title)
    spacer = (align - margin - len(title) - 1)
    if spacer <= 0:
        s.write("\n")
        s.write(' ' * align)
    else:
        s.write(' ' * spacer)
    s.write(description)
    s.write("\n")

