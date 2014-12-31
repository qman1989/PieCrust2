import io
import sys
import time
import os.path
import logging
import argparse
import colorama
from piecrust import APP_VERSION
from piecrust.app import PieCrust, PieCrustConfiguration
from piecrust.chefutil import (
        setup_logging, format_timed, log_friendly_exception,
        print_help_item)
from piecrust.commands.base import CommandContext
from piecrust.pathutil import SiteNotFoundError, find_app_root
from piecrust.plugins.base import PluginLoader


logger = logging.getLogger(__name__)


class NullPieCrust:
    def __init__(self):
        self.root_dir = None
        self.debug = False
        self.templates_dirs = []
        self.plugins_dirs = []
        self.theme_dir = None
        self.cache_dir = None
        self.config = PieCrustConfiguration()
        self.plugin_loader = PluginLoader(self)
        self.env = None


def main():
    argv = sys.argv
    pre_args = pre_parse_chef_args(argv)
    try:
        exit_code = _run_chef(pre_args)
    except Exception as ex:
        if pre_args.debug:
            logger.exception(ex)
        else:
            log_friendly_exception(logger, ex)
        exit_code = 1
    sys.exit(exit_code)


class PreParsedChefArgs(object):
    def __init__(self, root=None, cache=True, debug=False, quiet=False,
                 log_file=None, log_debug=False, config_variant=None):
        self.root = root
        self.cache = cache
        self.debug = debug
        self.quiet = quiet
        self.log_file = log_file
        self.log_debug = log_debug
        self.config_variant = config_variant


def pre_parse_chef_args(argv):
    # We need to parse some arguments before we can build the actual argument
    # parser, because it can affect which plugins will be loaded. Also, log-
    # related arguments must be parsed first because we want to log everything
    # from the beginning.
    res = PreParsedChefArgs()
    i = 1
    while i < len(argv):
        arg = argv[i]
        if arg.startswith('--root='):
            res.root = os.path.expanduser(arg[len('--root='):])
        elif arg == '--root':
            res.root = argv[i + 1]
            ++i
        elif arg.startswith('--config='):
            res.config_variant = arg[len('--config='):]
        elif arg == '--config':
            res.config_variant = argv[i + 1]
            ++i
        elif arg == '--log':
            res.log_file = argv[i + 1]
            ++i
        elif arg == '--log-debug':
            res.log_debug = True
        elif arg == '--no-cache':
            res.cache = False
        elif arg == '--debug':
            res.debug = True
        elif arg == '--quiet':
            res.quiet = True

        if arg[0] != '-':
            break

        i = i + 1

    # Setup the logger.
    if res.debug and res.quiet:
        raise Exception("You can't specify both --debug and --quiet.")

    colorama.init()
    setup_logging(res.quiet, res.debug, res.log_file, res.log_debug)

    return res


def _run_chef(pre_args):
    # Setup the app.
    start_time = time.clock()
    root = pre_args.root
    if root is None:
        try:
            root = find_app_root()
        except SiteNotFoundError:
            root = None

    if not root:
        app = NullPieCrust()
    else:
        app = PieCrust(root, cache=pre_args.cache, debug=pre_args.debug)

    # Handle a configuration variant.
    if pre_args.config_variant is not None:
        if not root:
            raise SiteNotFoundError("Can't apply any variant.")
        app.config.applyVariant('variants/' + pre_args.config_variant)

    # Setup the arg parser.
    parser = argparse.ArgumentParser(
            prog='chef',
            description="The PieCrust chef manages your website.",
            formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--version', action='version', version=('%(prog)s ' + APP_VERSION))
    parser.add_argument('--root', help="The root directory of the website.")
    parser.add_argument('--config', help="The configuration variant to use for this command.")
    parser.add_argument('--debug', help="Show debug information.", action='store_true')
    parser.add_argument('--no-cache', help="When applicable, disable caching.", action='store_true')
    parser.add_argument('--quiet', help="Print only important information.", action='store_true')
    parser.add_argument('--log', help="Send log messages to the specified file.")
    parser.add_argument('--log-debug', help="Log debug messages to the log file.", action='store_true')

    commands = sorted(app.plugin_loader.getCommands(),
            key=lambda c: c.name)
    subparsers = parser.add_subparsers(title='list of commands')
    for c in commands:
        p = subparsers.add_parser(c.name, help=c.description)
        c.setupParser(p, app)
        p.set_defaults(func=c.checkedRun)

    help_cmd = next(filter(lambda c: c.name == 'help', commands), None)
    if help_cmd and help_cmd.has_topics:
        with io.StringIO() as epilog:
            epilog.write("additional help topics:\n")
            for name, desc in help_cmd.getTopics():
                print_help_item(epilog, name, desc)
            parser.epilog = epilog.getvalue()


    # Parse the command line.
    result = parser.parse_args()
    logger.debug(format_timed(start_time, 'initialized PieCrust', colored=False))

    # Print the help if no command was specified.
    if not hasattr(result, 'func'):
        parser.print_help()
        return 0

    # Run the command!
    ctx = CommandContext(app, parser, result)
    exit_code = result.func(ctx)
    if exit_code is None:
        return 0
    if not isinstance(exit_code, int):
        logger.error("Got non-integer exit code: %s" % exit_code)
        return -1
    return exit_code

