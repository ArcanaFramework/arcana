import sys
import logging
from argparse import ArgumentParser
from arcana2.core.utils import wrap_text
from arcana2.__about__ import __version__
from .run import RunAppCmd, RunBidsAppCmd
from .wrapper import Wrap4XnatCSCmd

logger = logging.getLogger('arcana')

DEFAULT_LINE_LENGTH = 79
DEFAULT_INDENT = 4
DEFAULT_SPACER = 4


class HelpCmd():

    desc = "Show help for a particular command"

    @classmethod
    def construct_parser(cls, parser):
        parser.add_argument('command',
                            help="The sub-command to show the help info for")

    @classmethod
    def run(cls, args):
        MainCmd.commands[args.command].parser().print_help()



class MainCmd():

    commands = {
        'run-app': RunAppCmd,
        'run-bids-app': RunBidsAppCmd,
        'wrap4xnat': Wrap4XnatCSCmd,
        'help': HelpCmd}

    @classmethod
    def parser(cls):
        usage = "arcana <command> [<args>]\n\nAvailable commands:"
        desc_start = max(len(k) for k in cls.commands) + DEFAULT_SPACER
        for name, cmd_cls in cls.commands.items():
            spaces = ' ' * (desc_start - len(name))
            usage += '\n{}{}{}{}'.format(
                ' ' * DEFAULT_INDENT, name, spaces,
                wrap_text(cmd_cls.desc, DEFAULT_LINE_LENGTH,
                          desc_start + DEFAULT_INDENT))
        parser = ArgumentParser(
            description="Base Arcana command",
            usage=usage)
        parser.add_argument('command', help="The sub-command to run")
        parser.add_argument('--version', '-v', action='version',
                            version='%(prog)s {}'.format(__version__))
        return parser

    @classmethod
    def run(cls, argv=None):
        if argv is None:
            argv = sys.argv[1:]
        parser = cls.parser()
        args = parser.parse_args(argv[:1])
        try:
            cmd_cls = cls.commands[args.command]
        except KeyError:
            print("Unrecognised command '{}'".format(args.command))
            parser.print_help()
            exit(1)
        if args.command == 'help' and len(argv) == 1:
            parser.print_help()
        else:
            cmd_parser = ArgumentParser(prog='arcana ' + args.command,
                                        description=cmd_cls.desc)
            cmd_cls.construct_parser(cmd_parser)
            cmd_args = cmd_parser.parse_args(argv[1:])
            cmd_cls.run(cmd_args)


if __name__ == '__main__':
    MainCmd.run()
