import click
from arcana import __version__


# Define the base CLI entrypoint
@click.group()
@click.version_option(version=__version__)
def cli():
    pass


# import sys
# import logging
# from abc import ABCMeta, abstractclassmethod
# from argparse import ArgumentParser
# from arcana.core.utils import classproperty, resolve_subclass, list_subclasses, wrap_text, submodules
# from arcana import __version__

# logger = logging.getLogger('arcana')

# DEFAULT_LINE_LENGTH = 79
# DEFAULT_INDENT = 4
# DEFAULT_SPACER = 4


# class BaseCmd(metaclass=ABCMeta):
#     """Abstract base class for new Arcana entrypoints
#     """

#     @abstractclassmethod
#     def construct_parser(cls, parser):
#         pass

#     @abstractclassmethod
#     def run(cls, args):
#         pass


# class HelpCmd(BaseCmd):

#     cmd_name = 'help'
#     desc = "Show help for a particular command"

#     @classmethod
#     def construct_parser(cls, parser):
#         parser.add_argument('command',
#                             help=("The sub-command to show the help info for."
#                                   " Available sub-commands are:\n"
#                                   + "\n".join(MainCmd.commands)))

#     @classmethod
#     def run(cls, args):
#         MainCmd.get_parser(args.command).print_help()


# class MainCmd():

#     @classmethod
#     def parser(cls):
#         usage = "arcana <command> [<args>]\n\nAvailable commands:"
#         desc_start = max(len(k) for k in cls.commands) + DEFAULT_SPACER
#         for name, cmd_cls in cls.commands.items():
#             spaces = ' ' * (desc_start - len(name))
#             usage += '\n{}{}{}{}'.format(
#                 ' ' * DEFAULT_INDENT, name, spaces,
#                 wrap_text(cmd_cls.desc, DEFAULT_LINE_LENGTH,
#                           desc_start + DEFAULT_INDENT))
#         parser = ArgumentParser(
#             description="Base Arcana command",
#             usage=usage)
#         parser.add_argument('command', help="The sub-command to run")
#         parser.add_argument('--version', '-v', action='version',
#                             version='%(prog)s {}'.format(__version__))
#         return parser

#     @classmethod
#     def run(cls, argv=None):
#         if argv is None:
#             argv = sys.argv[1:]
#         parser = cls.parser()
#         args = parser.parse_args(argv[:1])
#         try:
#             cmd_cls = cls.commands[args.command]
#         except KeyError:
#             print("Unrecognised command '{}'".format(args.command))
#             parser.print_help()
#             exit(1)
#         if args.command == 'help' and len(argv) == 1:
#             parser.print_help()
#         else:
#             cmd_parser = ArgumentParser(prog='arcana ' + args.command,
#                                         description=cmd_cls.desc)
#             cmd_cls.construct_parser(cmd_parser)
#             cmd_args = cmd_parser.parse_args(argv[1:])
#             output = cmd_cls.run(cmd_args)
#             if output is not None:
#                 print(output)

#     @classmethod
#     def get_parser(cls, command_name):
#         cmd_cls = cls.commands[command_name]
#         cmd_parser = ArgumentParser(prog='arcana ' + command_name,
#                                     description=cmd_cls.desc)
#         cmd_cls.construct_parser(cmd_parser)
#         return cmd_parser

#     @classproperty
#     def commands(cls):
#         if cls._commands is None:
#             import arcana.entrypoints
#             cls._commands = {
#                 c.cmd_name: c
#                 for c in list_subclasses(arcana.entrypoints, BaseCmd)
#                 if hasattr(c, 'cmd_name')}
#             cls._commands['help'] = HelpCmd
#         return cls._commands

#     _commands = None
