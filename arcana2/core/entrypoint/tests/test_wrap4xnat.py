from argparse import ArgumentParser
from arcana2.core.entrypoint.run import Wrap4XnatCSCmd


def test_run_app(xnat_repository, docker_registry):
    parser = ArgumentParser()
    Wrap4XnatCSCmd.construct_parser(parser)
    args = parser.parse_args()
    Wrap4XnatCSCmd().run(args)
