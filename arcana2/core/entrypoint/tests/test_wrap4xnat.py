from argparse import ArgumentParser
from arcana2.core.entrypoint.wrap4xnat import Wrap4XnatCmd


def test_wrap4xnat_app(xnat_repository, docker_registry):
    parser = ArgumentParser()
    Wrap4XnatCmd.construct_parser(parser)
    args = parser.parse_args()
    Wrap4XnatCmd().run(args)
