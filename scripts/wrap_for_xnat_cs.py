#!/usr/bin/env python3
"""Creates Dockerfile that wraps a BIDS app (by extending the app image) and
provides all necessary boilerplate code for the image it creates to be installed
in the XNAT CS.
"""

from arcana2.data.repository.xnat_cs import XnatCSRepo
from arcana2.data.item import FileGroup
from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument("base_image", help="The app image to wrap")
parser.add_argument("entrypoint", help="The entrypoint of the app image")
parser.add_argument('image_name', )
parser.add_argument('--input', '-i', action='append', nargs=2,
                    metavar=('PATH', 'FORMAT'), help=(
                        "The inputs expected by the app image"))
parser.add_argument('--output', '-o', action='append', nargs=2,
                    metavar=('PATH', 'FORMAT'), help=(
                        "The outputs expected to be produced by the app image"))
parser.add_argument('--brainlife',
                    help=("Brainlife app name for automatic detection of "
                          "inputs/outputs)"))
parser.add_argument('--frequency', default='per_session',
                    choices=FileGroup.VALID_REQUENCIES,
                    help="Whether the ")
parser.add_argument('--docker_index', default="https://index.docker.io/v1/",
                    help="The docker index to use in the command JSON")
args = parser.parse_args()

repo = XnatCSRepo.command_json(
    image_name, analysis_cls, inputs, derivatives, parameters, desc,
    frequency='per_session', docker_index=args.docker_index)