from pathlib import Path

PACKAGE_NAME = "arcana"
PACKAGE_ROOT = Path(__file__).parent
CODE_URL = f"https://github.com/australian-imaging-service/{PACKAGE_NAME}"

__authors__ = [("Thomas G. Close", "tom.g.close@gmail.com")]

install_requires = [
    "docker>=5.0.2",
    "jq>=1.2.2",
    "click>=7.1.2",  # 8.1.3",
    "PyYAML>=6.0",
    "natsort>=7.1.1",
    "fasteners>=0.7.0",
    "numexpr>=1.10.1",
    "importlib-metadata>=1.4",
    "deepdiff>=3.3",
    "pydicom>=1.0.2",
    "nibabel>=3.2.1",
    "neurodocker>=0.9.1",
    "xnat>=0.3.17",
    "pydra>=0.20",  # @ git+https://github.com/Australian-Imaging-Service/pydra.git@0.19+ais1",
    "pydra-dcm2niix>=1.2.0",
    "pydra-mrtrix3>=0.2",
]

tests_require = [
    "pytest>=5.4.3",
    "pytest-env>=0.6.2",
    "pytest-cov>=2.12.1",
    "xnat4tests>=0.2",
    "medimages4tests>=0.3",
]

docs_require = [
    "docutils>=0.10",
    "mock>1.0",
    "numpydoc>=0.6.0",
    "sphinx-argparse>=0.2.0",
    "sphinx-click>=3.1",
    "furo>=2022.2.14.1]",
]

dev_requires = ["black>=21.4b2", "pre-commit>=2.19.0"]

all_requires = install_requires + tests_require + docs_require + dev_requires

python_versions = ["3.8", "3.9", "3.10"]
