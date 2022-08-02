import sys
from pathlib import Path
import versioneer
from setuptools import setup, find_packages

# Get version from module inside package
sys.path.insert(0, str(Path(__file__).parent / "arcana"))
from __about__ import (
    PACKAGE_NAME,
    CODE_URL,
    install_requires,
    tests_require,
    dev_requires,
    docs_require,
    all_requires,
    python_versions,
)  # noqa pylint: disable=no-name-in-module

sys.path.pop(0)


setup(
    name=PACKAGE_NAME,
    version=versioneer.get_version(),
    author="Thomas G. Close",
    author_email="tom.g.close@gmail.com",
    packages=find_packages(exclude=["tests", "test"]),
    url=CODE_URL,
    license="Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License",
    description=("Abstraction of Repository-Centric ANAlysis framework"),
    long_description=open("README.rst").read(),
    install_requires=install_requires,
    tests_require=tests_require,
    entry_points={
        "console_scripts": [
            "arcana=arcana.cli:cli",
            "run-arcana-pipeline=arcana.cli.deploy:run_pipeline",
        ]
    },
    extras_require={"test": tests_require,
                    "dev": dev_requires},
    cmdclass=versioneer.get_cmdclass(),
    classifiers=(
        [
            "Development Status :: 4 - Beta",
            "Intended Audience :: Healthcare Industry",
            "Intended Audience :: Science/Research",
            "License :: OSI Approved :: Apache Software License",
            "Natural Language :: English",
            "Topic :: Scientific/Engineering :: Bio-Informatics",
            "Topic :: Scientific/Engineering :: Medical Science Apps.",
        ]
        + ["Programming Language :: Python :: " + str(v) for v in python_versions]
    ),
    keywords="repository analysis neuroimaging workflows pipelines",
)
