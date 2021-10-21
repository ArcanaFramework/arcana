import sys
from pathlib import Path
from setuptools import setup, find_packages

# Get version from module inside package
sys.path.insert(0, str(Path(__file__).parent / 'arcana2'))
from __about__ import __version__, PACKAGE_NAME, install_requires, tests_require, python_versions  # noqa pylint: disable=no-name-in-module
sys.path.pop(0)


setup(
    name=PACKAGE_NAME,
    version=__version__,
    author='Tom G. Close',
    author_email='tom.g.close@gmail.com',
    packages=find_packages(),
    url='https://github.com/australian-imaging-service/arcana2',
    license='Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License',
    description=(
        'Abstraction of Repository-Centric ANAlysis framework'),
    long_description=open('README.rst').read(),
    install_requires=install_requires,
    tests_require=tests_require,
    entry_points={
        'console_scripts': ['arcana = arcana2.core.entrypoint:MainCmd.run']},
    extras_require={
        'test': tests_require},
    classifiers=(
        ["Development Status :: 4 - Beta",
         "Intended Audience :: Healthcare Industry",
         "Intended Audience :: Science/Research",
         "License :: OSI Approved :: Apache Software License",
         "Natural Language :: English",
         "Topic :: Scientific/Engineering :: Bio-Informatics",
         "Topic :: Scientific/Engineering :: Medical Science Apps."]
        + ["Programming Language :: Python :: " + str(v)
           for v in python_versions]),
    keywords='repository analysis neuroimaging workflows pipelines')
