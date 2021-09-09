import sys
import os.path
from setuptools import setup, find_packages

PACKAGE_NAME = 'arcana2'

# Get version from module inside package
sys.path.insert(0, os.path.join(os.path.dirname(__file__),
                                PACKAGE_NAME))
from __about__ import __version__, install_requires, tests_require  # noqa pylint: disable=no-name-in-module
sys.path.pop(0)


setup(
    name=PACKAGE_NAME,
    version=__version__,
    author='Tom G. Close',
    author_email='tom.g.close@gmail.com',
    packages=find_packages(),
    url='https://github.com/australian-imaging-service/arcana2',
    license='The Apache Software Licence 2.0',
    description=(
        'Abstraction of Repository-Centric ANAlysis framework'),
    long_description=open('README.rst').read(),
    install_requires=install_requires,
    tests_require=tests_require,
    entry_points={
        'console_scripts': ['arcana = arcana2.entrypoint.main:MainCmd.run']},
    extras_require={
        'test': tests_require},
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Healthcare Industry",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
        "Topic :: Scientific/Engineering :: Medical Science Apps."],
    keywords='repository analysis neuroimaging workflows pipelines')
