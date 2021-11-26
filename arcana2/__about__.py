PACKAGE_NAME = 'arcana2'

__version__ = '2.0.0a0'

__authors__ = [
    ("Thomas G. Close", "tom.g.close@gmail.com")]

install_requires = [
    'xnat>=0.3.17',
    'pydra>=0.14.1',
    'pydicom>=1.0.2',
    'nibabel>=3.2.1',
    'pydra-mrtrix3',
    'pydra-dcm2niix',
    'natsort>=7.1.1',
    'fasteners>=0.7.0',
    'docker>=5.0.2',
    'neurodocker==0.7.0',
    'deepdiff>=3.3',
    'tqdm>=4.25.0',
    'pytest>=5.4.3',]


tests_require = [
    'pytest-env>=0.6.2',
    'pytest-cov>=2.12.1',
    'nibabel>=3.2.1']

python_versions = ['3.6', '3.7', '3.8', '3.9']
