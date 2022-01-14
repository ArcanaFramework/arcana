PACKAGE_NAME = 'arcana2'

__version__ = '2.0.0a0'

__authors__ = [
    ("Thomas G. Close", "tom.g.close@gmail.com")]

install_requires = [
    'xnat>=0.3.17',
    'pydra @ https://github.com/tclose/pydra/archive/serial-worker-fix.zip',
    'pydra-dcm2niix @ https://github.com/tclose/pydra-dcm2niix/archive/pydra-branch.zip',
    'pydra-mrtrix3 @ https://github.com/tclose/pydra-mrtrix3/archive/pydra-branch.zip',
    'pydicom>=1.0.2',
    'nibabel>=3.2.1',
    'natsort>=7.1.1',
    'fasteners>=0.7.0',
    'docker>=5.0.2',
    'neurodocker==0.7.0',
    'deepdiff>=3.3',
    # Tests
    'pytest>=5.4.3',
    'pytest-env>=0.6.2',
    'pytest-cov>=2.12.1',]

tests_require = []

python_versions = ['3.7', '3.8', '3.9']
