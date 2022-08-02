from pathlib import Path
PACKAGE_NAME = 'arcana'
PACKAGE_ROOT = Path(__file__).parent
CODE_URL = f'https://github.com/australian-imaging-service/{PACKAGE_NAME}'

__authors__ = [
    ("Thomas G. Close", "tom.g.close@gmail.com")]

install_requires = [
    'docker>=5.0.2',
    'jq>=1.2.2',
    'click==7.1.2',
    'PyYAML>=6.0',
    'natsort>=7.1.1',
    'fasteners>=0.7.0',
    'numexpr>=1.10.1',
    'importlib-metadata>=1.4',
    'deepdiff>=3.3',
    'pydicom>=1.0.2',
    'nibabel>=3.2.1',
    'neurodocker @ git+https://github.com/ReproNim/neurodocker.git@master',
    'xnat>=0.3.17',
    'pydra>=0.19',
    'pydra-dcm2niix>=1.1.0',
    'pydra-mrtrix3>=0.1',]

tests_require = [
    'pytest>=5.4.3',
    'pytest-env>=0.6.2',
    'pytest-cov>=2.12.1',
    'xnat4tests>=0.1',
    'medimages4tests>=0.1']

python_versions = ['3.8', '3.9']
