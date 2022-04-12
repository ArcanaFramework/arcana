from click.testing import CliRunner
from neurodocker.cli.generate import generate

runner = CliRunner()
result = runner.invoke(
    generate, [
        'docker',
        "--pkg-manager", "apt",
        "--base-image", "debian:buster-slim",
        '--arg', 'DEBIAN_FRONTEND=noninteractive',
        '--copy', "boo.txt", "/test.txt",
        '--run', 'chown -R neuro /home/neuro/nipype_tutorial',
        '--install', 'convert3d', 'ants', 'fsl', 'gcc', 'g++', 'graphviz', 'tree', 'git-annex-standalone', 'vim', 'emacs-nox', 'nano', 'less', 'ncdu', 'tig', 'git-annex-remote-rclone', 'octave', 'netbase',
        '--spm12', 'version=r7771',
        '--miniconda', 'version=latest',
        'conda_install="python=3.8 pytest jupyter jupyterlab jupyter_contrib_nbextensions traits pandas matplotlib scikit-learn scikit-image seaborn nbformat nb_conda"',
        'pip_install="https://github.com/nipy/nipype/tarball/master https://github.com/INCF/pybids/tarball/master nilearn datalad[full] nipy duecredit nbval"'])

