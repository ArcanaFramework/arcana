# Timestamp: 2021/11/15 10:16:57 UTC
# 
# Thank you for using Neurodocker. If you discover any issues
# or ways to improve this software, please submit an issue or
# pull request on our GitHub repository:
# 
#     https://github.com/ReproNim/neurodocker

FROM debian:bullseye

USER root

ARG DEBIAN_FRONTEND="noninteractive"

ENV LANG="en_US.UTF-8" \
    LC_ALL="en_US.UTF-8" \
    ND_ENTRYPOINT="/neurodocker/startup.sh"
RUN export ND_ENTRYPOINT="/neurodocker/startup.sh" \
    && apt-get update -qq \
    && apt-get install -y -q --no-install-recommends \
           apt-utils \
           bzip2 \
           ca-certificates \
           curl \
           locales \
           unzip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen \
    && dpkg-reconfigure --frontend=noninteractive locales \
    && update-locale LANG="en_US.UTF-8" \
    && chmod 777 /opt && chmod a+s /opt \
    && mkdir -p /neurodocker \
    && if [ ! -f "$ND_ENTRYPOINT" ]; then \
         echo '#!/usr/bin/env bash' >> "$ND_ENTRYPOINT" \
    &&   echo 'set -e' >> "$ND_ENTRYPOINT" \
    &&   echo 'export USER="${USER:=`whoami`}"' >> "$ND_ENTRYPOINT" \
    &&   echo 'if [ -n "$1" ]; then "$@"; else /usr/bin/env bash; fi' >> "$ND_ENTRYPOINT"; \
    fi \
    && chmod -R 777 /neurodocker && chmod a+s /neurodocker

ENTRYPOINT ["/neurodocker/startup.sh"]

RUN apt-get update -qq \
    && apt-get install -y -q --no-install-recommends \
           git \
           vim \
           ssh-client \
           python3 \
           python3-pip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY ["./arcana2", "/python-packages/arcana2"]

COPY ["./pydra", "/python-packages/pydra"]

COPY ["./pydra-mrtrix3", "/python-packages/pydra-mrtrix3"]

COPY ["./pydra-dcm2niix", "/python-packages/pydra-dcm2niix"]

ENV CONDA_DIR="/opt/miniconda-latest" \
    PATH="/opt/miniconda-latest/bin:$PATH"
RUN export PATH="/opt/miniconda-latest/bin:$PATH" \
    && echo "Downloading Miniconda installer ..." \
    && conda_installer="/tmp/miniconda.sh" \
    && curl -fsSL --retry 5 -o "$conda_installer" https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh \
    && bash "$conda_installer" -b -p /opt/miniconda-latest \
    && rm -f "$conda_installer" \
    && conda update -yq -nbase conda \
    && conda config --system --prepend channels conda-forge \
    && conda config --system --set auto_update_conda false \
    && conda config --system --set show_channel_urls true \
    && sync && conda clean -y --all && sync \
    && conda create -y -q --name arcana \
    && conda install -y -q --name arcana \
           "python=3.9" \
           "numpy" \
           "traits" \
    && sync && conda clean -y --all && sync \
    && bash -c "source activate arcana \
    &&   pip install --no-cache-dir  \
             "/python-packages/arcana2" \
             "/python-packages/pydra" \
             "/python-packages/pydra-mrtrix3" \
             "/python-packages/pydra-dcm2niix"" \
    && rm -rf ~/.cache/pip/* \
    && sync

LABEL maintainer="some.one@an.org"

RUN echo '{ \
    \n  "pkg_manager": "apt", \
    \n  "instructions": [ \
    \n    [ \
    \n      "base", \
    \n      "debian:bullseye" \
    \n    ], \
    \n    [ \
    \n      "install", \
    \n      [ \
    \n        "git", \
    \n        "vim", \
    \n        "ssh-client", \
    \n        "python3", \
    \n        "python3-pip" \
    \n      ] \
    \n    ], \
    \n    [ \
    \n      "copy", \
    \n      [ \
    \n        "./arcana2", \
    \n        "/python-packages/arcana2" \
    \n      ] \
    \n    ], \
    \n    [ \
    \n      "copy", \
    \n      [ \
    \n        "./pydra", \
    \n        "/python-packages/pydra" \
    \n      ] \
    \n    ], \
    \n    [ \
    \n      "copy", \
    \n      [ \
    \n        "./pydra-mrtrix3", \
    \n        "/python-packages/pydra-mrtrix3" \
    \n      ] \
    \n    ], \
    \n    [ \
    \n      "copy", \
    \n      [ \
    \n        "./pydra-dcm2niix", \
    \n        "/python-packages/pydra-dcm2niix" \
    \n      ] \
    \n    ], \
    \n    [ \
    \n      "miniconda", \
    \n      { \
    \n        "create_env": "arcana", \
    \n        "conda_install": [ \
    \n          "python=3.9", \
    \n          "numpy", \
    \n          "traits" \
    \n        ], \
    \n        "pip_install": [ \
    \n          "/python-packages/arcana2", \
    \n          "/python-packages/pydra", \
    \n          "/python-packages/pydra-mrtrix3", \
    \n          "/python-packages/pydra-dcm2niix" \
    \n        ] \
    \n      } \
    \n    ], \
    \n    [ \
    \n      "label", \
    \n      { \
    \n        "maintainer": "some.one@an.org" \
    \n      } \
    \n    ] \
    \n  ] \
    \n}' > /neurodocker/neurodocker_specs.json