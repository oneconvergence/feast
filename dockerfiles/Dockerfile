FROM ubuntu:20.04

USER root
WORKDIR /root

ARG PYTHON_VERSION_TAG=3.9.10
ARG LINK_PYTHON_TO_PYTHON3=1

SHELL [ "/bin/bash", "-c" ]

RUN apt-get -qq -y update && \
    DEBIAN_FRONTEND=noninteractive apt-get -qq -y install \
        gcc \
        g++ \
        zlibc \
        zlib1g-dev \
        libssl-dev \
        libbz2-dev \
        libsqlite3-dev \
        libncurses5-dev \
        libgdbm-dev \
        libgdbm-compat-dev \
        liblzma-dev \
        libreadline-dev \
        libnss3-dev \
        uuid-dev \
        libffi-dev \
        tk-dev \
        wget \
        curl \
        git \
        make \
        sudo \
        bash-completion \
        tree \
        vim \
        software-properties-common && \
    mv /usr/bin/lsb_release /usr/bin/lsb_release.bak && \
    apt-get -y autoclean && \
    apt-get -y autoremove && \
    rm -rf /var/lib/apt/lists/*

COPY dockerfiles/install_python.sh install_python.sh
RUN bash install_python.sh ${PYTHON_VERSION_TAG} ${LINK_PYTHON_TO_PYTHON3} && \
    rm -r install_python.sh Python-${PYTHON_VERSION_TAG}

RUN cd /opt \
    && git clone https://github.com/oneconvergence/feast.git \
    && cd feast \
    && git checkout -t origin/feast_changes \
    && git submodule init \
    && git submodule update \
    && pip install -r feast_requirements.txt \
    && pip install -r requirements.txt \
    && cd feast \
    && patch -p1 < ../feast_latest.patch \
    && python setup.py install \
    && cd /opt/feast \
    && bash dockerfiles/copy_dkube_template.sh
    #&& make install-python

ENV PYTHONPATH "${PYTHONPATH}:/opt/feast/:/opt/feast/provider/sdk"

RUN pip install git+https://github.com/oneconvergence/dkube.git@feast_changes

# Installing jupyterlab
RUN pip3 install jupyterlab==1.2.4 && \
    pip3 install jupyterlab[extras] && \
    pip3 install jupyterlab-github==1.0.0 && \
    pip3 install jupyterlab_github==1.0.0

RUN apt update && apt install nodejs npm -y

# Installing git extension for jupyterlab
RUN cd /usr/local/src && mkdir jupyterlab_git && cd jupyterlab_git && \
    git clone https://github.com/jupyterlab/jupyterlab-git.git -b v0.10.0 && \
    python3 -m pip install jupyterlab-git/ && \
    jupyter lab build

RUN jupyter labextension install @jupyterlab/git@^0.10.0
RUN jupyter labextension install @jupyterlab/github
RUN jupyter labextension install @jupyterlab/google-drive

RUN jupyter serverextension enable --py jupyterlab_git --system

RUN jupyter serverextension enable --py jupyterlab_github --system

RUN useradd -m dkube -u 5123 -U && echo "dkube:dkube" | chpasswd && adduser dkube sudo
RUN usermod -aG sudo,root dkube
RUN echo 'dkube ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

LABEL heritage="dkube"
RUN python3 -m ipykernel install --user
ENV SHELL=/bin/bash
RUN jupyter notebook --generate-config
RUN git config --global user.email "dkube@oneconvergence.com"
RUN git config --global user.name "dkube"
ENV DKUBE_NB_ARGS ""
ADD dockerfiles/notebook_startup.sh /
RUN chmod +x /notebook_startup.sh
CMD ["/notebook_startup.sh"]

# CMD ["jupyter", "lab", "--ip=0.0.0.0", "--allow-root"]
