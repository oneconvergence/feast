# feast

## Installation steps

- Pre-requisite - Setup a virtual environment with python 3.9.7 or above.
- Steps:

        git clone https://github.com/oneconvergence/feast.git
        git checkout -b feast_changes origin/feast_changes
        cd feast/feast
        git submodule init
        git submodule update
        patch -p1 < ../feast.patch
        make install-python

- Set PYTHONPATH:

        export BASE=<path to above cloned feast dir>
        export PYTHONPATH=$BASE:$PYTHONPATH
        export PYTHONPATH=$BASE/provider/sdk/:$PYTHONPATH

- Set these environment variables:

        export DKUBE_ACCESS_URL=<Dkube access url>
        export DKUBE_ACCESS_TOKEN=<Dkube access token>
        export DKUBE_USER=<Dkube user name>
        export OFFLINE_DATASET=<Dataset for offline store>
        export ONLINE_DATASET=<Dataset for Dkube online store>
        export FEAST_ONLINE_SERVER_URL=<URL to access Feast Knative service>

   Note:- In Dkube environment, only __OFFLINE_DATASET__ needs to be set explicitly by user.

- That's it.
