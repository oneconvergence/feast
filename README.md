# feast

## Run online service as Knative service

        1. Build image
                git clone https://github.com/oneconvergence/feast.git
                git checkout -b online_server_changes origin/online_server_changes
                cd feast/
                docker build -i <image name> .

        2. Run Knative service
                Edit file:
                        online_server/online_svc.yaml
                kubectl apply -f online_server/online_svc.yaml
                kubectl get ksvc


## Installation steps to run local dev environment

- Pre-requisite - Setup a virtual environment with python 3.9.7 or above.
- Steps:

        git clone https://github.com/oneconvergence/feast.git
        git checkout -b online_server_changes origin/online_server_changes
        cd feast/feast
        git submodule init
        git submodule update
        patch -p1 < ../feast_ol_server.patch
        make install-python

- Set PYTHONPATH:

        export BASE=<path to above cloned feast dir>
        export PYTHONPATH=$BASE:$PYTHONPATH
        export PYTHONPATH=$BASE/provider/sdk/:$PYTHONPATH

- That's it.

## Generate repo configuraton

        cd online_server
        python -m common.utils.utils $(pwd)/config/cfg.json <name of o/p file>
