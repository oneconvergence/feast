# feast

## Installation steps

- Pre-requisite - Setup a virtual environment with python 3.9.7 or above.
- Steps:

        git clone https://github.com/oneconvergence/feast.git
        git checkout -b feast_changes origin/feast_changes
        cd feast/feast
        git submodule init
        git submodule update
        patch -p0 < ../feast.patch
        make install-python

- Set PYTHONPATH:

        export BASE=<path to above cloned feast dir>
        export PYTHONPATH=$BASE:$PYTHONPATH
        export PYTHONPATH=$BASE/provider/sdk/:$PYTHONPATH

- That's it.
