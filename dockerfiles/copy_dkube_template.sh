#!/bin/bash

set -ex

# This is used for temporary purpose only. Currently, feast installation script
# doesn't copy dkube template to standard install location. The installation
# script need to be patched accordingly and then this file needs to get deleted.
std_loc=/usr/lib/python3.9/site-packages
fegg=$(ls /usr/lib/python3.9/site-packages | grep feast)
echo $fegg

dst="${std_loc}/${fegg}/feast/templates/"
src=/opt/feast/feast/sdk/python/feast/templates/dkube/

sudo cp -r $src $dst
ls $dst
