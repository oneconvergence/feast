#!/bin/bash
set -e

source /etc/dkube/dkube.env

cd /home/$DKUBE_USER_LOGIN_NAME

if $EXTERNAL_HOMEDIR; then
	ln -sfT /mnt/dkube/home/$DKUBE_USER_LOGIN_NAME /home/$DKUBE_USER_LOGIN_NAME/dkube_home
fi

#Check if USER_UID and USER_GID are empty. Assign values when condition is true.
if [[ -z "$USER_UID" && -z "$USER_GID" ]]; then
	USER_UID=1000
	USER_GID=$USER_UID
fi
if ! grep -q $USER_GID /etc/group ; then
	groupadd -g $USER_GID $DKUBE_USER_LOGIN_NAME
fi
adduser --disabled-password --uid $USER_UID --gid $USER_GID --gecos '' $DKUBE_USER_LOGIN_NAME
adduser $DKUBE_USER_LOGIN_NAME sudo
echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
su $DKUBE_USER_LOGIN_NAME -c "cd /home/$DKUBE_USER_LOGIN_NAME; source /etc/profile; jupyter lab --ip=0.0.0.0 --port=8888 --allow-root $DKUBE_NB_ARGS"
