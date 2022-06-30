#!/bin/bash

cd /home/$DKUBE_USER_LOGIN_NAME

ls -a --ignore={.local,.cache,.ipynb_checkpoints,.jupyter,.rstudio,dataset,model,notebook,rstudio,workspace,.,..}  | xargs -I {} sudo chown -R $USER:$USER {}
