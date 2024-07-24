
# Handling conda environment in Makefile:
# 	https://stackoverflow.com/questions/53382383/makefile-cant-use-conda-activate
# 	Makefile can't use `conda activate`

.ONESHELL:

SHELL = /bin/bash

CONDA_ACTIVATE = source $$(conda info --base)/etc/profile.d/conda.sh ; conda activate ; conda activate

db:
	sudo -u postgres bash -c "psql -h 127.0.0.1 -p 5432 < ./input/localhost.sql"
