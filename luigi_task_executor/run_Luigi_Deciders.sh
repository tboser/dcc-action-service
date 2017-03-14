#!/bin/bash

#exit script if we try to use an uninitialized variable.
set -o nounset
#exit the script if any statement returns a non-true return value
set -o errexit

VIRTUAL_ENV_PATH=/home/ubuntu/luigi_consonance_rnaseq_testing/luigienv/bin
LUIGI_RUNS_PATH=/home/ubuntu/luigi_decider_runs/RNA-Seq_decider_runs
DECIDER_SOURCE_PATH=/home/ubuntu/RNAseq3_0_x_testing

now=$(date +"%T")

#mkdir -p ${LUIGI_RUNS_PATH}


#Go into the appropriate folder
cd /home/ubuntu/luigi_decider_runs/RNA-Seq_decider_runs

#Activate the virtualenv
set +o nounset
source "${VIRTUAL_ENV_PATH}"/activate
set +o nounset

REDWOOD_ACCESS_TOKEN=$(<${LUIGI_RUNS_PATH}/redwood_access_token.txt)

# run the decider
PYTHONPATH=${DECIDER_SOURCE_PATH} luigi --module RNASeq3_1_x RNASeqCoordinator --redwood-client-path /home/ubuntu/ucsc-storage-client/ --redwood-host storage.ucsc-cgl.org --redwood-token $REDWOOD_ACCESS_TOKEN --es-index-host 172.31.25.227 --image-descriptor ~/gitroot/BD2KGenomics/dcc-dockstore-tool-runner/Dockstore.cwl --local-scheduler --tmp-dir /datastore --max-jobs 50 > cron_log_RNA-Seq_decider_stdout.txt 2> ${LUIGI_RUNS_PATH}/cron_log_RNA-Seq_decider_stderr.txt
#--test-mode  > >(tee stdout.txt) 2> >(tee stderr.txt >&2)
#echo "${now} DEBUG!! run of lugi decider!!! redwood token is ${REDWOOD_ACCESS_TOKEN}" > ${LUIGI_RUNS_PATH}/logfile.txt


# deactivate virtualenv
set +o nounset
deactivate
set -o nounset

