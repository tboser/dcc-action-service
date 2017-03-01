#!/bin/bash

#exit script if we try to use an uninitialized variable.
set -o nounset
#exit the script if any statement returns a non-true return value
set -o errexit

#now=$(date +"%T")

mkdir -p /home/ubuntu/luigi_decider_runs/RNA-Seq_decider_runs

#Go into the appropriate folder
cd /home/ubuntu/luigi_decider_runs/RNA-Seq_decider_runs

#Activate the virtualenv
source /home/ubuntu/luigi_consonance_rnaseq_testing/luigienv/bin/activate

# run the decider
PYTHONPATH='.' luigi --module RNASeq3_1_x RNASeqCoordinator --redwood-client-path /home/ubuntu/ucsc-storage-client/ --redwood-host storage.ucsc-cgl.org --redwood-token $REDWOOD_ACCESS_TOKEN --es-index-host 172.31.25.227 --image-descriptor ~/gitroot/BD2KGenomics/dcc-dockstore-tool-runner/Dockstore.cwl --local-scheduler --tmp-dir /datastore --max-jobs 1 --tst-mode > log_RNA-Seq_3_1_3_decider_stdout.txt 2> log_RNA-Seq_3_1_3_decider_stderr.txt
#--test-mode  > >(tee stdout.txt) 2> >(tee stderr.txt >&2)

# deactivate virtualenv
deactivate
