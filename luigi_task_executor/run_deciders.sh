#!/bin/bash

set -o errexit
set -o nounset

echo "Executing the run deciders script; log files are run_deciders_stderr.txt and run_deciders_stdout.txt"

#Go into the appropriate folder
cd /home/ubuntu/RNAseq3_0_x_testing
#Activate the virtualenv
source /home/ubuntu/luigi_consonance_rnaseq_testing/luigienv/bin/activate
# run the RNA-Seq 3.0.x decider
PYTHONPATH='.' luigi --module RNASeq3_0_xTask RNASeqCoordinator --redwood-client-path /home/ubuntu/ucsc-storage-client/ --redwood-host storage.ucsc-cgl.org --redwood-token $REDWOOD_ACCESS_TOKEN --es-index-host 172.31.25.227 --image-descriptor ~/gitroot/BD2KGenomics/dcc-dockstore-tool-runner/Dockstore.cwl --local-scheduler --tmp-dir /datastore --max-jobs 1  --test-mode > >(tee run_deciders_stdout.txt) 2> >(tee run_deciders_stderr.txt >&2)
# deactivate virtualenv
deactivate





