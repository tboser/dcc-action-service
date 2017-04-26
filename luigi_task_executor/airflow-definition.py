"""
A test to see if the RNA-seq workflow can be run using airflow.
This file will be a "base" for workflows using airflow?
"""

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta

default_args = {
	'es_index_host' : 'localhost',
	'es_index_port' : '9200',
	'redwood_token' : 'must_be_defined',
	'redwood_host' : 'storage.ucsc-cgl.org',
	'image_descriptor' : 'must_be_defined',
	'dockstore_tool_running_dockstore_tool' : 'quay.io/ucsc_cgl/dockstore-tool-runner:1.0.8',
	'tmp_dir' : '/datastore',
	'max_jobs' : 1,
	'touch_file_bucket' : 'must_be_defined',
	'custom_es' : False,
	'custom_es_search' : 'must_be_defined_if_used',
	'test_mode' : False,
}

base_dag = DAG(
	dag_id='base_dag',
	default_args=default_args,
	schedule_interval=timedelta(hours=1)
)

#TODO : figure out structure for workflows
scan_metadata = BashOperator(
	""" stuff goes here """
)

run_dockstore = BashOperator(
	""" stuff here """
)

run_consonance = BashOperator(
	""" stuff here """
)