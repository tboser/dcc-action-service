import argparse
import datetime
import os.path
import subprocess
import sys 
from uuid import uuid4

'''
python wrapper_runner.py dockstore_URL filepath/to/JSON

Problems:
'''
desc = "Generic Dockstore Tool Wrapper and Redwood Uploader/Downloader"
parser = argparse.ArgumentParser(description=desc)
parser.add_argument('--ucsc_storage_client_path', type=str, required=True, 
					help='Path to the UCSC Storage Client')
parser.add_argument('--ucsc_storage_host', type=str, required=True, 
					help='Path to the UCSC Storage Host')
parser.add_argument('--input_filename', type=str, default="filename",
					help='Name of the input BAM file')
parser.add_argument('--json_path', type=str, default="filename", required=True, 
					help='Path to the json file containing inputs for BAMstats')
parser.add_argument('--uuid', type=str, default="NA", help='uuid')
parser.add_argument('--bundle_uuid', type=str, default="NA", help='bundle_uuid')
parser.add_argument('--parent_uuid', type=str, default="NA", help='parent_uuid')
parser.add_argument('--tmp_dir',  type=str, default='/tmp', 
					help='Name of temporary directory')
parser.add_argument('--data_dir', type=str, default='/tmp/data_dir', 
					help='Name for the data directory')

args.parse_args()

# just going to generate this UUID
# Ancestor of summer code. Will fix at later date.
upload_uuid = str(uuid4())
filepath = args.tmp_dir + "/" + args.bundle_uuid + "/" + args.filename

def download_redwood(ucsc_storage_client_path, ucsc_storage_host, data_dir, uuid, filepath):
    print "** DOWNLOADER **"
    cmd = "java -Djavax.net.ssl.trustStore=" + ucsc_storage_client_path + \
    	  "/ssl/cacerts -Djavax.net.ssl.trustStorePassword=changeit -Dmetadata.url="+\
    	  ucsc_storage_host + ":8444 -Dmetadata.ssl.enabled=true -Dclient.ssl.custom=false -Dstorage.url="+\
    	  ucsc_storage_host + ":5431 -DaccessToken=`cat " + ucsc_storage_client_path+\
    	  "/accessToken` -jar " + ucsc_storage_client_path + \
    	  "/icgc-storage-client-1.0.14-SNAPSHOT/lib/icgc-storage-client.jar download --output-dir "+\
    	  data_dir + " --object-id " + uuid + " --output-layout bundle"
    print cmd
    result = subprocess.call(cmd, shell=True)
    print "DOWNLOAD RESULT: " + str(result)
    if result == 0:
    	with open(filepath,'w') as p:
	        print >>p, "finished downloading"

def run_pipeline(data_dir, bundle_uuid, dockstore_URL, tmp_dir, filename, parent_uuid, upload_uuid, uuid, json_path):
        print "** RUNNING REPORT GENERATOR **"
        bundle_path = data_dir + "/" + bundle_uuid

        ''' FIXME: docker-machine is likely to break on Linux hosts '''
        #cmd = "eval $(docker-machine env default); dockstore tool launch --entry quay.io/briandoconnor/dockstore-tool-bamstats:1.25-5 --json %s/%s/params.json" % (self.tmp_dir, self.bundle_uuid)
        cmd = "cd %s && dockstore tool launch --entry %s --json %s" % \
        	  (bundle_path, dockstore_URL, json_path)
        print "CMD: " + cmd
        result = subprocess.call(cmd, shell=True)
        print "REPORT GENERATOR RESULT: " + str(result)

        if result == 0:
            # cleanup input
            cmd = "rm %s/%s/%s" % (data_dir, bundle_uuid, filename)
            print "CLEANUP CMD: " + cmd
            result = subprocess.call(cmd, shell=True)
            if result == 0:
                print "CLEANUP SUCCESSFUL"

            # generate timestamp
            ts_str = datetime.datetime.utcnow().isoformat()

            # now generate a metadata.json which is used for the next upload step

            metadata_filename = '%s/%s/metadata.json' % (tmp_dir, bundle_uuid)

            with open(metadata_filename, 'w') as f:
            print >>f, '''{
      "parent_uuids": [
        "%s"
      ],
      "analysis_type": "alignment_qc_report",
      "bundle_uuid": "%s",
      "workflow_name": "quay.io/briandoconnor/dockstore-tool-bamstats",
      "workflow_version": "1.25-5",
      "workflow_outputs": [
       {
        "file_path": "bamstats_report.zip",
        "file_type": "zip"
       }
      ],
      "workflow_inputs" : [
        {
          "file_storage_bundle_uri" : "%s",
          "file_storage_bundle_files" : [
            {
              "file_path": "%s",
              "file_type": "bam",
              "file_storage_uri": "%s"
            }
          ]
        }
      ],
      "timestamp": "%s"
    }''' % (parent_uuid, upload_uuid, bundle_uuid, filename, uuid, ts_str)

def upload_redwood(tmp_dir, bundle_uuid, upload_uuid, data_dir, ucsc_storage_client_path, ucsc_storage_host):
	# Package necessary file
        print "** UPLOADING **"
        prefix = "%s/%s" % (tmp_dir, bundle_uuid)
        cmd = '''mkdir -p %s/upload/%s %s/manifest/%s && ln -s %s/bamstats_report.zip %s/metadata.json %s/%s/upload/%s && \
echo "Register Uploads:" && \
java -Djavax.net.ssl.trustStore=%s/ssl/cacerts -Djavax.net.ssl.trustStorePassword=changeit -Dserver.baseUrl=%s:8444 -DaccessToken=`cat %s/accessToken` -jar %s/dcc-metadata-client-0.0.16-SNAPSHOT/lib/dcc-metadata-client.jar -i %s/upload/%s -o %s/manifest/%s -m manifest.txt && \
echo "Performing Uploads:" && \
java -Djavax.net.ssl.trustStore=%s/ssl/cacerts -Djavax.net.ssl.trustStorePassword=changeit -Dmetadata.url=%s:8444 -Dmetadata.ssl.enabled=true -Dclient.ssl.custom=false -Dstorage.url=%s:5431 -DaccessToken=`cat %s/accessToken` -jar %s/icgc-storage-client-1.0.14-SNAPSHOT/lib/icgc-storage-client.jar upload --force --manifest %s/manifest/%s/manifest.txt
''' % (prefix, upload_uuid, prefix, upload_uuid, data_dir, bundle_uuid, prefix, prefix, upload_uuid, ucsc_storage_client_path, ucsc_storage_host, ucsc_storage_client_path, ucsc_storage_client_path, prefix, upload_uuid, prefix, upload_uuid, ucsc_storage_client_path, ucsc_storage_host, ucsc_storage_host, ucsc_storage_client_path, ucsc_storage_client_path, prefix, upload_uuid)
        print "CMD: " + cmd
        result = subprocess.call(cmd, shell=True)
        if result == 0:
        	prefix = "%s/%s" % (data_dir, bundle_uuid)
            cmd = "rm -rf %s/bamstats_report.zip %s/datastore/" % (prefix, prefix)
            print "CLEANUP CMD: " + cmd
            result = subprocess.call(cmd, shell=True)
            if result == 0:
                print "CLEANUP SUCCESSFUL"

            filestr = '%s/uploaded' % (prefix)
        	with open(filestr, 'w') as f:
	            print >>f, "uploaded"

download_redwood(args.ucsc_storage_client_path, args.ucsc_storage_host, args.data_dir, args.uuid, filepath)
run_pipeline(args.data_dir, args.bundle_uuid, dockstore_URL, args.tmp_dir, args.filename, args.parent_uuid, upload_uuid, args.uuid, args.json_path)
upload_redwood(args.tmp_dir, args.bundle_uuid, upload_uuid, args.data_dir, args.ucsc_storage_client_path, args.ucsc_storage_host)