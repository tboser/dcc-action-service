import luigi
import json
import time
import re
import datetime
import subprocess
import base64
from urllib import urlopen

import uuid
from uuid import uuid4
from uuid import uuid5
import os
import sys
import copy

from elasticsearch import Elasticsearch

#for hack to get around non self signed certificates
import ssl

#Amazon S3 support for writing touch files to S3
from luigi.s3 import S3Target
#luigi S3 uses boto for AWS credentials
import boto

class ConsonanceTask(luigi.Task):
    redwood_host = luigi.Parameter("storage.ucsc-cgl.org")
    redwood_token = luigi.Parameter("must_be_defined")
    dockstore_tool_running_dockstore_tool = luigi.Parameter(default="quay.io/ucsc_cgl/dockstore-tool-runner:1.0.7")
    target_tool = luigi.Parameter(default="quay.io/ucsc_cgl/rnaseq-cgl-pipeline:3.0.2-3")
    target_tool_url = luigi.Parameter(default="https://dockstore.org/containers/quay.io/ucsc_cgl/rnaseq-cgl-pipeline")
    workflow_type = luigi.Parameter(default="rna_seq_quantification")
    image_descriptor = luigi.Parameter("must be defined")
 
    starfilename = luigi.Parameter(default="redwood://storage.ucsc-cgl.org/d0117ff1-cf53-43a0-aaab-cb15809fbb49/ca79c317-e410-591f-b802-3a0be6b658b7/starIndex_hg38_no_alt.tar.gz")
    rsemfilename = luigi.Parameter(default="redwood://storage.ucsc-cgl.org/d0117ff1-cf53-43a0-aaab-cb15809fbb49/b850460d-23c0-57a4-9d4b-af60726476a5/rsem_ref_hg38_no_alt.tar.gz")
    kallistofilename = luigi.Parameter(default="redwood://storage.ucsc-cgl.org/d0117ff1-cf53-43a0-aaab-cb15809fbb49/c92d30f3-2731-56b1-b8e4-41d09b1bb2dc/kallisto_hg38.idx")

    disable_cutadapt = luigi.Parameter(default="false")
    save_bam = luigi.Parameter(default="true")
    save_wiggle = luigi.Parameter(default="true")
    no_clean = luigi.Parameter(default="true")
    resume = luigi.Parameter(default="")
    cores = luigi.Parameter(default=36)
    bamqc = luigi.Parameter(default="true")

    paired_filenames = luigi.ListParameter(default=["must input sample files"])
    paired_file_uuids = luigi.ListParameter(default=["uuid"])
    paired_bundle_uuids = luigi.ListParameter(default=["bundle_uuid"])

    single_filenames = luigi.ListParameter(default=["must input sample files"])
    single_file_uuids = luigi.ListParameter(default=["uuid"])
    single_bundle_uuids = luigi.ListParameter(default=["bundle_uuid"])

    tar_filenames = luigi.ListParameter(default=["must input sample files"])
    tar_file_uuids = luigi.ListParameter(default=["uuid"])
    tar_bundle_uuids = luigi.ListParameter(default=["bundle_uuid"])

    parent_uuids = luigi.ListParameter(default=["parent_uuid"])

    tmp_dir = luigi.Parameter(default='/datastore')

    submitter_sample_id = luigi.Parameter(default='must input submitter sample id')
    meta_data = luigi.Parameter(default="must input metadata")
    touch_file_path = luigi.Parameter(default='must input touch file path')

    #Consonance will not be called in test mode
    test_mode = luigi.BooleanParameter(default = False)


    def run(self):
        print "\n\n\n** TASK RUN **"
        #get a unique id for this task based on the some inputs
        #this id will not change if the inputs are the same
        task_uuid = self.get_task_uuid()

        print "** MAKE TEMP DIR **"
        # create a unique temp dir
        cmd = '''mkdir -p %s/consonance-jobs/RNASeq_3_1_x_Coordinator/fastq_gz/%s/''' % (self.tmp_dir, task_uuid)
        print cmd
        result = subprocess.call(cmd, shell=True)
        if result != 0:
            print "PROBLEMS MAKING DIR!!"
        print "** MAKE JSON FOR WORKER **"
        # create a json for RNA-Seq which will be executed by the dockstore-tool-running-dockstore-tool and passed as base64encoded
        # will need to encode the JSON above in this: https://docs.python.org/2/library/base64.html
        # see http://luigi.readthedocs.io/en/stable/api/luigi.parameter.html?highlight=luigi.parameter
        # TODO: this is tied to the requirements of the tool being targeted
        json_str = '''
{
'''
        if len(self.paired_filenames) > 0:
            json_str += '''
"sample-paired": [
        '''
            i = 0
            while i<len(self.paired_filenames):
                # append file information
                json_str += '''
            {
              "class": "File",
              "path": "redwood://%s/%s/%s/%s"
            }''' % (self.redwood_host, self.paired_bundle_uuids[i], self.paired_file_uuids[i], self.paired_filenames[i])
                if i < len(self.paired_filenames) - 1:
                   json_str += ","
                i += 1
            json_str += '''
  ],
            '''

        if len(self.single_filenames) > 0:
            json_str += '''
"sample-single": [
        '''
            i = 0
            while i<len(self.single_filenames):
                # append file information
                json_str += '''
            {
               "class": "File",
               "path": "redwood://%s/%s/%s/%s"
            }''' % (self.redwood_host, self.single_bundle_uuids[i], self.single_file_uuids[i], self.single_filenames[i])
                if i < len(self.single_filenames) - 1:
                    json_str += ","
                i += 1
            json_str += '''
  ],
            '''

        if len(self.tar_filenames) > 0:
            json_str += '''
"sample-tar": [
        '''
            i = 0
            while i<len(self.tar_filenames):
                # append file information
                json_str += '''
            {
              "class": "File",
              "path": "redwood://%s/%s/%s/%s"
            }''' % (self.redwood_host, self.tar_bundle_uuids[i], self.tar_file_uuids[i], self.tar_filenames[i])
                if i < len(self.tar_filenames) - 1:
                    json_str += ","
                i += 1
            json_str += '''
  ],
            '''



        json_str += '''
"rsem":
  {
    "class": "File",
    "path": "%s"
  },
            ''' %  (self.rsemfilename)

        json_str += '''
"star":
  {
    "class": "File",
    "path": "%s"
  },
            ''' % (self.starfilename)


        json_str += '''
"kallisto":
  {
    "class": "File",
    "path": "%s"
  },
            ''' % (self.kallistofilename)

        json_str += '''
"save-wiggle": %s,
''' % self.save_wiggle

        json_str += '''
"no-clean": %s,
''' % self.no_clean

        json_str += '''
"save-bam": %s,
''' % self.save_bam

        json_str += '''
"disable-cutadapt": %s,
''' % self.disable_cutadapt

        json_str += '''
"resume": "%s",
''' % self.resume

        json_str += '''
"cores": %d,
''' % self.cores

        json_str += '''
"work-mount": "%s",
 ''' % self.tmp_dir

        json_str += '''
"bamqc": %s,
''' % self.bamqc

        json_str += '''
"output_files": [
        '''
        new_filename = self.parent_uuids[0] + '.tar.gz'
        json_str += '''
    {
      "class": "File",
      "path": "/tmp/%s"
    }''' % (new_filename)
 

        json_str += '''
  ]'''


        # if the user wants to save the wiggle output file
        if self.save_wiggle == 'true':
            json_str += ''',

"wiggle_files": [
        '''
            new_filename = self.parent_uuids[0] + '.wiggle.bg'
            json_str += '''
    {
      "class": "File",
      "path": "/tmp/%s"
    }''' % (new_filename)
 
            json_str += '''
  ]'''

        # if the user wants to save the BAM output file
        if self.save_bam == 'true':
            json_str += ''',

"bam_files": [
        '''
            new_filename = self.parent_uuids[0] + '.sortedByCoord.md.bam'
            json_str += '''
    {
      "class": "File",
      "path": "/tmp/%s"
    }''' % (new_filename)
 
            json_str += '''
  ]'''


        json_str += '''
}
'''

        print "THE JSON: "+json_str
        # now make base64 encoded version
        base64_json_str = base64.urlsafe_b64encode(json_str)
        print "** MAKE JSON FOR DOCKSTORE TOOL WRAPPER **"
        # create a json for dockstoreRunningDockstoreTool, embed the RNA-Seq JSON as a param
# below used to be a list of parent UUIDs; which is correct????
#            "parent_uuids": "[%s]",
        parent_uuids = ','.join(map("{0}".format, self.parent_uuids))

        print "parent uuids:%s" % parent_uuids

        p = self.save_dockstore_json().open('w')
        print >>p, '''{
            "json_encoded": "%s",
            "docker_uri": "%s",
            "dockstore_url": "%s",
            "redwood_token": "%s",
            "redwood_host": "%s",
            "parent_uuids": "%s",
            "workflow_type": "%s",
            "tmpdir": "%s",
            "vm_instance_type": "c4.8xlarge",
            "vm_region": "us-west-2",
            "vm_location": "aws",
            "vm_instance_cores": 36,
            "vm_instance_mem_gb": 60,
            "output_metadata_json": "/tmp/final_metadata.json"
        }''' % (base64_json_str, self.target_tool, self.target_tool_url, self.redwood_token, self.redwood_host, parent_uuids, self.workflow_type, self.tmp_dir )
        p.close()

        # execute consonance run, parse the job UUID
        cmd = ["consonance", "run", "--image-descriptor", self.image_descriptor, "--flavour", "c4.8xlarge", "--run-descriptor", p.path]
        # loop to check the consonance status until finished or failed
        #print "consonance status --job_uuid e2ad3160-74e2-4b04-984f-90aaac010db6"

        if self.test_mode == False:
            print "** SUBMITTING TO CONSONANCE **"
            print "executing:"+ ' '.join(cmd)
            print "** WAITING FOR CONSONANCE **"
            try:
                result = subprocess.call(cmd)
            except Exception as e:
                print "Error in Consonance call!!!:" + e.message

            if result == 0:
                print "Consonance job returned success code!"
                self.meta_data["consonance_id"] = '????'
            else:
                print "ERROR: Consonance job failed!!!"
        else:
            print "TEST MODE: Consonance command would be:"+ ' '.join(cmd)
            self.meta_data["consonance_id"] = 'no consonance id in test mode'


        #save the donor metadata for the sample being processed to the touch
        # file directory
        meta_data_json = json.dumps(self.meta_data)
        m = self.save_metadata_json().open('w')
        print >>m, meta_data_json
        m.close()

            
#        if result == 0:
#            cmd = "rm -rf "+self.data_dir+"/"+self.bundle_uuid+"/bamstats_report.zip "+self.data_dir+"/"+self.bundle_uuid+"/datastore/"
#            print "CLEANUP CMD: "+cmd
#            result = subprocess.call(cmd, shell=True)
#            if result == 0:
#                print "CLEANUP SUCCESSFUL"
         # NOW MAke a final report
        f = self.output().open('w')
        # TODO: could print report on what was successful and what failed?  Also, provide enough details like donor ID etc
        print >>f, "Consonance task is complete"
        f.close()
        print "\n\n\n\n** TASK RUN DONE **"


    def get_task_uuid(self):
        #get a unique id for this task based on the some inputs
        #this id will not change if the inputs are the same
        #This helps make the task idempotent; it that it
        #always has the same task id for the same inputs
        #TODO??? should this be based on all the inputs
        #including the path to star, kallisto, rsem and
        #save BAM, etc.???
        task_uuid = uuid5(uuid.NAMESPACE_DNS,  
                 self.target_tool + self.target_tool_url 
                 + ''.join(map("{0}".format, self.single_filenames))  
                 + ''.join(map("{0}".format, self.paired_filenames))
                 + ''.join(map("{0}".format, self.tar_filenames)) 
                 + ''.join(map("{0}".format, self.parent_uuids))  
                 + self.workflow_type + self.save_bam + self.save_wiggle + self.disable_cutadapt)
#        print("task uuid:%s",str(task_uuid))
        return task_uuid

    def save_metadata_json(self):
        #task_uuid = self.get_task_uuid()
        #return luigi.LocalTarget('%s/consonance-jobs/RNASeq_3_1_x_Coordinator/fastq_gz/%s/metadata.json' % (self.tmp_dir, task_uuid))
        #return S3Target('s3://cgl-core-analysis-run-touch-files/consonance-jobs/RNASeq_3_1_x_Coordinator/%s/metadata.json' % ( task_uuid))
        return S3Target('%s/%s_meta_data.json' % (self.touch_file_path, self.submitter_sample_id ))

    def save_dockstore_json(self):
        #task_uuid = self.get_task_uuid()
        #return luigi.LocalTarget('%s/consonance-jobs/RNASeq_3_1_x_Coordinator/fastq_gz/%s/dockstore_tool.json' % (self.tmp_dir, task_uuid))
        #return S3Target('s3://cgl-core-analysis-run-touch-files/consonance-jobs/RNASeq_3_1_x_Coordinator/%s/dockstore_tool.json' % ( task_uuid))
        return S3Target('%s/%s_dockstore_tool.json' % (self.touch_file_path, self.submitter_sample_id ))

    def output(self):
        #task_uuid = self.get_task_uuid()
        #return luigi.LocalTarget('%s/consonance-jobs/RNASeq_3_1_x_Coordinator/fastq_gz/%s/finished.txt' % (self.tmp_dir, task_uuid))
        #return S3Target('s3://cgl-core-analysis-run-touch-files/consonance-jobs/RNASeq_3_1_x_Coordinator/%s/finished.txt' % ( task_uuid))
        return S3Target('%s/%s_finished.json' % (self.touch_file_path, self.submitter_sample_id ))

class RNASeqCoordinator(luigi.Task):

    es_index_host = luigi.Parameter(default='localhost')
    es_index_port = luigi.Parameter(default='9200')
    redwood_token = luigi.Parameter("must_be_defined")
    redwood_client_path = luigi.Parameter(default='../ucsc-storage-client')
    redwood_host = luigi.Parameter(default='storage.ucsc-cgl.org')
    image_descriptor = luigi.Parameter("must be defined") 
    dockstore_tool_running_dockstore_tool = luigi.Parameter(default="quay.io/ucsc_cgl/dockstore-tool-runner:1.0.7")
    tmp_dir = luigi.Parameter(default='/datastore')
    max_jobs = luigi.Parameter(default='1')
    bundle_uuid_filename_to_file_uuid = {}
    process_sample_uuid = luigi.Parameter(default = "")

    touch_file_path_prefix = "s3://cgl-core-analysis-run-touch-files/consonance-jobs/RNASeq_3_1_x_Coordinator/"

    #Consonance will not be called in test mode
    test_mode = luigi.BooleanParameter(default = False)


    def requires(self):
        print "\n\n\n\n** COORDINATOR REQUIRES **"
        # now query the metadata service so I have the mapping of bundle_uuid & file names -> file_uuid
        print str("https://"+self.redwood_host+":8444/entities?page=0")

#hack to get around none self signed certificates
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        json_str = urlopen(str("https://"+self.redwood_host+":8444/entities?page=0"), context=ctx).read()
 
#        json_str = urlopen(str("https://"+self.redwood_host+":8444/entities?page=0")).read()
        metadata_struct = json.loads(json_str)
        print "** METADATA TOTAL PAGES: "+str(metadata_struct["totalPages"])
        for i in range(0, metadata_struct["totalPages"]):
            print "** CURRENT METADATA TOTAL PAGES: "+str(i)
            json_str = urlopen(str("https://"+self.redwood_host+":8444/entities?page="+str(i)), context=ctx).read()
#            json_str = urlopen(str("https://"+self.redwood_host+":8444/entities?page="+str(i))).read()
            metadata_struct = json.loads(json_str)
            for file_hash in metadata_struct["content"]:
                self.bundle_uuid_filename_to_file_uuid[file_hash["gnosId"]+"_"+file_hash["fileName"]] = file_hash["id"]

        # now query elasticsearch
        es = Elasticsearch([{'host': self.es_index_host, 'port': self.es_index_port}])
        # see jqueryflag_alignment_qc
        # curl -XPOST http://localhost:9200/analysis_index/_search?pretty -d @jqueryflag_alignment_qc
        res = es.search(index="analysis_index", body={"query" : {"bool" : {"should" : [{"term" : { "flags.normal_rna_seq_cgl_workflow_3_0_x" : "false"}},{"term" : {"flags.tumor_rna_seq_cgl_workflow_3_0_x" : "false" }}],"minimum_should_match" : 1 }}}, size=5000)

        listOfJobs = []

        print("Got %d Hits:" % res['hits']['total'])
        for hit in res['hits']['hits']:
            print("\n\n\nDonor uuid:%(donor_uuid)s Center Name:%(center_name)s Program:%(program)s Project:%(project)s" % hit["_source"])
            print("Got %d specimens:" % len(hit["_source"]["specimen"]))
            
#            if hit["_source"]["program"] != "SU2C" or hit["_source"]["project"] != "WCDT":
#                continue
#            if hit["_source"]["program"] != "Treehouse":
#                continue

            for specimen in hit["_source"]["specimen"]:
               print("Next sample of %d samples:" % len(specimen["samples"]))
               for sample in specimen["samples"]:
                   print("Next analysis of %d analysis:" % len(sample["analysis"]))
                   #if a particular sample uuid is requested for processing and
                   #the current sample uuid does not match go on to the next sample
                   if self.process_sample_uuid and (self.process_sample_uuid != sample["sample_uuid"]):
			continue

#should we only put the sample metatdata being processed, not the donor
#metadata in the touch file????

#                   sample_meta_data["center_name"] = hit["_source"]["center_name"]
#                   sample_meta_data["timestamp"] = hit["_source"]["timestamp"]
#                   sample_meta_data["project"] = hit["_source"]["project"]
#                   sample_meta_data["program"] = hit["_source"]["program"]
#                   sample_meta_data["submitter_donor_id"] = hit["_source"]["submitter_donor_id"]
#                   sample_meta_data["specimen"] = []
#                   sample_meta_data["submitter_specimen_id"] = specimen["submitter_specimen_id"]
#                   sample_meta_data["samples"] = [sample]
#                   meta_data = json.dumps(sample_meta_data)
#                   meta_data = json.dumps(hit["_source"])
#                   print "sample json:"
#                   print sample_json 
                  

                   for analysis in sample["analysis"]:
                        print "\nMetadata:submitter specimen id:"+specimen["submitter_specimen_id"]+" submitter sample id:"+sample["submitter_sample_id"]+" sample uuid:"+sample["sample_uuid"]+" analysis type:"+analysis["analysis_type"] 
                        print "normal RNA Seq quant:"+str(hit["_source"]["flags"]["normal_rna_seq_quantification"])+" tumor RNA Seq quant:"+str(hit["_source"]["flags"]["tumor_rna_seq_quantification"])
                        print "Specimen type:"+specimen["submitter_specimen_type"]+" Experimental design:"+str(specimen["submitter_experimental_design"]+" Analysis bundle uuid:"+analysis["bundle_uuid"])
                        print "Normal RNASeq 3.0.x flag:"+str(hit["_source"]["flags"]["normal_rna_seq_cgl_workflow_3_0_x"])+" Tumor RNASeq 3.0.x flag:"+str(hit["_source"]["flags"]["tumor_rna_seq_cgl_workflow_3_0_x"])
                        print "Normal missing items RNASeq 3.0.x:"+str(sample["sample_uuid"] in hit["_source"]["missing_items"]["normal_rna_seq_cgl_workflow_3_0_x"])
                        print "Tumor missing items RNASeq 3.0.x:"+str(sample["sample_uuid"] in hit["_source"]["missing_items"]["tumor_rna_seq_cgl_workflow_3_0_x"])
                        print "work flow outputs:"
                        for output in analysis["workflow_outputs"]:
                            print output                       
 
                        #find out if there were result files from the last RNA Seq pipeline
                        #run on this sample 
                        rna_seq_outputs_len = 0
                        for filter_analysis in sample["analysis"]:
                                if filter_analysis["analysis_type"] == "rna_seq_quantification":
                                    rna_seq_outputs = filter_analysis["workflow_outputs"] 
                                    print "rna seq workflow outputs:"
                                    print rna_seq_outputs
                                    rna_seq_outputs_len = len(filter_analysis["workflow_outputs"]) 
                                    print "len of rna_seq outputs is:"+str(rna_seq_outputs_len)      

                        if ( (analysis["analysis_type"] == "sequence_upload" and \
                              ((hit["_source"]["flags"]["normal_rna_seq_cgl_workflow_3_0_x"] == False and \
                                   sample["sample_uuid"] in hit["_source"]["missing_items"]["normal_rna_seq_cgl_workflow_3_0_x"] and \
                                   re.match("^Normal - ", specimen["submitter_specimen_type"]) and \
                                   re.match("^RNA-Seq$", specimen["submitter_experimental_design"])) or \
                               (hit["_source"]["flags"]["tumor_rna_seq_cgl_workflow_3_0_x"] == False and \
                                   sample["sample_uuid"] in hit["_source"]["missing_items"]["tumor_rna_seq_cgl_workflow_3_0_x"] and \
                                   re.match("^Primary tumour - |^Recurrent tumour - |^Metastatic tumour - |^Cell line -", specimen["submitter_specimen_type"]) and \
                                   re.match("^RNA-Seq$", specimen["submitter_experimental_design"])))) or \

                             #if the workload has already been run but we have no
                             #output from the workload run it again
                             (analysis["analysis_type"] == "sequence_upload" and \
                              ((hit["_source"]["flags"]["normal_rna_seq_cgl_workflow_3_0_x"] == True and \
                                   (sample["sample_uuid"] in hit["_source"]["missing_items"]["normal_rna_seq_cgl_workflow_3_0_x"] or \
                                   (sample["sample_uuid"] in hit["_source"]["present_items"]["normal_rna_seq_cgl_workflow_3_0_x"] and rna_seq_outputs_len == 0)) and \
                                   re.match("^Normal - ", specimen["submitter_specimen_type"]) and \
                                   re.match("^RNA-Seq$", specimen["submitter_experimental_design"])) or \
                               (hit["_source"]["flags"]["tumor_rna_seq_cgl_workflow_3_0_x"] == True and \
                                   (sample["sample_uuid"] in hit["_source"]["missing_items"]["tumor_rna_seq_cgl_workflow_3_0_x"] or \
                                   (sample["sample_uuid"] in hit["_source"]["present_items"]["tumor_rna_seq_cgl_workflow_3_0_x"] and rna_seq_outputs_len == 0)) and \
                                   re.match("^Primary tumour - |^Recurrent tumour - |^Metastatic tumour - |^Cell line -", specimen["submitter_specimen_type"]) and \
                                   re.match("^RNA-Seq$", specimen["submitter_experimental_design"])))) ):

                            
                            touch_file_path = self.touch_file_path_prefix+"/"+hit["_source"]["center_name"]+"_"+hit["_source"]["program"] \
                                                                    +"_"+hit["_source"]["project"]+"_"+hit["_source"]["submitter_donor_id"] \
                                                                    +"_"+specimen["submitter_specimen_id"]
                            submitter_sample_id = sample["submitter_sample_id"]

                            #make a copy of the metadata for this donor submission and remove
                            #all the sample information except for the sample we are going to
                            #process in the next job submission
                            #also remove other unneeded data such as 'present items' and 
                            #'missing items' etc.
                            #This metadata will be passed to the Consonance Task and some
                            #some of the meta data will be used in the Luigi status page for the job
                            meta_data = {}
                            meta_data = copy.deepcopy(hit["_source"])
    
                            del meta_data["present_items"]
                            del meta_data["missing_items"]
                            for meta_data_specimen in meta_data["specimen"]:
                                found_sample_in_specimen = False
                                for meta_data_sample in meta_data_specimen["samples"]:
                                    if meta_data_sample["sample_uuid"] != sample["sample_uuid"]:
                                        print "\n\ndeleting meta data sample:"+json.dumps(meta_data_sample)
                                        print "\n\n"
                                        meta_data_specimen["samples"].remove(meta_data_sample)
                                    else:
                                        found_specimen_in_sample = True
                                if found_sample_in_specimen is False:
                                    meta_data["specimen"].remove(meta_data_specimen)
                                    print "\n\ndeleting meta data specimen:"+json.dumps(meta_data_specimen)
                                    print "\n\n"

#                            meta_data_json = json.dumps(meta_data)
#                            print "meta data:"
#                            print meta_data_json


                            #print analysis
                            print "HIT!!!! "+analysis["analysis_type"]+" "+str(hit["_source"]["flags"]["normal_rna_seq_quantification"])+" "+str(hit["_source"]["flags"]["tumor_rna_seq_quantification"])+" "+specimen["submitter_specimen_type"]+" "+str(specimen["submitter_experimental_design"])


                            paired_files = []
                            paired_file_uuids = []
                            paired_bundle_uuids = []

                            single_files = []
                            single_file_uuids = []
                            single_bundle_uuids = []

                            tar_files = []
                            tar_file_uuids = []
                            tar_bundle_uuids = []

                            parent_uuids = {}

                            for file in analysis["workflow_outputs"]:
                                print "file type:"+file["file_type"]
                                print "file name:"+file["file_path"]

                                if (file["file_type"] == "fastq" or
                                    file["file_type"] == "fastq.gz"):
                                        #if there is only one sequenc upload output then this must
                                        #be a single read sample
                                        if( len(analysis["workflow_outputs"]) == 1): 
                                            print "adding %s of file type %s to files list" % (file["file_path"], file["file_type"])
                                            single_files.append(file["file_path"])
                                            single_file_uuids.append(self.fileToUUID(file["file_path"], analysis["bundle_uuid"]))
                                            single_bundle_uuids.append(analysis["bundle_uuid"])
                                            parent_uuids[sample["sample_uuid"]] = True
                                        #otherwise we must be dealing with paired reads
                                        else: 
                                            print "adding %s of file type %s to files list" % (file["file_path"], file["file_type"])
                                            paired_files.append(file["file_path"])
                                            paired_file_uuids.append(self.fileToUUID(file["file_path"], analysis["bundle_uuid"]))
                                            paired_bundle_uuids.append(analysis["bundle_uuid"])
                                            parent_uuids[sample["sample_uuid"]] = True
                                elif (file["file_type"] == "fastq.tar"):
                                    print "adding %s of file type %s to files list" % (file["file_path"], file["file_type"])
                                    tar_files.append(file["file_path"])
                                    tar_file_uuids.append(self.fileToUUID(file["file_path"], analysis["bundle_uuid"]))
                                    tar_bundle_uuids.append(analysis["bundle_uuid"])
                                    parent_uuids[sample["sample_uuid"]] = True

                            if len(listOfJobs) < int(self.max_jobs) and (len(paired_files) + len(tar_files) + len(single_files)) > 0:

                                 if len(tar_files) > 0 and (len(paired_files) > 0 or len(single_files) > 0):
                                     print >> sys.stderr, ('\n\nWARNING: mix of tar files and fastq(.gz) files submitted for' 
                                                        ' input for one sample! This is probably an error!')
                                     print >> sys.stderr, ('WARNING: files were\n paired %s\n tar: %s\n single:%s') % (paired_files, tar_files, single_files)
                                     print >> sys.stderr, ('WARNING: sample uuid:%s') % parent_uuids.keys()[0]
                                     print >> sys.stderr, ('WARNING: Skipping this job!\n\n')
                                     continue

                                 elif len(paired_files) > 0 and len(single_files) > 0:
                                     print >> sys.stderr, ('\n\nWARNING: mix of single and paired fastq(.gz) files submitted for'
                                                    ' input for one sample! This is probably an error!')
                                     print >> sys.stderr, ('WARNING: files were\n paired %s\n single:%s') % (paired_files,  single_files)
                                     print >> sys.stderr, ('WARNING: sample uuid:%s\n') % parent_uuids.keys()[0]
                                     print >> sys.stderr, ('WARNING: Skipping this job!\n\n')
                                     continue
 
                                 elif len(tar_files) > 1:
                                     print >> sys.stderr, ('\n\nWARNING: More than one tar file submitted for'
                                                    ' input for one sample! This is probably an error!')
                                     print >> sys.stderr, ('WARNING: files were\n tar: %s') % tar_files
                                     print >> sys.stderr, ('WARNING: sample uuid:%s') % parent_uuids.keys()[0]
                                     print >> sys.stderr, ('WARNING: Skipping this job!\n\n')
                                     continue 

                                 elif len(paired_files) % 2 != 0:
                                     print >> sys.stderr, ('\n\nWARNING: Odd number of paired files submitted for'
                                                    ' input for one sample! This is probably an error!')
                                     print >> sys.stderr, ('WARNING: files were\n paired: %s') % paired_files
                                     print >> sys.stderr, ('WARNING: sample uuid:%s') % parent_uuids.keys()[0]
                                     print >> sys.stderr, ('WARNING: Skipping this job!\n\n')
                                     continue 

                                 else:
                                    print "will run report for %s and %s and %s" % (paired_files, tar_files, single_files)
                                    print "total of %d files in this %s job; job %d of %d" % (len(paired_files) + (len(tar_files) + len(single_files)), hit["_source"]["program"], len(listOfJobs)+1, int(self.max_jobs))
                                    listOfJobs.append(ConsonanceTask(redwood_host=self.redwood_host, redwood_token=self.redwood_token, \
                                         image_descriptor=self.image_descriptor, dockstore_tool_running_dockstore_tool=self.dockstore_tool_running_dockstore_tool, \
                                         parent_uuids = parent_uuids.keys(), \
                                         single_filenames=single_files, single_file_uuids = single_file_uuids, single_bundle_uuids = single_bundle_uuids, \
                                         paired_filenames=paired_files, paired_file_uuids = paired_file_uuids, paired_bundle_uuids = paired_bundle_uuids, \
                                         tar_filenames=tar_files, tar_file_uuids = tar_file_uuids, tar_bundle_uuids = tar_bundle_uuids, \
                                         tmp_dir=self.tmp_dir, submitter_sample_id = submitter_sample_id, meta_data = meta_data, \
                                         touch_file_path = touch_file_path, test_mode=self.test_mode))
        print "total of %d jobs; max jobs allowed is %d\n\n" % (len(listOfJobs), int(self.max_jobs))

        # these jobs are yielded to
        print "\n\n** COORDINATOR REQUIRES DONE!!! **"
        return listOfJobs

    def run(self):
        print "\n\n\n\n** COORDINATOR RUN **"
         # now make a final report
        f = self.output().open('w')
        # TODO: could print report on what was successful and what failed?  Also, provide enough details like donor ID etc
        print >>f, "batch is complete"
        f.close()
        print "\n\n\n\n** COORDINATOR RUN DONE **"

    def output(self):
        print "\n\n\n\n** COORDINATOR OUTPUT **"
        # the final report
        ts = time.time()
        ts_str = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d_%H:%M:%S')
        #return luigi.LocalTarget('%s/consonance-jobs/RNASeq_3_1_x_Coordinator/RNASeqTask-%s.txt' % (self.tmp_dir, ts_str))
        return S3Target('s3://cgl-core-analysis-run-touch-files/consonance-jobs/RNASeq_3_1_x_Coordinator/RNASeqTask-%s.txt' % (ts_str))

    def fileToUUID(self, input, bundle_uuid):
        return self.bundle_uuid_filename_to_file_uuid[bundle_uuid+"_"+input]
        #"afb54dff-41ad-50e5-9c66-8671c53a278b"


if __name__ == '__main__':
    luigi.run()