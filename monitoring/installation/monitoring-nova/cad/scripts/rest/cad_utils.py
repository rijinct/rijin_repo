from flask import Flask, jsonify
from flask import send_file, Response, request
from zipfile import ZipFile
import zipfile
import glob
import os
import json
import sys
sys.path.insert(0,'/opt/nsn/ngdb/monitoring/cad/scripts/')
sys.path.insert(0,'/opt/nsn/ngdb/monitoring/utils')
from logger_util import *
import constants
from pathlib import Path
import subprocess

create_logger()
LOGGER = Logger.getLogger(__name__)
default_log_level = get_default_log_level()
LOGGER.setLevel(get_logging_instance(default_log_level))

class CADConfigUtil:

    def get_output_xmls(date_format,metadata):
        LOGGER.info("Retrieving XMLs")
        try:
            if date_format == "latest":
                file_pattern = metadata+'Results*'
            else:
                file_pattern = metadata + 'Results_'+date_format+'_*'
            output_dir = "/opt/nsn/ngdb/monitoring/output/cad/"+metadata+"/"
            if os.listdir(output_dir):
                output_file = glob.glob(output_dir + file_pattern)
                CADConfigUtil.zip_file(output_file)

            return send_file(constants.output_directory + 'output.zip',
                             mimetype='zip',
                             attachment_filename='output.zip',
                             as_attachment=True)
        except ValueError as e:
            message = json.dumps(
                {"response": "Requested output xmls are not found for given date:{0}".format(date_format)})
            resp = Response(message, status=400, mimetype='application/json')
            return resp
            
    def zip_file(output_file):
        zip_obj = ZipFile(constants.output_directory+'output.zip', 'w', compression=zipfile.ZIP_DEFLATED)
        final_export_file_path = max(output_file, key=os.path.getctime)
        final_export_file= final_export_file_path.split(os.sep)[-1]
        LOGGER.info("final_export_file:{}".format(final_export_file))
        zip_obj.write(final_export_file_path,final_export_file)
        zip_obj.close()
        
    def put_custom_xsl(file_type,image_file):
        file_path = constants.custom_xsl_path
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        required_file_name = file_type + '.xsl'

        if image_file is None or image_file.filename.split(".")[1] != "xsl":
            message = json.dumps(
                {"response": "Required file {} neither uploaded nor in required format".format(required_file_name)})
            resp = Response(message, status=400, mimetype='application/json')
            return resp
        else:           
            image_file.save(file_path + required_file_name)
            os.system('dos2unix {}'.format(file_path + required_file_name))
            message = json.dumps(
                {"response": "{} successfully uploaded to the location: {}".format(required_file_name, file_path)})
            resp = Response(message, status=200, mimetype='application/json')
            return resp
            
    def trigger_script(metadata):
        script = '{} {}/{} {}'.format("python",Path(__file__).resolve().parent.parent,"getMetadataAsXML.py",metadata)
        command = script.split(" ")
        status = subprocess.Popen(command)
        exit_status = status.poll()
        if 1 == exit_status:
            LOGGER.error("Error in executing {}".format(script))
            message = json.dumps(
                {"response": "Script execution failed"})
            status_code = 400
        else:
            LOGGER.info("Successfully executed {}".format(script))
            message = json.dumps(
                {"response": "Successfully Triggered script to export {0} metadata".format(metadata)})
            status_code = 200
        return Response(message, status=status_code, mimetype='application/json')