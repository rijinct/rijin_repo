import glob
import json
from os import path
from flask import Flask,  request, send_file, Response
from flask_restful import Resource, Api, reqparse
import werkzeug
from flasgger import Swagger
from flasgger.utils import swag_from
from flasgger import LazyString, LazyJSONEncoder
from zipfile import ZipFile
import zipfile
import os
import controller_constants
import sys
sys.path.insert(0,'/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/com/rijin/dqhi')
import common_utils
LOGGER = common_utils.get_logger_nova()
import subprocess

app = Flask(__name__)
app.config["SWAGGER"] = {"title": "Swagger-UI", "uiversion": 3}
api = Api(app)

swagger_config = {
    "swagger":"2.0",
    "info":{
        "description":"Provides an interface to manage DQHI configurations.", 
        "version":"1.0.0", 
        "title":"Data Quality Health Index"},
    "specs": [
        {
            "endpoint": '/v2/api-docs',
            "route": '/v2/api-docs',
            "rule_filter": lambda rule: True,  # all in
            "model_filter": lambda tag: True,  # all in
        }
    ],
     #"host":"data-quality-health-index.dis-nci.svc.cluster.local:8080",
     "basePath": "/dqhi",
    "tags":[{"name":"Configuration Management","description":"Read and Update hints for Hive/Spark"}],
    "headers": [],
    "static_url_path": "/flasgger_static",
    # "static_folder": "static",  # must be set by user
    "swagger_ui": True,
    "specs_route": "/swagger/",
}

template = dict(
    swaggerUiPrefix=LazyString(lambda: request.environ.get("HTTP_X_SCRIPT_NAME", ""))
)

app.json_encoder = LazyJSONEncoder
swagger = Swagger(app, config=swagger_config, template=template)


class DQHIExcel(Resource):
    @swag_from("excel_get_documentation.yml")
    def get(self):
        location = request.args.get('location')
        fileType = request.args.get('fileName')
        file_path = location == "Custom" and controller_constants.DQHI_CUSTOM_PATH or controller_constants.DQHI_PRODUCT_PATH
        file_name = fileType == "KPI_CDE_MAPPING" and controller_constants.KPI_CDE_FILE_NAME or controller_constants.DQ_RULES_AND_FIELD_DEFINITIONS_FILE
        absolute_file_name = file_path + file_name
        is_file_present = path.exists(absolute_file_name)
        if is_file_present:
            return send_file(file_path + file_name, as_attachment=True,
                             mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        else:
            message = json.dumps(
                {"response": "Requested file {} not present in the location: {}".format(file_name, file_path)})
            resp = Response(message, status=404, mimetype='application/json')
            return resp

    @swag_from("excel_put_documentation.yml")
    def put(self):
        location = request.args.get('location')
        file_type = request.args.get('fileName')
        file_path = location == "Custom" and controller_constants.DQHI_CUSTOM_PATH or controller_constants.DQHI_PRODUCT_PATH
        parse = reqparse.RequestParser()
        parse.add_argument('file', type=werkzeug.datastructures.FileStorage, location='files')
        args = parse.parse_args()
        image_file = args['file']

        file_name_dict = {'KPI_CDE_MAPPING': 'KPI_CDE_MAPPING.xlsx', 'DQ_RULES_AND_FIELD_DEFINITIONS': 'DQ_RULES_AND_FIELD_DEFINITIONS.xlsx'}
        required_file_name = file_name_dict[file_type]

        if image_file is None:
            message = json.dumps(
                {"response": "Required file {} not uploaded".format(required_file_name)})
            resp = Response(message, status=400, mimetype='application/json')
            return resp

        is_valid = image_file.filename.split(".")[1] == "xlsx" and True or False
        if is_valid:
            image_file.save(file_path + required_file_name)
            message = json.dumps(
                {"response": "{} successfully uploaded to the location: {}".format(required_file_name, file_path)})
            resp = Response(message, status=200, mimetype='application/json')
            return resp
            
        else:
            message = json.dumps(
                {"response": "Requested file {} not in expected format".format(image_file.filename)})
            resp = Response(message, status=400, mimetype='application/json')
            return resp


class FinalXMLS(Resource):

    @swag_from("xml_get_documentation.yml")
    def get(self):
        date_format = request.args.get('date')
        zip_obj = ZipFile(controller_constants.DQHI_XML_PATH+'output.zip', 'w', compression=zipfile.ZIP_DEFLATED)
        export_files = ['dataQualityResults','dataQualityRule','dataQualityStatistics']
        try:
            for export_file in export_files:
                if date_format == "latest":
                    file_pattern = export_file+'*'
                else:
                    file_pattern = export_file + '_'+date_format+'_*'
                required_file = glob.glob(controller_constants.DQHI_XML_PATH + file_pattern)
                final_export_file = max(required_file, key=os.path.getctime)
                zip_obj.write(final_export_file)
            zip_obj.close()
            return send_file(controller_constants.DQHI_XML_PATH + 'output.zip',
                             mimetype='zip',
                             attachment_filename='output.zip',
                             as_attachment=True)
        except ValueError as e:
            message = json.dumps(
                {"response": "Requested output xmls not found for given date:{}".format(date_format)})
            resp = Response(message, status=400, mimetype='application/json')
            return resp



class Trigger(Resource):
    
    @swag_from("trigger_dqhi_documentation.yml")
    def patch(self):
        date_format = request.args.get('date')
        if date_format == "None":
            score_calculator = '{} {} {}'.format("python3", "-m",
                                                    "com.rijin.dqhi.score_calculator_wrapper")
        else:
            score_calculator = '{} {} {} {}'.format("python3", "-m", "com.rijin.dqhi.score_calculator_wrapper", date_format)
        command = score_calculator.split(" ")
        status = subprocess.Popen(command)
        exit_status = status.poll()
        if 1 == exit_status:
            LOGGER.error("Error in executing {}".format(score_calculator))
            message = json.dumps(
                {"response": "Score calculation failed"})
            status_code = 400
        else:
            LOGGER.info("Successfully executed {}".format(score_calculator))
            message = json.dumps(
                {"response": "Successfully triggered score calculation"})
            status_code = 200
        resp = Response(message, status=status_code, mimetype='application/json')
        return resp

class Test(Resource):
    def get(self):
        return {'k1':'V1'}

api.add_resource(DQHIExcel, '/api/dqhi/v1/configurations')
api.add_resource(FinalXMLS, "/api/dqhi/v1/data_quality_metrics")
api.add_resource(Trigger, "/api/dqhi/v1/schedule")
api.add_resource(Test, "/api/test")

if __name__ == '__main__':

    app.run(debug=True, host='0.0.0.0', port=8080)
