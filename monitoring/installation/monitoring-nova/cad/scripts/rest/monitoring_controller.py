from flask import Flask, send_file, Response, request
from flasgger import Swagger , swag_from
from flask_restful import Resource, Api, reqparse
from flasgger import LazyString, LazyJSONEncoder
from cad_utils import CADConfigUtil
from config_map_util import ConfigMapUtil
import werkzeug
import sys
sys.path.insert(0,'/opt/nsn/ngdb/monitoring/utils')
from logger_util import *

create_logger()
LOGGER = Logger.getLogger(__name__)
default_log_level = get_default_log_level()
LOGGER.setLevel(get_logging_instance(default_log_level))

app = Flask(__name__)
app.config["SWAGGER"] = {"title": "Swagger-UI", "uiversion": 3}
api = Api(app)

swagger_config = {
    "swagger":"2.0",
    "info":{
        "description":"Provides an interface to manage CAD configurations.", 
        "version":"1.0.0", 
        "title":"Technical/Operational Metadata Configurations"},
    "specs": [
        {
            "endpoint": '/v2/api-docs',
            "route": '/v2/api-docs',
            "rule_filter": lambda rule: True,  # all in
            "model_filter": lambda tag: True,  # all in
        }
    ],
     "basePath": "/monitoring",
    "tags":[{"name":"Configuration Management","description":"Read and Update CAD Configurations"}],
    "headers": [],
    "static_url_path": "/flasgger_static",
    "swagger_ui": True,
    "specs_route": "/swagger/",
}

template = dict(
    swaggerUiPrefix=LazyString(lambda: request.environ.get("HTTP_X_SCRIPT_NAME", ""))
)

app.json_encoder = LazyJSONEncoder
swagger = Swagger(app, config=swagger_config, template=template)

def get_file(Resource):
     parse = reqparse.RequestParser()
     parse.add_argument('file', type=werkzeug.datastructures.FileStorage, location='files')
     args = parse.parse_args()
     return args['file']
     
class TechnicalMetadataOutputXMLs(Resource):
    @swag_from("technical_xml_get_documentation.yml")
    def get(self):
        date_format = request.args.get('date') if request.args.get('date') is not None else 'latest'
        return CADConfigUtil.get_output_xmls(date_format,'technical')

class TechnicalMetadataConfig(Resource):
    @swag_from('technical_config_get_documentation.yml')
    def get(self):
        return ConfigMapUtil.retrieve_config_param('technicalMetadata')
        
    @swag_from('technical_config_put_documentation.yml')
    def patch(self):
        return ConfigMapUtil.patch_configMap_data('technicalMetadata',request.data.decode())
        
class TechnicalMetadataXSL(Resource):
    @swag_from('technical_xsl_put_documentation.yml')
    def put(self):
        return CADConfigUtil.put_custom_xsl('technicalMetadata',get_file(Resource))

class TechnicalMetadataScript(Resource):
    @swag_from('technical_trigger_documentation.yml')
    def post(self):
        return CADConfigUtil.trigger_script('technical')
        
class OperationalMetadataOutputXMLs(Resource):
    @swag_from("operational_xml_get_documentation.yml")
    def get(self):
        date_format = request.args.get('date') if request.args.get('date') is not None else 'latest'
        return CADConfigUtil.get_output_xmls(date_format,'operational')

class OperationalMetadataConfig(Resource):
    @swag_from('operational_config_get_documentation.yml')
    def get(self):
        return ConfigMapUtil.retrieve_config_param('operationalMetadata')
        
    @swag_from('operational_config_put_documentation.yml')
    def patch(self):
        return ConfigMapUtil.patch_configMap_data('operationalMetadata',request.data.decode())
        
class OperationalMetadataXSL(Resource):
    @swag_from('operational_xsl_put_documentation.yml')
    def put(self):
        return CADConfigUtil.put_custom_xsl('operationalMetadata',get_file(Resource))
        
class OperationalMetadataScript(Resource):
    @swag_from('operational_trigger_documentation.yml')
    def post(self):
        return CADConfigUtil.trigger_script('operational')
        
class Test(Resource):
    def get(self):
        return {'k1':'V1'}
        
api.add_resource(Test, "/api/v1/test")

api.add_resource(TechnicalMetadataOutputXMLs, "/api/v1/metadata/technical/xml")
api.add_resource(OperationalMetadataOutputXMLs, "/api/v1/metadata/operational/xml")

api.add_resource(TechnicalMetadataConfig, "/api/v1/metadata/configurations/technical")
api.add_resource(OperationalMetadataConfig, "/api/v1/metadata/configurations/operational")

api.add_resource(TechnicalMetadataXSL, "/api/v1/metadata/configurations/technical/xsl")
api.add_resource(OperationalMetadataXSL, "/api/v1/metadata/configurations/operational/xsl")

api.add_resource(TechnicalMetadataScript, "/api/v1/metadata/technical/trigger")
api.add_resource(OperationalMetadataScript, "/api/v1/metadata/operational/trigger")

if __name__ == '__main__':
    LOGGER.info("Starting monitoring controller...")
    app.run(debug=True, host='0.0.0.0', port=8080)
