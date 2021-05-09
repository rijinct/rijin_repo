from healthmonitoring.framework.util.loader import SpecificationLoader
from healthmonitoring.framework.db.connection_factory import \
    ConnectionFactory as ClientConnectionFactory


class ConnectionFactory:

    @staticmethod
    def get_connection(db_type):
        SpecificationLoader.load_if_required()
        ClientConnectionFactory.instantiate_connection(db_type)
        return ClientConnectionFactory.get_connection()
