from flocker.node import BackendDescription, DeployerType
from .azure_storage_driver import (
    azure_driver_from_configuration
)


def api_factory(**kwargs):

    return azure_driver_from_configuration(
        service_name=kwargs[u"service_name"],
        subscription_id=kwargs[u"subscription_id"],
        storage_account_name=kwargs[u"storage_account_name"],
        certificate_data_path=kwargs[u"./azure-cert.pem"],
        debug=kwargs[u"debug"])

FLOCKER_BACKEND = BackendDescription(
    name=u"azure_flocker_driver",
    needs_reactor=True, needs_cluster_id=False,
    api_factory=api_factory, deployer_type=DeployerType.block)
