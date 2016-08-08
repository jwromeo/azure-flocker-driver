from flocker.node import BackendDescription, DeployerType
from .azure_storage_driver import (
    azure_driver_from_configuration
)


def api_factory(**kwargs):

    return azure_driver_from_configuration(
        client_id=kwargs['client_id'],
        client_secret=kwargs['client_secret'],
        tenant_id=kwargs['tenant_id'],
        subscription_id=kwargs['subscription_id'],
        storage_account_name=kwargs['storage_account_name'],
        storage_account_key=kwargs['storage_account_key'],
        storage_account_container=kwargs['storage_account_container'],
        group_name=kwargs['group_name'],
        location=kwargs['location'],
        debug=kwargs['debug'])

FLOCKER_BACKEND = BackendDescription(
    name=u"azure_flocker_driver",
    needs_reactor=True, needs_cluster_id=False,
    api_factory=api_factory, deployer_type=DeployerType.block)
