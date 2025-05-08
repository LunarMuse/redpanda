# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from typing import Any
import logging
import json
import time
import requests

from ducktape.mark import matrix
from rptest.tests.redpanda_cloud_test import RedpandaCloudTest
from rptest.services.provider_clients.rpcloud_client import RpCloudApiClient

from rptest.clients.installpack import InstallPackClient
from rptest.clients.rpk import RpkTool, TopicSpec, RpkException

from rptest.services.redpanda import get_cloud_provider
from rptest.services.cluster import cluster
from rptest.services.redpanda import RedpandaServiceCloud

from rptest.services.databricks_workspace import DatabricksWorkspace
from rptest.context.databricks import DatabricksContext as DatabricksContext
from rptest.services.catalog_service import CatalogType
#from rptest.tests.datalake.datalake_services import DatalakeServices
#from rptest.tests.datalake.query_engine_base import QueryEngineType
#from rptest.tests.datalake.utils import supported_storage_types
#from rptest.tests.redpanda_test import RedpandaTest
#from rptest.utils.mode_checks import cleanup_on_early_exit


def supported_catalog_types():
    return ["aws_glue", "databricks_unity", "snowflake"]


def supported_network_types():
    return ["public", "private"]


class IcebergCloudCatalogsTest(RedpandaCloudTest):
    """
    Verify that cluster infra/config matches config profile used to launch - only applies to cloudv2
    """
    def __init__(self, test_context):
        super().__init__(test_context=test_context)
        self._ctx = test_context
        self._ipClient = InstallPackClient(
            self.redpanda._cloud_cluster.config.install_pack_url_template,
            self.redpanda._cloud_cluster.config.install_pack_auth_type,
            self.redpanda._cloud_cluster.config.install_pack_auth)

    def setUp(self):
        super().setUp()
        cloud_cluster = self.redpanda._cloud_cluster
        self.logger.debug(f"Cloud Cluster Info: {vars(cloud_cluster)}")
        install_pack_version = cloud_cluster.get_install_pack_version()
        self._ip = self._ipClient.getInstallPack(install_pack_version)
        self._clusterId = cloud_cluster.cluster_id
        self._configProfile = self._ip['config_profiles'][
            cloud_cluster.config.config_profile_name]

    def test_healthy(self):
        r = self.redpanda.cluster_unhealthy_reason()
        assert r is None, r
        assert self.redpanda.cluster_healthy()
        self.redpanda.assert_cluster_is_reusable()

    @cluster(num_nodes=1)
    def test_databricks_basic(self):
        # Get globals.json map
        globals = self._ctx.globals

        cloud_cluster = self.redpanda._cloud_cluster
        client: RpCloudApiClient = cloud_cluster.public_api

        databricks_client = DatabricksWorkspace(context=self._ctx)
        bucket = f"redpanda-cloud-storage-{self._clusterId}"
        catalog_info = databricks_client.create_catalog(bucket=bucket)

        enable_resp = cloud_cluster.update_cluster_property_public(
            self._clusterId, "iceberg_enabled", True)
        self.logger.debug(f"Enable iceberg response: {enable_resp}")

        # Parameters for creating redpanda secret
        secret_id = "UNITY_CLIENT_SECRET5"
        secret_data = globals["databricks_client_secret"]
        create_resp = cloud_cluster.create_secret(secret_id, secret_data)
        self.logger.debug(f"Create secret response: {create_resp}")

        iceberg_rest_catalog_endpoint = globals[
            "databricks_workspace_url"] + "/api/2.1/unity-catalog/iceberg-rest"
        databricks_client_id = globals["databricks_client_id"]
        iceberg_rest_catalog_warehouse = globals[
            "databricks_sql_warehouse_path"]

        # Construct the payload for the request
        payload = {
            "cluster_configuration": {
                "custom_properties": {
                    "iceberg_rest_catalog_endpoint":
                    iceberg_rest_catalog_endpoint,
                    "iceberg_rest_catalog_authentication_mode": "oauth2",
                    "iceberg_rest_catalog_client_id": databricks_client_id,
                    "iceberg_rest_catalog_client_secret":
                    "${secrets.UNITY_CLIENT_SECRET5}",
                    "iceberg_rest_catalog_warehouse":
                    iceberg_rest_catalog_warehouse,
                    "iceberg_catalog_type": "rest"
                }
            }
        }

        # Log the constructed payload for debugging
        self.logger.debug(f"Payload to be sent: {payload}")

        # Send the HTTP PATCH request
        response = client._http_patch(
            base_url=cloud_cluster.config.public_api_url,
            endpoint=f"/v1/clusters/{cloud_cluster.current.cluster_id}",
            json=payload)

        # Log the response for debugging
        self.logger.debug(
            f"Response for passing catalog params via public API: {response}")

        # TODO Marat insert topics and verification (separate PR)

        # databricks cleanup
        databricks_client.stop()
