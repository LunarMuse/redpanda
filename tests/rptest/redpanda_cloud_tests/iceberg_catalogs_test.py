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
from rptest.clients.databricks_client import DatabricksClient

from rptest.clients.installpack import InstallPackClient
from rptest.clients.rpk import RpkTool, TopicSpec, RpkException

from rptest.services.redpanda import get_cloud_provider
from rptest.services.cluster import cluster
from rptest.services.redpanda import RedpandaServiceCloud


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

    @cluster(num_nodes=3)
    def test_iceberg_unity_catalog(self):
        # Get globals.json map
        globals = self._ctx.globals

        cloud_cluster = self.redpanda._cloud_cluster
        client: RpCloudApiClient = cloud_cluster.public_api

        # Log catalog params, except secret/token
        for k in (
                "iceberg_rest_catalog_endpoint",
                "iceberg_rest_catalog_authentication_mode",
                "iceberg_rest_catalog_client_id",
                "iceberg_rest_catalog_warehouse",
        ):
            self.logger.debug(f"{k}: {globals.get(k)}")

        cloud_cluster = self.redpanda._cloud_cluster
        client: RpCloudApiClient = cloud_cluster.public_api

        # Enable Iceberg feature via public API
        enable_resp = client._http_patch(
            base_url=cloud_cluster.config.public_api_url,
            endpoint=f"/v1/clusters/{cloud_cluster.current.cluster_id}",
            json={
                "cluster_configuration": {
                    "custom_properties": {
                        "iceberg_enabled": True
                    }
                }
            })
        self.logger.debug(f"Enable iceberg response: {enable_resp}")

        secret = globals["iceberg_rest_catalog_client_secret"]

        # Create new secret on dataplane and then pass it to Redpanda iceberg config
        try:
            response = client._http_post(
                # base_url=cloud_cluster.config.dataplane_api_url,
                # TODO Marat: Fetch correct dataplane URL
                base_url=
                "https://api-fa3bbf12.d057avt4knf5ipm986eg.byoc.ign.cloud.redpanda.com",
                endpoint=f"/v1/secrets",
                json={
                    "id": "TEST",
                    "scopes": ["SCOPE_REDPANDA_CLUSTER"],
                    "secret_data": secret,
                })
            self.logger.debug(
                f"Response for creating new secret via public API: {response}")
        except requests.exceptions.HTTPError as e:
            # log full response body to see the validation error
            self.logger.error(
                f"Failed to create secret, status={e.response.status_code}, body={e.response.text}"
            )
            raise

        # Pass catalog params
        response = client._http_patch(
            base_url=cloud_cluster.config.public_api_url,
            endpoint=f"/v1/clusters/{cloud_cluster.current.cluster_id}",
            json={
                "cluster_configuration": {
                    "custom_properties": {
                        "iceberg_rest_catalog_endpoint":
                        globals["iceberg_rest_catalog_endpoint"],
                        "iceberg_rest_catalog_authentication_mode":
                        globals["iceberg_rest_catalog_authentication_mode"],
                        "iceberg_rest_catalog_client_id":
                        globals["iceberg_rest_catalog_client_id"],
                        "iceberg_rest_catalog_client_secret":
                        "${secrets.TEST}",
                        "iceberg_rest_catalog_warehouse":
                        globals["iceberg_rest_catalog_warehouse"],
                        "iceberg_catalog_type":
                        "rest",
                    }
                }
            })
        self.logger.debug(
            f"Response for passing catalog params via public API: {response}")

        ## Databricks specific section
        host = "dbc-0f5177e3-6aa4.cloud.databricks.com"
        token = globals["databricks_token"]
        sql_http_path = "/sql/1.0/warehouses/762a1e2735b5e17d"
        principal = "47ef0607-df2b-482f-9cbc-ba02d8c776a1"

        db = DatabricksClient(host=host,
                              token=token,
                              sql_http_path=sql_http_path)

        # Build object storage path
        storage_uri_prefix = "s3"
        bucket = f"redpanda-cloud-storage-{self._clusterId}"
        warehouse = globals["iceberg_rest_catalog_warehouse"]
        s3_path = f"{storage_uri_prefix}://{bucket}/{warehouse}".rstrip("/")

        # Not needed as have credentials for all accounts
        '''
        # Create the storage credential
        cred_name = f"testing-cloud-devprod-{bucket}"
        iam_role_arn="arn:aws:iam::471112860801:role/DatabricksUnityCatalogRole"
        try:
            cred = db.create_storage_credential(
                iam_role_arn=iam_role_arn,
                name=cred_name,
                comment="Created with Ducktape tests"
            )
            self.logger.info(f"Storage credential created: {cred.name!r}")
        except Exception as e:
            self.logger.exception("Failed to create storage credential")
        '''

        # The name of the pre-existing storage credential in Unity Catalog
        # TODO Marat: map which account is being used during tests and select credentials
        cred_name = "testing-cloud-devprod-471112860801"

        # Create an External Location pointing at storage path
        loc_name = f"testing-cloud-devprod-extloc-{bucket}"
        external_location = db.create_external_location(
            name=loc_name,
            credential_name=cred_name,
            url=s3_path,
            comment="Iceberg external location for Redpanda tests")
        self.logger.info(
            f"External location created: {external_location.name}")

        # Create Unity Catalog
        catalog_name = f"testing-cloud-devprod-{self._clusterId}"
        catalog = db.create_catalog(
            name=catalog_name,
            storage_root=external_location.url,
            comment="Iceberg Unity Catalog for Redpanda tests")
        self.logger.info(
            f"Unity Catalog created (or fetched existing): {catalog.name}")

        db.grant_all_privileges_on_catalog(catalog_name, principal)

        # create a schema and grant external use
        db.create_schema_and_grant(catalog_name, "redpanda", principal)

        # Create iceberg enabled topics
        self.rpk = RpkTool(self.redpanda)

        test_topic = 'test_topic'
        self.rpk.create_topic(test_topic)
        self.rpk.alter_topic_config(test_topic,
                                    TopicSpec.PROPERTY_ICEBERG_MODE,
                                    'key_value')

        MESSAGE_COUNT = 20
        for i in range(MESSAGE_COUNT):
            self.rpk.produce(test_topic, f"foo {i} ", f"bar {i}")

        self.logger.debug("Waiting 1 minute...")
        time.sleep(60)

        # TODO Marat: verify data and metadata files on object storage
        # TODO Marat: query data using rest APIs
        # TODO Marat: verify that tables on databricks catalog match redpanda topics data
        # TODO Marat: Clean up


#    @cluster(num_nodes=3)
#    @matrix(
#    catalog_type=supported_catalog_types(),
#    network_type=supported_network_types()
#    )
#    def test_iceberg_unity_catalog(self):
#TODO Marat: performance and OMB tests
