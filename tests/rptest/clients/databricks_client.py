# rptest/services/provider_clients/databricks_client.py

from uuid import uuid4
from typing import Optional, Any

import databricks.sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors.platform import BadRequest


class DatabricksClient:
    """
    Lightweight wrapper around the Databricks SDK, databricks.sql and PyIceberg
    to work with hosted Databricks setup

    Supports both PAT-based auth (token) and OAuth2 client-credential flow
    (client_id/client_secret).
    """
    def __init__(
        self,
        host: Optional[str] = None,
        token: Optional[str] = None,
        sql_http_path: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        tenant_id: Optional[str] = None,
        auth_type: Optional[str] = None,
    ):
        # Store for SQL connection
        self.host = host
        self.token = token
        self.sql_http_path = sql_http_path
        # Build config for SDK client connection
        cfg = {}
        if host:
            cfg["host"] = host
        if token:
            cfg["token"] = token
        if client_id and client_secret:
            cfg["client_id"] = client_id
            cfg["client_secret"] = client_secret
        if tenant_id:
            cfg["tenant_id"] = tenant_id
        if auth_type:
            cfg["auth_type"] = auth_type

        # will also pick up DATABRICKS_HOST, DATABRICKS_TOKEN, etc. from env
        self.ws = WorkspaceClient(**cfg)

    def create_storage_credential(
        self,
        iam_role_arn: str,
        name: Optional[str] = None,
        comment: Optional[str] = None,
    ) -> Any:
        """
        Create a Unity Catalog storage credential (AWS IAM role).
        """
        cred_name = name or f"iceberg_cred_{uuid4().hex[:8]}"
        kwargs = {
            "name": cred_name,
            "aws_iam_role__role_arn": iam_role_arn,
        }
        if comment:
            kwargs["comment"] = comment
        return self.ws.unity_catalog.storage_credentials.create(**kwargs)

    def create_external_location(
        self,
        name: str,
        credential_name: str,
        url: str,
        comment: Optional[str] = None,
    ) -> Any:
        """
        Create an external location in Unity Catalog, or return existing one.
        """
        kwargs = {"name": name, "credential_name": credential_name, "url": url}
        if comment:
            kwargs["comment"] = comment
        try:
            return self.ws.external_locations.create(**kwargs)
        except BadRequest as e:
            if "already exists" in str(e):
                return self.ws.external_locations.get(name=name)
            raise

    def create_catalog(self,
                       name: str,
                       storage_root: str,
                       comment: Optional[str] = None) -> Any:
        """
        Create a Unity Catalog catalog, wired to the given storage_root (external location).
        Returns existing catalog if one already exists.
        """
        kwargs = {"name": name, "storage_root": storage_root}
        if comment:
            kwargs["comment"] = comment
        try:
            return self.ws.catalogs.create(**kwargs)
        except BadRequest as e:
            # if it's already there, fetch and return it
            if "already exists" in str(e):
                return self.ws.catalogs.get(name=name)
            raise

    def grant_all_privileges_on_catalog(
        self,
        catalog_name: str,
        principal: str,
    ) -> None:
        """
        Grant ALL PRIVILEGES on a catalog to a principal via SQL.
        """
        # connect via SQL connector
        conn = databricks.sql.connect(server_hostname=self.host,
                                      access_token=self.token,
                                      http_path=self.sql_http_path)
        try:
            with conn.cursor() as cursor:
                sql = f"GRANT ALL PRIVILEGES ON CATALOG `{catalog_name}` TO `{principal}`"
                cursor.execute(sql)
        finally:
            conn.close()

    def create_schema_and_grant(self,
                                catalog_name: str,
                                schema_name: str,
                                principal: Optional[str] = None) -> None:
        """
        Create a schema in the Unity Catalog and grant EXTERNAL USE SCHEMA to principal.
        If principal is None, uses the current user.
        """
        conn = databricks.sql.connect(server_hostname=self.host,
                                      access_token=self.token,
                                      http_path=self.sql_http_path)
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"CREATE SCHEMA IF NOT EXISTS `{catalog_name}`.`{schema_name}`"
                )

                # determine principal
                if principal is None:
                    row = cursor.execute("SELECT current_user()").fetchone()
                    principal = row[0]

                # grant external use schema
                grant_sql = (
                    f"GRANT EXTERNAL USE SCHEMA ON SCHEMA `{catalog_name}`.`{schema_name}` TO `{principal}`"
                )
                cursor.execute(grant_sql)
        finally:
            conn.close()

    # TODO Marat: Add other management functions that we created during integration phase (list, cleanups, etc.)
