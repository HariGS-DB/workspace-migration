from __future__ import annotations

from databricks.sdk import WorkspaceClient

from common.config import MigrationConfig


class AuthManager:
    """Manages Databricks workspace authentication via SPN credentials."""

    def __init__(self, config: MigrationConfig, dbutils: object | None = None) -> None:
        self.config = config
        self.dbutils = dbutils
        self._source_client: WorkspaceClient | None = None
        self._target_client: WorkspaceClient | None = None

    @property
    def source_client(self) -> WorkspaceClient:
        if self._source_client is None:
            self._source_client = self._build_client(self.config.source_workspace_url)
        return self._source_client

    @property
    def target_client(self) -> WorkspaceClient:
        if self._target_client is None:
            self._target_client = self._build_client(self.config.target_workspace_url)
        return self._target_client

    def _build_client(self, host: str) -> WorkspaceClient:
        secret = self.dbutils.secrets.get(  # type: ignore[union-attr]
            scope=self.config.spn_secret_scope,
            key=self.config.spn_secret_key,
        )
        return WorkspaceClient(
            host=host,
            client_id=self.config.spn_client_id,
            client_secret=secret,
        )

    def test_connectivity(self) -> dict[str, bool]:
        results: dict[str, bool] = {}
        for name, client_prop in [("source", "source_client"), ("target", "target_client")]:
            try:
                getattr(self, client_prop).current_user.me()
                results[name] = True
            except Exception:  # noqa: BLE001
                results[name] = False
        return results
