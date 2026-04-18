from __future__ import annotations

from unittest.mock import MagicMock, patch

from common.auth import AuthManager


class TestAuthManager:
    def test_source_client_lazy_creation(self, mock_config, mock_dbutils):
        with patch("common.auth.WorkspaceClient") as mock_ws:
            mock_ws.return_value = MagicMock()
            auth = AuthManager(mock_config, mock_dbutils)

            _ = auth.source_client
            _ = auth.source_client

            mock_ws.assert_called_once()

    def test_target_client_lazy_creation(self, mock_config, mock_dbutils):
        with patch("common.auth.WorkspaceClient") as mock_ws:
            mock_ws.return_value = MagicMock()
            auth = AuthManager(mock_config, mock_dbutils)

            _ = auth.target_client
            _ = auth.target_client

            mock_ws.assert_called_once()

    def test_connectivity_success(self, mock_config, mock_dbutils, mock_workspace_client):
        auth = AuthManager(mock_config, mock_dbutils)
        auth._source_client = mock_workspace_client
        auth._target_client = mock_workspace_client

        result = auth.test_connectivity()

        assert result == {"source": True, "target": True}

    def test_connectivity_failure(self, mock_config, mock_dbutils, mock_workspace_client):
        failing_client = MagicMock()
        failing_client.current_user.me.side_effect = Exception("connection refused")

        auth = AuthManager(mock_config, mock_dbutils)
        auth._source_client = failing_client
        auth._target_client = mock_workspace_client

        result = auth.test_connectivity()

        assert result == {"source": False, "target": True}
