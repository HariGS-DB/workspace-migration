# Databricks notebook source

# COMMAND ----------

# Bootstrap: put the bundle's `src/` dir on sys.path so `from common...` imports resolve
import sys  # noqa: E402
_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F821
_nb = _ctx.notebookPath().get()
_src = "/Workspace" + _nb.split("/files/")[0] + "/files/src"
if _src not in sys.path:
    sys.path.insert(0, _src)

# COMMAND ----------
# Setup Delta Sharing: create share, recipient, add tables, grant access,
# and ensure target catalogs/schemas exist.

import logging

from databricks.sdk.service.sharing import (
    AuthenticationType,
    PermissionsChange,
    Privilege,
    SharedDataObject,
    SharedDataObjectUpdate,
    SharedDataObjectUpdateAction,
)
try:
    from databricks.sdk.service.sharing import SharedDataObjectDataObjectType as _DataObjectType  # type: ignore
    _TABLE_TYPE: object = _DataObjectType.TABLE
except ImportError:
    _TABLE_TYPE = "TABLE"

from common.auth import AuthManager
from common.config import MigrationConfig
from common.tracking import TrackingManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("setup_sharing")

SHARE_NAME = "cp_migration_share"


# COMMAND ----------


def _is_notebook() -> bool:
    """Return True when running inside a Databricks notebook."""
    try:
        _ = dbutils  # type: ignore[name-defined]  # noqa: F821
        return True
    except NameError:
        return False


# COMMAND ----------
# 1. Create or get the delta share on source


def get_or_create_share(auth_mgr: AuthManager, share_name: str, *, dry_run: bool = False) -> str:
    """Create or retrieve a delta share on the source workspace. Returns share name."""
    source = auth_mgr.source_client
    try:
        share = source.shares.get(share_name)
        logger.info("Share '%s' already exists.", share.name)
        return share.name  # type: ignore[return-value]
    except Exception:  # noqa: BLE001
        logger.info("Share '%s' not found, creating...", share_name)

    if dry_run:
        logger.info("[DRY RUN] Would create share '%s'.", share_name)
        return share_name

    share = source.shares.create(name=share_name)
    logger.info("Created share '%s'.", share.name)
    return share.name  # type: ignore[return-value]


# COMMAND ----------
# 3. Create or get recipient for target


def get_or_create_recipient(auth_mgr: AuthManager, metastore_id: str, *, dry_run: bool = False) -> str:
    """Create or retrieve a sharing recipient for the target metastore."""
    source = auth_mgr.source_client
    recipient_name = f"cp_migration_recipient_{metastore_id}"
    try:
        recipient = source.recipients.get(recipient_name)
        logger.info("Recipient '%s' already exists.", recipient.name)
        return recipient.name  # type: ignore[return-value]
    except Exception:  # noqa: BLE001
        logger.info("Recipient '%s' not found, creating...", recipient_name)

    if dry_run:
        logger.info("[DRY RUN] Would create recipient '%s'.", recipient_name)
        return recipient_name

    recipient = source.recipients.create(
        name=recipient_name,
        authentication_type=AuthenticationType.DATABRICKS,
        data_recipient_global_metastore_id=metastore_id,
    )
    logger.info("Created recipient '%s'.", recipient.name)
    return recipient.name  # type: ignore[return-value]


# COMMAND ----------
# 5. Add tables to share in batches of 100


def add_tables_to_share(
    auth_mgr: AuthManager,
    share_name: str,
    tables: list[dict],
    *,
    dry_run: bool = False,
) -> None:
    """Add tables to a delta share in batches of 100 (removes stale entries first)."""
    source = auth_mgr.source_client
    batch_size = 100

    # Remove any existing objects first so re-runs start from a clean slate and
    # don't conflict with entries that used the old shared_as format.
    try:
        existing_share = source.shares.get(name=share_name, include_shared_data=True)
        removals = [
            SharedDataObjectUpdate(
                action=SharedDataObjectUpdateAction.REMOVE,
                data_object=SharedDataObject(name=o.name, data_object_type=o.data_object_type),
            )
            for o in (existing_share.objects or [])
        ]
        if removals and not dry_run:
            source.shares.update(name=share_name, updates=removals)
            logger.info("Removed %d stale object(s) from share '%s'.", len(removals), share_name)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not pre-clean share: %s", exc)
    existing_names: set[str] = set()

    for i in range(0, len(tables), batch_size):
        batch = tables[i : i + batch_size]
        updates = []
        for tbl in batch:
            obj_name = tbl["object_name"]
            # object_name is expected to be FQN like `catalog`.`schema`.`table`
            parts = obj_name.strip("`").split("`.`")
            if len(parts) != 3:
                logger.warning("Skipping malformed FQN: %s", obj_name)
                continue
            clean_name = ".".join(parts)  # catalog.schema.table without backticks
            if clean_name in existing_names:
                continue
            # NOTE: don't set shared_as — let Databricks expose the original catalog.schema.table
            # structure in the consumer catalog. With shared_as as "schema.table", the UC
            # share-consumer catalog returned an internal schema UUID error on DEEP CLONE.
            updates.append(
                SharedDataObjectUpdate(
                    action=SharedDataObjectUpdateAction.ADD,
                    data_object=SharedDataObject(
                        name=clean_name,
                        data_object_type=_TABLE_TYPE,
                    ),
                )
            )

        if not updates:
            continue

        if dry_run:
            logger.info(
                "[DRY RUN] Would add %d tables to share '%s' (batch %d).",
                len(updates),
                share_name,
                i // batch_size + 1,
            )
            continue

        source.shares.update(name=share_name, updates=updates)
        logger.info(
            "Added %d tables to share '%s' (batch %d).",
            len(updates),
            share_name,
            i // batch_size + 1,
        )


# COMMAND ----------
# 7. Create target catalogs/schemas if missing


def ensure_target_catalogs_and_schemas(
    auth_mgr: AuthManager,
    tables: list[dict],
    *,
    dry_run: bool = False,
) -> None:
    """Ensure all required catalogs and schemas exist on the target workspace."""
    target = auth_mgr.target_client
    seen_catalogs: set[str] = set()
    seen_schemas: set[str] = set()

    for tbl in tables:
        catalog_name = tbl.get("catalog_name", "")
        schema_name = tbl.get("schema_name", "")
        if not catalog_name or not schema_name:
            continue

        if catalog_name not in seen_catalogs:
            seen_catalogs.add(catalog_name)
            if dry_run:
                logger.info("[DRY RUN] Would create catalog '%s' on target.", catalog_name)
            else:
                try:
                    target.catalogs.get(catalog_name)
                    logger.info("Target catalog '%s' already exists.", catalog_name)
                except Exception:  # noqa: BLE001
                    target.catalogs.create(name=catalog_name)
                    logger.info("Created target catalog '%s'.", catalog_name)

        schema_fqn = f"{catalog_name}.{schema_name}"
        if schema_fqn not in seen_schemas:
            seen_schemas.add(schema_fqn)
            if dry_run:
                logger.info("[DRY RUN] Would create schema '%s' on target.", schema_fqn)
            else:
                try:
                    target.schemas.get(f"{catalog_name}.{schema_name}")
                    logger.info("Target schema '%s' already exists.", schema_fqn)
                except Exception:  # noqa: BLE001
                    target.schemas.create(
                        name=schema_name,
                        catalog_name=catalog_name,
                    )
                    logger.info("Created target schema '%s'.", schema_fqn)


# COMMAND ----------
# Notebook execution


def run(dbutils, spark) -> None:  # noqa: ARG001
    """Entry point when running as a Databricks notebook."""
    config = MigrationConfig.from_job_params(dbutils)
    auth = AuthManager(config, dbutils)
    spark_session = spark
    tracker = TrackingManager(spark_session, config)

    # 1. Create or get the delta share on source
    share = get_or_create_share(auth, SHARE_NAME, dry_run=config.dry_run)

    # 2. Get target metastore global_metastore_id (format: <cloud>:<region>:<uuid>)
    target_metastore = auth.target_client.metastores.summary()
    target_metastore_id = target_metastore.global_metastore_id
    logger.info("Target global metastore ID: %s", target_metastore_id)

    # 3. Create or get recipient for target
    recipient_name = get_or_create_recipient(auth, target_metastore_id, dry_run=config.dry_run)

    # 4. Read pending managed tables from tracker
    pending_tables = tracker.get_pending_objects("managed_table")
    logger.info("Found %d pending managed tables to share.", len(pending_tables))

    # 5. Add tables to share
    add_tables_to_share(auth, SHARE_NAME, pending_tables, dry_run=config.dry_run)

    # 6. Grant SELECT on share to recipient
    if config.dry_run:
        logger.info("[DRY RUN] Would grant SELECT on '%s' to '%s'.", share, recipient_name)
    else:
        auth.source_client.shares.update_permissions(
            name=SHARE_NAME,
            changes=[
                PermissionsChange(
                    principal=recipient_name,
                    add=[Privilege.SELECT.value],
                )
            ],
        )
        logger.info("Granted SELECT on '%s' to '%s'.", share, recipient_name)

    # 7. Ensure target catalogs and schemas exist
    ensure_target_catalogs_and_schemas(auth, pending_tables, dry_run=config.dry_run)

    # 8. Create share-consumer catalog on target (reads from the share)
    ensure_share_consumer_catalog(auth, SHARE_NAME, config.dry_run)

    logger.info("Delta sharing setup complete.")


def ensure_share_consumer_catalog(auth_mgr: AuthManager, share_name: str, dry_run: bool) -> None:
    """On target workspace, create a catalog that reads from the source share.

    The catalog name is ``<share_name>_consumer``. It's required for workers to
    DEEP CLONE shared tables on the target side.
    """
    consumer_catalog = f"{share_name}_consumer"
    target = auth_mgr.target_client
    source_metastore = auth_mgr.source_client.metastores.summary()
    source_metastore_id = source_metastore.global_metastore_id

    # Find the provider on target that matches the source metastore
    providers = list(target.providers.list())
    matching = [
        p for p in providers
        if getattr(p, "data_provider_global_metastore_id", None) == source_metastore_id
    ]
    if not matching:
        names = [p.name for p in providers]
        raise RuntimeError(
            f"No target-side provider found for source metastore {source_metastore_id}. "
            f"Available providers: {names}"
        )
    provider_name = matching[0].name
    logger.info("Matched target provider '%s' for source metastore.", provider_name)

    if dry_run:
        logger.info("[DRY RUN] Would CREATE CATALOG %s USING SHARE %s.%s", consumer_catalog, provider_name, share_name)
        return

    # Recreate on every run to pick up any shared_as changes.
    try:
        target.catalogs.delete(consumer_catalog, force=True)
        logger.info("Dropped existing share consumer catalog '%s'.", consumer_catalog)
    except Exception:  # noqa: BLE001
        pass

    target.catalogs.create(
        name=consumer_catalog,
        provider_name=provider_name,
        share_name=share_name,
    )
    logger.info("Created share consumer catalog '%s' from %s.%s", consumer_catalog, provider_name, share_name)


# COMMAND ----------

if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
