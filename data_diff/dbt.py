import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import nullcontext

import pydantic
import rich

from data_diff import Algorithm, connect_to_table, diff_tables
from data_diff.dbt_parser import DbtParser, TDatadiffConfig
from data_diff.diff_tables import DiffResultWrapper
from data_diff.errors import (
    DataDiffCustomSchemaNoConfigError,
    DataDiffDbtProjectVarsNotFoundError,
)
from data_diff.format import jsonify, jsonify_error
from data_diff.utils import (
    LogStatusHandler,
    columns_added_template,
    columns_removed_template,
    columns_type_changed_template,
    getLogger,
    no_differences_template,
    print_version_info,
)

logger = getLogger(__name__)


class TDiffVars(pydantic.BaseModel):
    dev_path: list[str]
    prod_path: list[str]
    primary_keys: list[str]
    connection: dict[str, str | None]
    threads: int | None = None
    where_filter: str | None = None
    include_columns: list[str]
    exclude_columns: list[str]
    dbt_model: str | None = None
    stats_flag: bool = False


def dbt_diff(
    profiles_dir_override: str | None = None,
    project_dir_override: str | None = None,
    dbt_selection: str | None = None,
    json_output: bool = False,
    state: str | None = None,
    log_status_handler: LogStatusHandler | None = None,
    where_flag: str | None = None,
    stats_flag: bool = False,
    columns_flag: tuple[str, ...] | None = None,
    production_database_flag: str | None = None,
    production_schema_flag: str | None = None,
) -> None:
    print_version_info()
    dbt_parser = DbtParser(profiles_dir_override, project_dir_override, state)
    models = dbt_parser.get_models(dbt_selection)
    config = dbt_parser.get_datadiff_config()

    if not state and not (config.prod_database or config.prod_schema):
        doc_url = "https://github.com/datafold/data-diff"
        raise DataDiffDbtProjectVarsNotFoundError(
            f"""vars: data_diff: section not found in dbt_project.yml.\n\nTo solve this, please configure your dbt project: \n{doc_url}\n\nOr specify a production manifest using the `--state` flag."""
        )

    dbt_parser.set_connection()

    futures = {}

    with (
        log_status_handler.status if log_status_handler else nullcontext(),
        ThreadPoolExecutor(max_workers=dbt_parser.threads) as executor,
    ):
        for model in models:
            if log_status_handler:
                log_status_handler.set_prefix(f"Diffing {model.alias} \n")

            diff_vars = _get_diff_vars(
                dbt_parser,
                config,
                model,
                where_flag,
                stats_flag,
                columns_flag,
                production_database_flag,
                production_schema_flag,
            )

            # we won't always have a prod path when using state
            # when the model DNE in prod manifest, skip the model diff
            if (
                state and len(diff_vars.prod_path) < 2
            ):  # < 2 because some providers like databricks can legitimately have *only* 2
                diff_output_str = _diff_output_base(".".join(diff_vars.dev_path), ".".join(diff_vars.prod_path))
                diff_output_str += "[green]New model: nothing to diff![/] \n"
                rich.print(diff_output_str)
                continue

            if diff_vars.primary_keys:
                future = executor.submit(_local_diff, diff_vars, json_output, log_status_handler)
                futures[future] = model
            else:
                if json_output:
                    print(
                        json.dumps(
                            jsonify_error(
                                table1=diff_vars.prod_path,
                                table2=diff_vars.dev_path,
                                dbt_model=diff_vars.dbt_model,
                                error="No primary key found. Add uniqueness tests, meta, or tags.",
                            )
                        ),
                        flush=True,
                    )
                else:
                    rich.print(
                        _diff_output_base(".".join(diff_vars.dev_path), ".".join(diff_vars.prod_path))
                        + "Skipped due to unknown primary key. Add uniqueness tests, meta, or tags.\n"
                    )

    errors = []
    for future in as_completed(futures):
        model = futures[future]
        try:
            future.result()
        except Exception as e:
            logger.error(f"Diff task failed for {model.unique_id}: {e}")
            errors.append((model.unique_id, e))

    if errors:
        raise RuntimeError(f"{len(errors)} diff task(s) failed. First error: {errors[0][1]}")


def _get_diff_vars(
    dbt_parser: "DbtParser",
    config: TDatadiffConfig,
    model,
    where_flag: str | None = None,
    stats_flag: bool = False,
    columns_flag: tuple[str, ...] | None = None,
    production_database_flag: str | None = None,
    production_schema_flag: str | None = None,
) -> TDiffVars:
    cli_columns = list(columns_flag) if columns_flag else []
    dev_database = model.database
    dev_schema = model.schema_
    dev_alias = prod_alias = model.alias
    primary_keys = dbt_parser.get_pk_from_model(model, dbt_parser.unique_columns, "primary-key")

    # prod path is constructed via configuration or the prod manifest via --state
    if dbt_parser.prod_manifest_obj:
        prod_database, prod_schema, prod_alias = _get_prod_path_from_manifest(model, dbt_parser.prod_manifest_obj)
    else:
        prod_database, prod_schema = _get_prod_path_from_config(config, model, dev_database, dev_schema)

    # cli flags take precedence over any project level config
    prod_database = production_database_flag or prod_database
    prod_schema = production_schema_flag or prod_schema

    if dbt_parser.requires_upper:
        dev_qualified_list = [x.upper() for x in [dev_database, dev_schema, dev_alias] if x]
        prod_qualified_list = [x.upper() for x in [prod_database, prod_schema, prod_alias] if x]
        primary_keys = [x.upper() for x in primary_keys]
    else:
        dev_qualified_list = [x for x in [dev_database, dev_schema, dev_alias] if x]
        prod_qualified_list = [x for x in [prod_database, prod_schema, prod_alias] if x]

    datadiff_model_config = dbt_parser.get_datadiff_model_config(model.meta)

    return TDiffVars(
        dbt_model=model.unique_id,
        dev_path=dev_qualified_list,
        prod_path=prod_qualified_list,
        primary_keys=primary_keys,
        connection=dbt_parser.connection,
        threads=dbt_parser.threads,
        # cli flags take precedence over any model level config
        where_filter=where_flag or datadiff_model_config.where_filter,
        include_columns=cli_columns or datadiff_model_config.include_columns,
        exclude_columns=[] if cli_columns else datadiff_model_config.exclude_columns,
        stats_flag=stats_flag,
    )


def _get_prod_path_from_config(config, model, dev_database, dev_schema) -> tuple[str, str]:
    # "custom" dbt config database
    if model.config.database:
        prod_database = model.config.database
    elif config.prod_database:
        prod_database = config.prod_database
    else:
        prod_database = dev_database

    # prod schema name differs from dev schema name
    if config.prod_schema:
        custom_schema = model.config.schema_

        # the model has a custom schema config(schema='some_schema')
        if custom_schema:
            if not config.prod_custom_schema:
                raise DataDiffCustomSchemaNoConfigError(
                    f"Found a custom schema on model {model.name}, but no value for\nvars:\n  data_diff:\n    prod_custom_schema:\nPlease set a value or utilize the `--state` flag!\n\n"
                    + "For more details see: https://github.com/datafold/data-diff"
                )
            prod_schema = config.prod_custom_schema.replace("<custom_schema>", custom_schema)
            # no custom schema, use the default
        else:
            prod_schema = config.prod_schema
    else:
        prod_schema = dev_schema
    return prod_database, prod_schema


def _get_prod_path_from_manifest(model, prod_manifest) -> tuple[str, str, str] | tuple[None, None, None]:
    prod_database = None
    prod_schema = None
    prod_alias = None
    prod_model = prod_manifest.nodes.get(model.unique_id, None)
    if prod_model:
        prod_database = prod_model.database
        prod_schema = prod_model.schema_
        prod_alias = prod_model.alias
    return prod_database, prod_schema, prod_alias


def _local_diff(
    diff_vars: TDiffVars, json_output: bool = False, log_status_handler: LogStatusHandler | None = None
) -> None:
    if log_status_handler:
        log_status_handler.diff_started(diff_vars.dev_path[-1])
    dev_qualified_str = ".".join(diff_vars.dev_path)
    prod_qualified_str = ".".join(diff_vars.prod_path)
    diff_output_str = _diff_output_base(dev_qualified_str, prod_qualified_str)

    table1 = connect_to_table(diff_vars.connection, prod_qualified_str, tuple(diff_vars.primary_keys))
    table2 = connect_to_table(diff_vars.connection, dev_qualified_str, tuple(diff_vars.primary_keys))

    try:
        table1_columns = table1.get_schema()
    except Exception as ex:
        logger.warning(f"Could not fetch schema for {prod_qualified_str}: {type(ex).__name__}: {ex}")
        diff_output_str += f"[red]Could not access prod table: {type(ex).__name__}[/] \n"
        rich.print(diff_output_str)
        raise

    try:
        table2_columns = table2.get_schema()
    except Exception as ex:
        logger.warning(f"Could not fetch schema for {dev_qualified_str}: {type(ex).__name__}: {ex}")
        diff_output_str += f"[red]Could not access dev table: {type(ex).__name__}[/] \n"
        rich.print(diff_output_str)
        raise

    table1_column_names = set(table1_columns.keys())
    table2_column_names = set(table2_columns.keys())
    column_set = table1_column_names.intersection(table2_column_names)
    columns_added = table2_column_names.difference(table1_column_names)
    columns_removed = table1_column_names.difference(table2_column_names)
    # col type is i = 1 in tuple
    columns_type_changed = {
        k for k, v in table2_columns.items() if k in table1_columns and v.data_type != table1_columns[k].data_type
    }

    diff_output_str += f"Primary Keys: {diff_vars.primary_keys} \n"

    if diff_vars.where_filter:
        diff_output_str += f"Where Filter: '{diff_vars.where_filter!s}' \n"

    if diff_vars.include_columns:
        diff_output_str += f"Included Columns: {diff_vars.include_columns} \n"

    if diff_vars.exclude_columns:
        diff_output_str += f"Excluded Columns: {diff_vars.exclude_columns} \n"

    if columns_removed:
        diff_output_str += columns_removed_template(columns_removed)

    if columns_added:
        diff_output_str += columns_added_template(columns_added)

    if columns_type_changed:
        diff_output_str += columns_type_changed_template(columns_type_changed)
        column_set = column_set.difference(columns_type_changed)

    column_set = column_set - set(diff_vars.primary_keys)

    if diff_vars.include_columns:
        column_set = {x for x in column_set if x.upper() in [y.upper() for y in diff_vars.include_columns]}

    if diff_vars.exclude_columns:
        column_set = {x for x in column_set if x.upper() not in [y.upper() for y in diff_vars.exclude_columns]}

    extra_columns = tuple(column_set)

    diff: DiffResultWrapper = diff_tables(
        table1,
        table2,
        threaded=True,
        algorithm=Algorithm.JOINDIFF,
        extra_columns=extra_columns,
        where=diff_vars.where_filter,
        skip_null_keys=True,
    )
    if json_output:
        # drain the iterator to get accumulated stats in diff.info_tree
        try:
            list(diff)
        except Exception as e:
            print(
                json.dumps(
                    jsonify_error(list(table1.table_path), list(table2.table_path), diff_vars.dbt_model, str(e))
                ),
                flush=True,
            )
            return

        dataset1_columns = [
            (info.column_name, info.data_type, table1.database.dialect.parse_type(table1.table_path, info))
            for info in table1_columns.values()
        ]
        dataset2_columns = [
            (info.column_name, info.data_type, table2.database.dialect.parse_type(table2.table_path, info))
            for info in table2_columns.values()
        ]

        print(
            json.dumps(
                jsonify(
                    diff,
                    dbt_model=diff_vars.dbt_model,
                    dataset1_columns=dataset1_columns,
                    dataset2_columns=dataset2_columns,
                    with_summary=True,
                    columns_diff={
                        "added": columns_added,
                        "removed": columns_removed,
                        "changed": columns_type_changed,
                    },
                    stats_only=diff_vars.stats_flag,
                )
            ),
            flush=True,
        )
        return

    if list(diff):
        diff_output_str += f"{diff.get_stats_string(is_dbt=True)} \n"
        rich.print(diff_output_str)
    else:
        diff_output_str += no_differences_template()
        rich.print(diff_output_str)

    if log_status_handler:
        log_status_handler.diff_finished(diff_vars.dev_path[-1])


def _diff_output_base(dev_path: str, prod_path: str) -> str:
    return f"\n[blue]{prod_path}[/] <> [green]{dev_path}[/] \n"
