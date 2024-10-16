# =============== IMPORT ===============
from dagster import (
    asset, Definitions
)
from dagster_duckdb import DuckDBResource

import pandas as pd
import logging
import os

# =============== CONFIG ===============
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =============== DAGSTER ASSETS ===============
# =============== DATA PREP ASSETS ===============
@asset(
    group_name="initial_preparation",
    tags={"asset_type":"csv-file", 
          "data_source":"csv"},
    code_version="0.1",
    owners=["alvinnoza.data@yahoo.com", "team:data-scientist"],
    compute_kind="pandas"
)
def applications_table(duckdb: DuckDBResource) -> None:
    try:
        ## load ke pandas dulu
        applications_df = pd.read_csv("./datasets/credit_applications.csv")

        ## log dataframe
        context.log.info("credit applications: %s", credit_applications_table.shape)
        context.log.info("first five rows: \n%s", credit_applications_table.head())

        ## metadata assetnya dagster
        columns = [
            TableColumn(name=col, type=str(credit_applications_table[col].dtype),
                        description=f"sample value: {credit_applications_table[col].iloc[33]}")
            for col in credit_applications_table.columns
        ]

        n_rows = credit_applications_table.shape[0]
        n_cols = credit_applications_table.shape[1]

        yield MaterializeResult(
                metadata={
                    "dagster/column_schema": TableSchema(columns=columns),
                    "dagster/type": "pandas-dataframe",
                    "dagster/column_count": n_cols,
                    "dagster/row_count": n_rows
                }
            )


        ## dump ke duckdb table
        with duckdb.get_connection() as connection:
            connection.execute("CREATE TABLE applications.raw_applications AS SELECT * from applications_df")
            
    except Exception as e:
        context.log.error("Error saat dump data ke database: %s", str(e))
        raise e


defs = Definitions(
    assets=[applications_table],
    resources={
        "duckdb": DuckDBResource(
            database="path/to/my_duckdb_database.duckdb",
        )
    },
)