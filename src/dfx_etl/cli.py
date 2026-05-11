import argparse
import logging
import os.path
import sys
from dfx_etl.database import get_engine, create_tables
from dfx_etl.pipelines import get_pipeline, list_pipelines
from sqlalchemy import inspect
import psycopg
import pandas as pd
import io
from dfx_etl.settings import SETTINGS
from dfx_etl.storage import  get_storage
from pathlib import Path
from dfx_etl.storage._base import FORMATS
logger = logging.getLogger(__name__)
STEPS = ["retrieve", "transform", "load"]
PIPELINES = list_pipelines()


def ingest_source(df: pd.DataFrame, engine, src_name: str):
    """
    High-speed atomic ingestion for the dfx schema.
    Resolves Natural Keys to Surrogate Keys and uses binary COPY.
    """
    # 1. Prepare Connection Strings
    raw_url = str(engine.url.render_as_string(hide_password=False))
    raw_url = raw_url.replace('postgresql+psycopg://', 'postgresql://')

    with psycopg.connect(raw_url) as conn:
        with conn.cursor() as cur:
            # 2. LAND IN THE CORRECT NAMESPACE
            cur.execute("SET search_path TO dfx")

            # 3. METADATA REGISTRY (Register names to get IDs)
            # Register Indicators
            unique_inds = df[['indicator_name']].drop_duplicates()
            cur.executemany(
                "INSERT INTO indicator (name, provider) VALUES (%s, %s) ON CONFLICT (name) DO NOTHING",
                [(r.indicator_name, src_name) for r in unique_inds.itertuples(index=False)]
            )

            # Register Dimensions
            unique_dims = df[['dimension']].drop_duplicates()
            cur.executemany(
                "INSERT INTO dimension (name) VALUES (%s) ON CONFLICT (name) DO NOTHING",
                [(r.dimension,) for r in unique_dims.itertuples(index=False)]
            )

            # 4. FETCH ID MAPPINGS
            cur.execute("SELECT name, id FROM indicator")
            ind_map = dict(cur.fetchall())

            cur.execute("SELECT name, id FROM dimension")
            dim_map = dict(cur.fetchall())

            cur.execute("SELECT iso_3, id FROM country")
            cty_map = dict(cur.fetchall())

            # 5. RESOLVE IDs IN DATAFRAME
            df['indicator_id'] = df['indicator_name'].map(ind_map)
            df['dimension_id'] = df['dimension'].map(dim_map)
            df['country_id'] = df['country_code'].map(cty_map)

            # 6. ARCHITECT'S DIAGNOSTIC GUARD
            # We check all 3 FKs. If one is NaN, the row is an 'Orphan'
            required_ids = ['country_id', 'indicator_id', 'dimension_id']
            missing_mask = df[required_ids].isnull().any(axis=1)

            if missing_mask.any():
                num_dropped = missing_mask.sum()
                logger.warning(f"Dropping {num_dropped} rows due to unmapped IDs in {src_name}")
                # Log the first 5 dropped rows to see the culprit (e.g. PER 2022)
                print("--- DROPPED ROWS SAMPLE ---")
                print(df[missing_mask][['country_code', 'indicator_name', 'year', 'dimension']].head())

            # Only keep rows that fully resolved
            df_clean = df.dropna(subset=required_ids).copy()

            # Convert to int for the binary COPY protocol
            for col in required_ids:
                df_clean[col] = df_clean[col].astype(int)

            # 7. HIGH-SPEED BINARY INGESTION
            # Create temp table matching 'series'
            cur.execute("CREATE TEMP TABLE stage_series (LIKE series INCLUDING ALL) ON COMMIT DROP")

            # Stream clean data via CSV buffer
            cols = ['country_id', 'indicator_id', 'dimension_id', 'year', 'value']
            buffer = io.StringIO()
            df_clean[cols].to_csv(buffer, index=False, header=False, sep='\t')
            buffer.seek(0)

            with cur.copy("COPY stage_series FROM STDIN") as copy:
                copy.write(buffer.read())

            # 8. ATOMIC UPSERT
            # This handles the 'On Conflict' logic for the 11.7M rows
            cur.execute("""
                INSERT INTO series (country_id, indicator_id, dimension_id, year, value)
                SELECT country_id, indicator_id, dimension_id, year, value FROM stage_series
                ON CONFLICT ON CONSTRAINT series_pkey 
                DO UPDATE SET value = EXCLUDED.value;
            """)

        # Commit everything as one atomic source transaction
        conn.commit()
        logger.info(f"Ingestion complete for {src_name}. Clean rows: {len(df_clean)}")


def setup_logs(level=None):

    azlogger = logging.getLogger('azure')
    azlogger.setLevel(logging.WARNING)
    httpx_logger = logging.getLogger('httpx')
    httpx_logger.setLevel(logging.WARNING)


class Formatter(
    argparse.ArgumentDefaultsHelpFormatter,
    argparse.RawDescriptionHelpFormatter,
):
    pass


def configure_logging() -> logging.Logger:
    logger = logging.getLogger()

    # Avoid duplicate handlers if main() is called multiple times
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(
            "%(asctime)s-%(filename)s:%(funcName)s:%(lineno)d:%(levelname)s:%(message)s",
            "%Y-%m-%d %H:%M:%S",
        ))
        logger.addHandler(handler)
        logger.propagate = False
    logger.name = __name__
    logger.setLevel(logging.INFO)
    return logger


EPILOG = f"""
        Tips: 
            omit -src to execute all pipelines: {', '.join(PIPELINES)}
            omit -step to execute all steps: {', '.join(STEPS)}
        Usage:
            dfx-etl -src imf_datamapper_api -dst /data/tmp/dfxp -step retrieve
            uv run dfx-etl -src imf_datamapper_api -dst /data/tmp/dfxp -step retrieve

        """


def build_parser() -> argparse.ArgumentParser:
    """
    Build an argparse parser suitable ot launch pipelines from command line
    Returns
    -------

    """
    parser = argparse.ArgumentParser(
        prog="dfx-etl",
        description="Run dfx_etl pipelines and store data  locally or on Azure ",
        formatter_class=Formatter,
        epilog=EPILOG
    )

    parser.add_argument(
        "-src",
        type=str,
        required=False,
        nargs='+',
        choices=PIPELINES,
        metavar="SOURCE",
        help="Execute DFx pipeline on one or multiple sources. (choices: %(choices)s)",
    )

    parser.add_argument(
        "-step",
        type=str,
        required=False,
        choices=STEPS,
        metavar="STEP",
        help="The step of the specified pipeline (choices: %(choices)s)",
    )

    parser.add_argument(
        "-dst",
        type=Path,
        required=False,
        metavar='PATH',
        help="A path to a directory on the local disk. The storage where the intermediary parquet files will be stored. ",
    )

    parser.add_argument(
        "-f", "--format",
        type=str,
        choices=FORMATS,
        default="parquet",  # Setting a default is usually best practice
        metavar="FMT",  # Keeps the help menu clean (prevents listing choices in the usage line)
        help="The serialization format for the output files. Parquet is recommended for performance. "
             "(choices: %(choices)s)",
    )
    parser.add_argument(
        "-d", "--debug", help="Enable debug logging", action="store_true"
    )

    return parser


def main(argv: list[str] | None = None) -> int:

    setup_logs()
    parser = build_parser()

    if argv is None:
        argv = sys.argv[1:]

    if not argv:
        parser.print_help()
        return 1

    args = parser.parse_args(argv)
    logger = configure_logging()
    # validate
    dst_folder = args.dst
    if dst_folder is not None:
        if not os.path.isabs(dst_folder):
            dst_folder = os.path.abspath(dst_folder)
        if not os.path.exists(dst_folder):
            os.makedirs(dst_folder)
        SETTINGS.local_storage = dst_folder

    _storage = get_storage()
    logger.info(f'Using {_storage} to save data')



    if args.debug:
        logger.setLevel(logging.DEBUG)

    format = args.format

    for src in args.src:
        pipeline = get_pipeline(src)



        if args.step == 'retrieve':
            df = pipeline.retrieve().df_raw
        if args.step == 'transform':
            df = pipeline.retrieve().transform().df_transformed
        if args.step in ['load', None]:
            df = pipeline()

        pipeline._storage.write_dataset(df, folder_path=dst_folder, format=format)
        # num_rows, num_cols = df.shape
        # logger.info(f'{num_rows} rows and {num_cols} columns worth of data was written to {df.name}.parquet')
        #
        # # TODO push to db
        # if args.step in ('transform', 'load', None):
        #     engine = get_engine()
        #     tables = inspect(engine).get_table_names()
        #     if not tables:
        #         tn = create_tables(engine=engine)
        #         assert len(tn) == 4, f'Failed to create the tables in DB'
        #
        #     if args.persist:
        #         logger.info(f"Processing database push for source: {src}")
        #         ingest_source(df, engine, src_name=src)
        #         logger.info(f"Source {src} is now synchronized in dfx schema.")

    return 0




