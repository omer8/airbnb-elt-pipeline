"""
Airbnb Data Pipeline — End-to-End ELT
Flow:
    S3 (listings / hosts / bookings)
    → Snowflake Staging (COPY INTO via External Stage)
    → dbt Source Tests
    → dbt Transformations (Bronze → Silver → Gold → Marts)
    → dbt SCD Type 2 Snapshots (listings / hosts / bookings)
    → dbt Model Tests
    → Email Notification (✅ Success | ❌ Failure)

Orchestration : Apache Airflow (Cosmos for dbt)
Schedule      : Every hour (@hourly)
Timezone      : Africa/Cairo
Start Date    : 2026-03-04
Max Active    : 1 run at a time
Notifications : omarmeto268@gmail.com
"""

# ============================================================
# Imports
# ============================================================

from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, task_group
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.smtp.operators.smtp import EmailOperator
from pendulum import timezone

from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ExecutionMode,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.operators.local import DbtSnapshotLocalOperator, DbtTestLocalOperator
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# ============================================================
# Constants
# ============================================================

S3_BUCKET          = 'omar-airbnb-data'
S3_PREFIX_LISTINGS = 'raw/listings/'
S3_PREFIX_HOSTS    = 'raw/hosts/'
S3_PREFIX_BOOKINGS = 'raw/bookings/'

AWS_CONN_ID       = 'aws-conn'
SNOWFLAKE_CONN_ID = 'snowflake-conn'
EMAIL_CONN_ID     = 'email-conn'
NOTIFY_EMAIL      = 'omarmeto268@gmail.com'

DBT_PROJECT_DIR = Path(__file__).parent / 'airbnb-analytics'

# ============================================================
# Default Arguments
# ============================================================

default_args = {
    'owner':            'data-engineering',
    'email':            [NOTIFY_EMAIL],
    'depends_on_past':  False,
    'email_on_failure': False,
    'email_on_retry':   True,
    'retries':          1,
    'retry_delay':      timedelta(minutes=1),
}

# ============================================================
# Cosmos Shared Config
# ============================================================

profile_config = ProfileConfig(
    profile_name='airbnb_dbt',
    target_name='dev',
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id=SNOWFLAKE_CONN_ID,
        profile_args={
            'database':  'AIRBNB_DB',
            'schema':    'STAGING',
            'warehouse': 'AIRBNB_WH',
            'role':      'ACCOUNTADMIN',
        },
    ),
)

execution_config = ExecutionConfig(execution_mode=ExecutionMode.LOCAL)

project_config = ProjectConfig(dbt_project_path=DBT_PROJECT_DIR)


# ============================================================
# DAG Definition
# ============================================================

@dag(
    dag_id='airbnb_elt_pipeline',
    default_args=default_args,
    description='End-to-end Airbnb ELT: S3 → Snowflake → dbt',
    schedule='@hourly',
    start_date=datetime(2026, 3, 4, tzinfo=timezone('Africa/Cairo')),
    catchup=False,
    max_active_runs=1,
    tags=['airbnb', 'elt', 'dbt', 'snowflake', 's3'],
)
def airbnb_elt_pipeline():

    # ── 1. Ingest raw data from S3 into Snowflake staging ──────────────────

    @task_group(group_id='data_ingestion', tooltip='Wait for S3 files and COPY into Snowflake staging')
    def data_ingestion():

        # Listings
        wait_listings = S3KeySensor(
            task_id='wait_for_s3_listings',
            bucket_name=S3_BUCKET,
            bucket_key=f'{S3_PREFIX_LISTINGS}*.csv',
            wildcard_match=True,
            aws_conn_id=AWS_CONN_ID,
            timeout=300,
            poke_interval=30,
            mode='reschedule',
        )
        copy_listings = SQLExecuteQueryOperator(
            task_id='copy_listings_to_snowflake',
            conn_id=SNOWFLAKE_CONN_ID,
            autocommit=True,
            sql="""
                USE SCHEMA AIRBNB_DB.STAGING;
                COPY INTO staging.raw_listings
                FROM (
                    SELECT
                        $1::NUMBER,         -- listing_id
                        $2::NUMBER,         -- host_id
                        $3::STRING,         -- property_type
                        $4::STRING,         -- room_type
                        $5::STRING,         -- city
                        $6::STRING,         -- country
                        $7::NUMBER,         -- accommodates
                        $8::NUMBER,         -- bedrooms
                        $9::NUMBER,         -- bathrooms
                        $10::NUMBER,        -- price_per_night
                        CURRENT_TIMESTAMP() -- created_at
                    FROM @airbnb_s3_stage/listings/
                )
                PATTERN     = '.*listings(_.*)?\\.csv'
                FILE_FORMAT = csv_format
                ON_ERROR    = abort_statement
                PURGE       = FALSE;
            """,
        )

        # Hosts
        wait_hosts = S3KeySensor(
            task_id='wait_for_s3_hosts',
            bucket_name=S3_BUCKET,
            bucket_key=f'{S3_PREFIX_HOSTS}*.csv',
            wildcard_match=True,
            aws_conn_id=AWS_CONN_ID,
            timeout=300,
            poke_interval=30,
            mode='reschedule',
        )
        copy_hosts = SQLExecuteQueryOperator(
            task_id='copy_hosts_to_snowflake',
            conn_id=SNOWFLAKE_CONN_ID,
            autocommit=True,
            sql="""
                USE SCHEMA AIRBNB_DB.STAGING;
                COPY INTO staging.raw_hosts
                FROM (
                    SELECT
                        $1::NUMBER,         -- host_id
                        $2::STRING,         -- host_name
                        $3::DATE,           -- host_since
                        $4::BOOLEAN,        -- is_superhost
                        $5::NUMBER,         -- response_rate
                        CURRENT_TIMESTAMP() -- created_at
                    FROM @airbnb_s3_stage/hosts/
                )
                PATTERN     = '.*hosts(_.*)?\\.csv'
                FILE_FORMAT = csv_format
                ON_ERROR    = abort_statement
                PURGE       = FALSE;
            """,
        )

        # Bookings
        wait_bookings = S3KeySensor(
            task_id='wait_for_s3_bookings',
            bucket_name=S3_BUCKET,
            bucket_key=f'{S3_PREFIX_BOOKINGS}*.csv',
            wildcard_match=True,
            aws_conn_id=AWS_CONN_ID,
            timeout=300,
            poke_interval=30,
            mode='reschedule',
        )
        copy_bookings = SQLExecuteQueryOperator(
            task_id='copy_bookings_to_snowflake',
            conn_id=SNOWFLAKE_CONN_ID,
            autocommit=True,
            sql="""
                USE SCHEMA AIRBNB_DB.STAGING;
                COPY INTO staging.raw_bookings
                FROM (
                    SELECT
                        $1::STRING,         -- booking_id
                        $2::NUMBER,         -- listing_id
                        $3::TIMESTAMP,      -- booking_date
                        $4::NUMBER,         -- nights_booked
                        $5::NUMBER,         -- booking_amount
                        $6::NUMBER,         -- cleaning_fee
                        $7::NUMBER,         -- service_fee
                        $8::STRING,         -- booking_status
                        CURRENT_TIMESTAMP() -- created_at
                    FROM @airbnb_s3_stage/bookings/
                )
                PATTERN     = '.*bookings(_.*)?\\.csv'
                FILE_FORMAT = csv_format
                ON_ERROR    = abort_statement
                PURGE       = FALSE;
            """,
        )

        # All three run in parallel
        wait_listings >> copy_listings
        wait_hosts    >> copy_hosts
        wait_bookings >> copy_bookings

    # ── 2. Validate raw sources before transforming ─────────────────────────

    dbt_test_sources = DbtTestLocalOperator(
        task_id='dbt_test_sources',
        project_dir=DBT_PROJECT_DIR,
        profile_config=profile_config,
        select='source:*',
    )

    # ── 3. Transform: Bronze → Silver → Gold (dims + facts) → Marts ─────────

    dbt_transformations = DbtTaskGroup(
        group_id='dbt_transformations',
        profile_config=profile_config,
        execution_config=execution_config,
        project_config=project_config,
        render_config= RenderConfig(
            select=['tag:bronze', 'tag:silver', 'tag:gold', 'tag:marts'],
        )
    )

    # ── 4. Snapshots: SCD Type 2 for slowly changing dimensions ─────────────

    @task_group(group_id='dbt_snapshots', tooltip='SCD Type 2 snapshots via Cosmos')
    def dbt_snapshots():
        DbtSnapshotLocalOperator(
            task_id='snapshot_listings',
            project_dir=DBT_PROJECT_DIR,
            profile_config=profile_config,
            select='listings_snapshot',
        )
        DbtSnapshotLocalOperator(
            task_id='snapshot_hosts',
            project_dir=DBT_PROJECT_DIR,
            profile_config=profile_config,
            select='hosts_snapshot',
        )
        DbtSnapshotLocalOperator(
            task_id='snapshot_bookings',
            project_dir=DBT_PROJECT_DIR,
            profile_config=profile_config,
            select='bookings_snapshot',
        )

    # ── 5. Test all transformed models (sources already tested in step 2) ───

    dbt_tests = DbtTestLocalOperator(
        task_id='dbt_tests',
        project_dir=DBT_PROJECT_DIR,
        profile_config=profile_config,
        exclude='source:*',
    )

    # ── 6. Notifications ────────────────────────────────────────────────────

    send_success_notification = EmailOperator(
        task_id='send_success_notification',
        conn_id=EMAIL_CONN_ID,
        to=NOTIFY_EMAIL,
        from_email=NOTIFY_EMAIL,
        trigger_rule='all_success',
        subject='✅ Airbnb ELT Pipeline Succeeded — {{ ds }}',
        html_content="""
            <h3>✅ Airbnb ELT Pipeline Succeeded</h3>
            <ul>
                <li><b>DAG</b>: {{ dag.dag_id }}</li>
                <li><b>Execution Date</b>: {{ ds }}</li>
                <li><b>Run ID</b>: {{ run_id }}</li>
            </ul>
        """,
    )

    send_failure_notification = EmailOperator(
        task_id='send_failure_notification',
        conn_id=EMAIL_CONN_ID,
        to=NOTIFY_EMAIL,
        from_email=NOTIFY_EMAIL,
        trigger_rule='one_failed',
        subject='❌ Airbnb ELT Pipeline Failed — {{ ds }}',
        html_content="""
            <h3>❌ Airbnb ELT Pipeline Failed</h3>
            <ul>
                <li><b>DAG</b>: {{ dag.dag_id }}</li>
                <li><b>Failed Task(s)</b>: {{ task_instance.xcom_pull(task_ids='get_failed_task') }}</li>
                <li><b>Execution Date</b>: {{ ds }}</li>
                <li><b>Logs</b>: <a href="{{ ti.log_url }}">View Logs</a></li>
            </ul>
        """,
    )

    # ── 7. Pipeline Flow ────────────────────────────────────────────────────

    start = EmptyOperator(task_id='start')
    end   = EmptyOperator(task_id='end', trigger_rule='none_failed_min_one_success')

    ingestion = data_ingestion()
    snapshots = dbt_snapshots()

    # Happy path: ingest → validate sources → transform → snapshot → test → notify ✅
    start >> ingestion >> dbt_test_sources >> dbt_transformations >> snapshots >> dbt_tests >> send_success_notification >> end

    # Failure path: any stage fails → failure email → end ❌
    [ingestion, dbt_test_sources, dbt_transformations, snapshots, dbt_tests] >> send_failure_notification >> end


airbnb_elt_pipeline()