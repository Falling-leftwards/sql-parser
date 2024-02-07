"""Tests for the query_parser module."""
from src.query_parser import QueryParser
import logging
import pytest

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def test_query_parser_simple():
    """Test a simple query."""
    query = """SELECT column_A FROM DB1.TABLE_A"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "SELECT"
    assert qp.source_object == ["db1", "table_a"]


def test_simple_lower_case():
    """Test that Queryparser is case insensitive."""
    query = """select column_A from db1.table_A"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "SELECT"
    assert qp.source_object == ["db1", "table_a"]


def test_drop_table():
    """Test to parse a DROP TABLE query."""
    query = """DROP TABLE DEV_DB.TABLE_B"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "DROP TABLE"
    assert qp.source_object == ["dev_db", "table_b"]


def test_drop_database():
    """Test to parse a DROP DATABASE query."""
    query = """DROP DATABASE SB_PRODUCTION"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "DROP DATABASE"
    assert qp.source_object == ["sb_production"]


def test_remove():
    """Test to parse a REMOVE query."""
    query = """REMOVE
    @'LANDING_DATA'.'PUBLIC'.'ATTREP_IS_LANDING_DATA_6af21a79_b97f_7c4f_8513_967ff6a786ce'/6af21a79_b97f_7c4f_8513_967ff6a786ce/0/CDC00001296.csv'"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "REMOVE"
    assert qp.source_object == [
        "landing_data",
        "public",
        "attrep_is_landing_data_6af21a79_b97f_7c4f_8513_967ff6a786ce/6af21a79_b97f_7c4f_8513_967ff6a786ce/0/cdc00001296",
        "csv",
    ]


def test_redacted():
    """Test to parse a redacted query.

    Snowflake redacts some queries, so we need to handle them.
    """
    query = """<redacted>"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "<REDACTED>"
    assert qp.source_object == ['Not identified']


def test_truncate():
    """Test to parse a TRUNCATE TABLE query."""
    query = """TRUNCATE TABLE 'DATA_LOADER_STATUS'.'attrep_changesD8492F6541312D7'"""
    qp = QueryParser(logger, query )
    assert qp.query_type == "TRUNCATE TABLE"
    assert qp.source_object == ["data_loader_status", "attrep_changesd8492f6541312d7"]


def test_alter_session():
    """Test to parse an ALTER SESSION query."""
    query = """ALTER SESSION SET JDBC_QUERY_RESULT_FORMAT='JSON'"""
    qp = QueryParser(logger, query )
    assert qp.query_type == "ALTER SESSION"
    assert qp.source_object == ['Not identified']



def test_create_or_replace():
    """Test to parse a CREATE OR REPLACE TABLE query."""
    query = """CREATE OR REPLACE TABLE "DB1"."TABLE_A" AS SELECT * FROM "DB2"."TABLE_B" """
    qp = QueryParser(logger, query)
    assert qp.query_type == "CREATE OR REPLACE"
    assert qp.source_object == ["db1", "table_a"]


def test_grant_query():
    """Test to parse a GRANT query."""
    query = """GRANT SELECT ON DB1.TABLE_A TO ROLE DBT_ROLE"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "GRANT"
    assert qp.source_object == ["db1", "table_a"]


def test_ALTER():
    """Test to parse an ALTER task query."""
    query = """ALTER TASK "LANDING_DATA"."PUBLIC"."Clone from prod" SUSPEND"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "ALTER TASK"
    assert qp.source_object == ["landing_data", "public", "clone"]


def test_ALTER_ACCOUNT():
    """Test to parse an ALTER ACCOUNT query."""
    query = """alter ACCOUNT set NETWORK_POLICY = 'MELTANO_NX'"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "ALTER ACCOUNT"
    assert qp.source_object == ["network_policy"]


def test_ALTER_NETWORK_POLICY():
    """Test to parse an ALTER NETWORK POLICY query."""
    query = """ALTER NETWORK POLICY COMP_NX SET ALLOWED_IP_LIST =
    (123.2314.21231.342')"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "ALTER NETWORK"
    assert qp.source_object == ["comp_nx"]


def test_ALTER_POLICY():
    """Test to parse an ALTER POLICY query."""
    query = """alter row access policy dwh_comp.schema.ricdfv set body ->  exists ( select 1            from  dwh_comp.data_governance.entitlements_for_row_access_policies a           where a.object_in_need_of_access_policy = UPPER('ricdfv')  and a.column_value_to_determine_access = TENANT  and   contains(CURRENT_ROLE(),a.aad_role_used_for_access));"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "ALTER ROW ACCESS"
    assert qp.source_object == ["dwh_comp", "schema", "ricdfv"]


def test_ALTER_SET_TAG():
    """Test to parse an ALTER SET TAG query."""
    query = """ALTER WAREHOUSE "DEMO_M" SET TAG "DDS_CORE_DATA_PLATFORM"."PLATFORM_MONITORING"."BUSINESS_UNIT" = 'CORE'"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "ALTER WAREHOUSE"
    assert qp.source_object == ["demo_m"]


def test_ALTER_TABLE():
    """Test to parse an ALTER TABLE query."""
    query = """alter table dwh_comp.ifwew.wefw cluster by (s_dk_date_no);"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "ALTER TABLE"
    assert qp.source_object == ["dwh_comp", "ifwew", "wefw"]


def test_ALTER_TABLE_ADD_COLUMN():
    """Test to parse an ALTER TABLE query with quotation marks."""
    query = """ALTER TABLE "TEMP_511__ESGAIA_CONTROVERSIES_API" ADD COLUMN "__HEVO__CONSUMPTION_ID" BIGINT DEFAULT 0"""
    qp = QueryParser(logger, query)
    assert qp.query == query.upper()
    assert qp.query_type == "ALTER TABLE"
    assert qp.source_object == ["temp_511__esgaia_controversies_api"]


def test_ALTER_TABLE_DROP_COLUMN():
    """Test to parse an ALTER TABLE query with quotation marks."""
    query = """ALTER TABLE DEV_DDS_COP.HDCEWV.JQNCQEKF DROP column EXTRACTED_DATE_PERIOD_S ;"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "ALTER TABLE"
    assert qp.source_object == ["dev_dds_cop", "hdcewv", "jqncqekf"]


def test_ALTER_TABLE_MANAGE_ROW_ACCESS_POLICY():
    """Test to parse an ALTER view query."""
    query = """alter view dwh_ceewf.dsvrv.brg_sh_party_tax_countrsaevy add row access policy rffr.vfffffffffff.vdfvrv on (TENANT)"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "ALTER VIEW"
    assert qp.source_object == ["dwh_ceewf", "dsvrv", "brg_sh_party_tax_countrsaevy"]


def test_ALTER_TABLE_MODIFY_COLUMN():
    """Test to parse an ALTER TABLE, with modify query."""
    query = (
        """ALTER TABLE DWH_SFVR.VREQR.VEQDR MODIFY column AUM_GROUP_TYPE TEXT(100) ;"""
    )
    qp = QueryParser(logger, query)
    assert qp.query_type == "ALTER TABLE"
    assert qp.source_object == ["dwh_sfvr", "vreqr", "veqdr"]


def test_ALTER_TAG_UNSET_ALLOWED_VALUES():
    """Test to parse an ALTER TAG query."""
    query = """ALTER TAG "DDS_CORE_DATA_PLATFORM"."PLATFORM_MONITORING"."BUSINESS_UNIT" UNSET ALLOWED_VALUES"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "ALTER TAG"
    assert qp.source_object == [
        "dds_core_data_platform",
        "platform_monitoring",
        "business_unit",
    ]


def test_ALTER_UNSET_TAG():
    """Test to parse an ALTER database tag query."""
    query = """ALTER DATABASE "DEV_LANDING_BOOST_AI" UNSET TAG "DDS_CORE_DATA_PLATFORM"."PLATFORM_MONITORING"."BUSINESS_UNIT"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "ALTER DATABASE"
    assert qp.source_object == ["dev_landing_boost_ai"]


def test_ALTER_USER():
    """Test to parse an ALTER USER query."""
    query = """ALTER USER sys_voqvnc_wefg SET DISABLED = FALSE"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "ALTER USER"
    assert qp.source_object == ["sys_voqvnc_wefg"]


def test_ALTER_VIEW_MODIFY_COLUMN_MANAGE_POLICY():
    """Test to parse an ALTER VIEW query."""
    query = """alter view  dev_rd_wolf.rd_skagenvps.vps_account__hist modify column  date_of_birth set masking policy dev_rd_wolf.rd_skagenvps.sam_fund_admin_sensitive_date;"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "ALTER VIEW"
    assert qp.source_object == ["dev_rd_wolf", "rd_skagenvps", "vps_account__hist"]


def test_ALTER_WAREHOUSE_RESUME():
    """Test to parse an ALTER WAREHOUSE query."""
    query = """ALTER WAREHOUSE "ANALYSIS_SAM_WH" RESUME;"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "ALTER WAREHOUSE"
    assert qp.source_object == ["analysis_sam_wh"]


def test_ALTER_WAREHOUSE_SUSPEND():
    """Test to parse an ALTER WAREHOUSE query."""
    query = """alter warehouse exporting_enterprise_xs suspend;"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "ALTER WAREHOUSE"
    assert qp.source_object == ["exporting_enterprise_xs"]


def test_BEGIN_TRANSACTION():
    """Test to parse a BEGIN TRANSACTION query."""
    query = """BEGIN"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "BEGIN"
    assert qp.source_object == ['unparceble']

def test_CALL():
    """Test to parse a CALL query."""
    query = """CALL "SNOWFLAKE"."LOCAL"."ACCOUNT_ROOT_BUDGET"!GET_SPENDING_LIMIT();"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "CALL"
    assert qp.source_object == ["snowflake", "local", "account_root_budget"]


def test_COMMIT():
    """Test to parse a COMMIT query."""
    query = """commit"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "COMMIT"
    assert qp.source_object == ['unparceble']


def test_COPY():
    """Test to parse a COPY query."""
    query = """COPY INTO "DATA_LOADER_STATUS"."attrep_changes248AD1592B3410B8"("seq"FROM '@"LANDING_SCD"."PUBLIC"."ATTREP_IS_LANDING_SCD_77047228_e4b5_ba45_9f3e_900c13ca4eb5"/77047228_e4b5_ba45_9f3e_900c13ca4eb5/0/') files = ('CDC00000FEE.csv.gz') force=true"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "COPY"
    assert qp.source_object == ["data_loader_status", "attrep_changes248ad1592b3410b8"]


def test_CREATE():
    """Test to parse a CREATE query."""
    query = """create schema if not exists rd_wolf.rd_skagenvps"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "CREATE SCHEMA"
    assert qp.source_object == ["rd_wolf", "rd_skagenvps"]


def test_CREATE_CONSTRAINT():
    """Test to parse a CREATE CONSTRAINT query."""
    query = """ALTER TABLE "DATA_LOADER_STATUS"."attrep_changes59CD484EC631CA9C" ADD CONSTRAINT "attrep_changes59CD484EC631CA9C_C631CA9C_PK" PRIMARY KEY ( "seq" )"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "ALTER TABLE"
    assert qp.source_object == ["data_loader_status", "attrep_changes59cd484ec631ca9c"]


def test_CREATE_MASKING_POLICY():
    """Test to parse a CREATE MASKING POLICY query."""
    query = """CREATE MASKING POLICY IF NOT EXISTS rd_wolf.rd_k3s_prod.sam_fund_admin_sensitive_number AS (val number) 
  RETURNS number ->
      CASE WHEN CURRENT_ROLE() IN ('DATA_ENGINEER_PLATFORM', 'DATA_ENGINEER_SAM', 'REPORTER_SAM_FUND_ADMIN', 'DATA_ANALYST_SAM_FUND_ADMINISTRATION_NO', 'DATA_ANALYST_SAM_FUND_ADMINISTRATION_SE', 'DATA_ENGINEER_GRC' ,'TRANSFORMER_PLATFORM', 'TRANSFORMER_SAM') THEN val 
      ELSE 9999999999999999999
      END"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "CREATE MASKING POLICY"
    assert qp.source_object == [
        "rd_wolf",
        "rd_k3s_prod",
        "sam_fund_admin_sensitive_number",
    ]


def test_CREATE_NETWORK_POLICY():
    """Test to parse a CREATE NETWORK POLICY query."""
    query = """CREATE NETWORK POLICY "deckwvN_NX" ALLOWED_IP_LIST=('234.541324.13245.314') COMMENT="A Networkpolicy for Snowflake created by Terrafrom."""
    qp = QueryParser(logger, query)
    assert qp.query_type == "CREATE NETWORK POLICY"
    assert qp.source_object == ["deckwvn_nx"]


def test_CREATE_ROLE():
    """Test to parse a CREATE ROLE query."""
    query = """CREATE ROLE "AR_DB_DDS_SAM_RISK_AND_OWNERSHIP_W" COMMENT='Access role created by Terraform'"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "CREATE ROLE"
    assert qp.source_object == ["ar_db_dds_sam_risk_and_ownership_w"]


def test_CREATE_ROW_ACCESS_POLICY():
    """Test to parse a CREATE ROW ACCESS POLICY query."""
    query = """create row access policy if not exists dwh_sam.fund_admin.dim_sh_party as (tenant varchar) returns boolean ->
exists (
          select 1 
          from  dwh_sam.data_governance.entitlements_for_row_access_policies a
          where a.object_in_need_of_access_policy = UPPER('dim_sh_party')
          and   a.column_value_to_determine_access = tenant
          and   contains(CURRENT_ROLE(),a.aad_role_used_for_access)
        )"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "CREATE ROW ACCESS"
    assert qp.source_object == ["dwh_sam", "fund_admin", "dim_sh_party"]


def test_CREATE_SESSION_POLICY():
    """Test to parse a CREATE SESSION POLICY query."""
    query = (
        """CREATE TAG "DDS_CORE_DATA_PLATFORM"."PLATFORM_MONITORING"."BUSINESS_UNIT"""
    )
    qp = QueryParser(logger, query)
    assert qp.query_type == "CREATE TAG"
    assert qp.source_object == [
        "dds_core_data_platform",
        "platform_monitoring",
        "business_unit",
    ]


def test_CREATE_TABLE():
    """Test to parse a CREATE TABLE query."""
    query = """create table dds_sam.dbt_cloud_pr_819_1104_intermediate.sust_field_type_correction (fieldid INTEGER,fieldtype VARCHAR(50))"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "CREATE TABLE"
    assert qp.source_object == [
        "dds_sam",
        "dbt_cloud_pr_819_1104_intermediate",
        "sust_field_type_correction",
    ]


def CREATE_TABLE_AS_SELECT():
    """Test to parse a CREATE TABLE AS Based on DBT SELECT query."""
    query = """create or replace temporary table "RD_SCD"."SNAPSHOTS"."SCD_TMSDAT__BUSINESSCLASSLEVEL1__SNAPSHOT__dbt_tmp"
         as
        (with snapshot_query as() 
SELECT
    *
FROM landing_scd.TMSDAT.businessclasslevel1
 

    ),

    snapshotted_data as (

        select *,
            bclev1ik as dbt_unique_key

        from "RD_SCD"."SNAPSHOTS"."SCD_TMSDAT__BUSINESSCLASSLEVEL1__SNAPSHOT"
        where dbt_valid_to is null

    ),

    insertions_source_data as (

        select
            *,
            bclev1ik as dbt_unique_key,
            data_loader_ts as dbt_updated_at,
            data_loader_ts as dbt_valid_from,
            nullif(data_loader_ts, data_loader_ts) as dbt_valid_to,
            md5(coalesce(cast(bclev1ik as varchar ), '')
         || '|' || coalesce(cast(data_loader_ts as varchar ), '')
        ) as dbt_scd_id

        from snapshot_query
    ),

    updates_source_data as (

        select
            *,
            bclev1ik as dbt_unique_key,
            data_loader_ts as dbt_updated_at,
            data_loader_ts as dbt_valid_from,
            data_loader_ts as dbt_valid_to

        from snapshot_query
    ),

    deletes_source_data as (

        select
            *,
            bclev1ik as dbt_unique_key
        from snapshot_query
    ),
    

    insertions as (

        select
            'insert' as dbt_change_type,
            source_data.*

        from insertions_source_data as source_data
        left outer join snapshotted_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where snapshotted_data.dbt_unique_key is null
           or (
                snapshotted_data.dbt_unique_key is not null
            and (
                (snapshotted_data.dbt_valid_from < source_data.data_loader_ts)
            )
        )

    ),

    updates as (

        select
            'update' as dbt_change_type,
            source_data.*,
            snapshotted_data.dbt_scd_id

        from updates_source_data as source_data
        join snapshotted_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where (
            (snapshotted_data.dbt_valid_from < source_data.data_loader_ts)
        )
    ),

    deletes as (

        select
            'delete' as dbt_change_type,
            source_data.*,
            to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) as dbt_valid_from,
            to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) as dbt_updated_at,
            to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) as dbt_valid_to,
            snapshotted_data.dbt_scd_id

        from snapshotted_data
        left join deletes_source_data as source_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where source_data.dbt_unique_key is null
    )

    select * from insertions
    union all
    select * from updates
    union all
    select * from deletes

        );"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "CREATE TABLE"
    assert qp.source_object == [
        "rd_scd",
        "snapshots",
        "scd_tmsdat__businessclasslevel1__snapshot__dbt_tmp",
    ]


def test_CREATE_TASK():
    """Test to parse a CREATE TASK query."""
    query = """CREATE TASK "LANDING_POWER_PLATFORM"."PUBLIC"."Clone POWER_PLATFORM from prod" WAREHOUSE = "LOADING_SAM_WH" SCHEDULE = '10080 MINUTE' COMMENT = 'This task clones the data from prod to dev, database for dev process.' AS create or replace database DEV_LANDING_POWER_PLATFORM clone LANDING_POWER_PLATFORM"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "CREATE TASK"
    assert qp.source_object == ["landing_power_platform", "public", "clone"]


def test_CREATE_USER():
    """Test to parse a CREATE USER query."""
    query = """CREATE USER "SYS_DBT_SPP" COMMENT='DBT System user created by Terraform' DEFAULT_ROLE='PUBLIC' LOGIN_NAME='SYS_DBT_SPP'"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "CREATE USER"
    assert qp.source_object == ["sys_dbt_spp"]


def test_CREATE_VIEW():
    """Test to parse a CREATE VIEW query."""
    query = """create or replace   view dev_rd_scd.rd_tmsdat.secids
  
   as (
    

with source as (

    select * from dev_landing_scd.TMSDAT.secids

),

renamed as (

    select
        recseqno,
        secik,
        identsysik,
        ident,
        data_loader_ts,
        data_loader_reload_time,
        data_loader_target_commit_ts
    from source

)

select * from renamed
  );"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "CREATE OR REPLACE"
    assert qp.source_object == ["dev_rd_scd", "rd_tmsdat", "secids"]


def test_DELETE():
    """Test to parse a DELETE query."""
    query = """DELETE  FROM "TMSDAT"."TRANSGAINLOSS" USING """
    qp = QueryParser(logger, query)
    assert qp.query_type == "DELETE"
    assert qp.source_object == ["tmsdat", "transgainloss"]


def test_DESCRIBE():
    """Test to parse a DESCRIBE query."""
    query = """describe table landing_trucost.trucost.sfdr_corporate_cpucu"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "DESCRIBE"
    assert qp.source_object == ["landing_trucost", "trucost", "sfdr_corporate_cpucu"]


def test_DROP():
    """Test to parse a DROP query."""
    query = """ DROP SCHEMA DDS_SAM.DBT_CLOUD_PR_819_553_SUSTAINALYTICS;"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "DROP SCHEMA"
    assert qp.source_object == ["dds_sam", "dbt_cloud_pr_819_553_sustainalytics"]


def test_DROP_CONSTRAINT():
    """Test to parse a DROP CONSTRAINT query."""
    query = """alter table SUSTAINALYTICS."ESG_SUST_PI_H" drop primary key;"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "ALTER TABLE"
    assert qp.source_object == ["sustainalytics", "esg_sust_pi_h"]


def test_DROP_MASKING_POLICY():
    """Test to parse a DROP MASKING POLICY query."""
    query = """drop masking policy rd_wolf.RD_ORDER.SAM_FUND_ADMIN_SENSITIVE_STRING;"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "DROP MASKING"
    assert qp.source_object == [
        "rd_wolf",
        "rd_order",
        "sam_fund_admin_sensitive_string",
    ]


def test_DROP_NETWORK_POLICY():
    """Test to parse a DROP NETWORK POLICY query."""
    query = """DROP NETWORK POLICY "QECQ_NX"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "DROP NETWORK"
    assert qp.source_object == ["qecq_nx"]


def test_DROP_ROLE():
    """Test to parse a DROP ROLE query."""
    query = """DROP ROLE "AR_DB_DDS_ANALYTICS_R"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "DROP ROLE"
    assert qp.source_object == ["ar_db_dds_analytics_r"]


def test_DROP_TASK():
    """Test to parse a DROP TASK query."""
    query = """DROP TASK "LANDING_SCD"."PUBLIC"."Clone from prod"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "DROP TASK"
    assert qp.source_object == ["landing_scd", "public", "clone"]


def test_DROP_USER():
    """Test to parse a DROP USER query."""
    query = """drop USER IDENTIFIER('"SYS_PREFECT_JANITOR_PLATFORM"')"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "DROP USER"
    assert qp.source_object == ["sys_prefect_janitor_platform"]


def test_EXECUTE_STREAMLIT():
    """Test to parse a EXECUTE STREAMLIT query."""
    query = """execute streamlit "DEV_DDS_SAM"."KU0_SCRATCHPADS"."G8G527BNVK_NJEM3"()"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "EXECUTE STREAMLIT"
    assert qp.source_object == ["dev_dds_sam", "ku0_scratchpads", "g8g527bnvk_njem3"]


def test_GET_FILES():
    """Test to parse a GET FILES query."""
    query = """GET '@worksheets_app.public.blobs/projects/783127510512160772/888184962c7c4a5f6cb7b1c3b433659d' 'file:///'"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "GET"
    assert qp.source_object == ['unparceble']


def test_GRANT():
    """Test to parse a GRANT query."""
    query = """GRANT select, insert, update, delete, truncate, references ON FUTURE views IN database dev_rd_nordic_trustee TO ROLE dev_ar_db_rd_nordic_trustee_w"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "GRANT"
    assert qp.source_object == ["dev_rd_nordic_trustee"]


def test_INSERT():
    """Test to parse an INSERT query."""
    query = """INSERT INTO "DATA_LOADER_STATUS"."attrep_history" ( "server_name","task_name","timeslot_type","timeslot","timeslot_duration","timeslot_latency","timeslot_records","timeslot_volume" )  SELECT 'p-dp-qlik1.common.storebrand.no','_Wolf_cashledger_to_DAMP','CHANGE PROCESSING','2023-12-27 22:56:17',30,1,0,0"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "INSERT"
    assert qp.source_object == ["data_loader_status", "attrep_history"]


def test_LIST_FILES():
    """Test to parse a LIST FILES query."""
    query = """list '@DEV_DDS_SPP.KWEDENBERG_POC."RC146MDX2H9D2XD8 (Stage)"/streamlit_app.py';"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "LIST"
    assert qp.source_object == [
        "dev_dds_spp",
        "kwedenberg_poc",
        "rc146mdx2h9d2xd8 (stage)",
        "streamlit_app.py",
    ]


def test_MERGE():
    """Test to parse a MERGE query."""
    query = """MERGE INTO "TMSDAT"."FXFORWARDS" T USING """
    qp = QueryParser(logger, query)
    assert qp.query_type == "MERGE"
    assert qp.source_object == ["tmsdat", "fxforwards"]


def test_MULTI_STATEMENT():
    """Test to parse a MULTI STATEMENT query."""
    query = """BEGIN TRANSACTION;
DROP TABLE IF EXISTS "SCREENER"."SCREENER LIST";
ALTER TABLE "SCREENER"."SCREENER LIST_AIRBYTE_TMP" RENAME TO "SCREENER"."SCREENER LIST";
COMMIT;
"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "BEGIN TRANSACTION"
    assert qp.source_object == ['unparceble']


def test_PUT_FILES():
    """Test to parse a PUT FILES query."""
    query = """PUT 'file:///11305ce8e2b425d90e1c61424eaf4a5f' '@worksheets_app.public.blobs/projects/1109056001759335584'"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "PUT"
    assert qp.source_object == ['unparceble']


def test_REMOVE_FILES():
    """Test to parse a REMOVE FILES query."""
    query = """REMOVE @"LANDING_WOLF"."PUBLIC"."ATTREP_IS_LANDING_WOLF_e1d8c1b1_0b28_264d_a1b4_53dc7af8d4e6"/e1d8c1b1_0b28_264d_a1b4_53dc7af8d4e6/0/CDC00000229.csv;"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "REMOVE"
    assert qp.source_object == [
        "landing_wolf",
        "public",
        "attrep_is_landing_wolf_e1d8c1b1_0b28_264d_a1b4_53dc7af8d4e6/e1d8c1b1_0b28_264d_a1b4_53dc7af8d4e6/0/cdc00000229",
        "csv",
    ]


def test_RENAME_COLUMN():
    """Test to parse a RENAME COLUMN query."""
    query = """ALTER TABLE HOLDINGS_PFC_EOD RENAME COLUMN LASTEXETS_TMP TO LASTEXETS"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "ALTER TABLE"
    assert qp.source_object == ["holdings_pfc_eod"]


def test_RENAME_SCHEMA():
    """Test to parse a RENAME SCHEMA query."""
    query = """alter SCHEMA IDENTIFIER('"SB_SAM_RISK_AND_OWNERSHIP"."FL_TEST"') rename to IDENTIFIER('"SB_SAM_RISK_AND_OWNERSHIP"."LEGACY_FL_TEST"')"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "ALTER SCHEMA"
    assert qp.source_object == ["sb_sam_risk_and_ownership", "fl_test"]


def test_RENAME_TABLE():
    """Test to parse a RENAME TABLE query."""
    query = """ALTER TABLE esg_scores RENAME TO esg_scores_v1;"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "ALTER TABLE"
    assert qp.source_object == ["esg_scores"]


def test_RENAME_VIEW():
    """Test to parse a RENAME VIEW query."""
    query = """alter VIEW IDENTIFIER('"SB_SAM_RISK_AND_OWNERSHIP"."FL_TEST"."OILGAS_SUS_L"') rename to IDENTIFIER('"SB_SAM_RISK_AND_OWNERSHIP"."FL_TEST"."OILGAS_SUS"')"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "ALTER VIEW"
    assert qp.source_object == ["sb_sam_risk_and_ownership", "fl_test", "oilgas_sus_l"]


def test_RESTORE():
    """Test to parse a RESTORE query."""
    query = """UNDROP TABLE INTERNAL_EXPORT_SAM.IE_ADS_STAGING.IE_ADS_ESG_PORTFOLIO_KPIS__SNAPSHOT_NEW_HW;"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "UNDROP"
    assert qp.source_object == [
        "internal_export_sam",
        "ie_ads_staging",
        "ie_ads_esg_portfolio_kpis__snapshot_new_hw",
    ]


def test_REVOKE():
    """Test to parse a REVOKE query."""
    query = """REVOKE select ON table dev_landing_scd.tmsdat.pmgfreecodes4 FROM ROLE ar_db_landing_scd_r"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "REVOKE"
    assert qp.source_object == ["dev_landing_scd", "tmsdat", "pmgfreecodes4"]


def test_ROLLBACK():
    """Test to parse a ROLLBACK query."""
    query = """ROLLBACK"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "ROLLBACK"
    assert qp.source_object == ['unparceble']


def test_SET():
    """Test to parse a SET query."""
    query = """set CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX=TRUE;"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "SET"
    assert qp.source_object == ["client_metadata_request_use_connection_ctx=true;"]


def test_LIST_FILES():
    """Test to parse a LIST FILES query."""
    query = """list '@DEV_DDS_SPP.KWEDENBERG_POC."RC146MDX2H9D2XD8 (Stage)"/streamlit_app.py';"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "LIST"
    assert qp.source_object == ["dev_dds_spp", "kwedenberg_poc", "rc146mdx2h9d2xd8"]


def test_SHOW():
    """Test to parse a SHOW query."""
    query = """show parameters like 'query_tag' in session"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "SHOW"
    assert qp.source_object == ['Not identified']


def test_TRUNCATE_TABLE():
    """Test to parse a TRUNCATE TABLE query."""
    query = """TRUNCATE TABLE "DATA_LOADER_STATUS"."attrep_changes1BAC39E90926EA78"""
    qp = QueryParser(logger, query)
    assert qp.query_type == "TRUNCATE TABLE"
    assert qp.source_object == ["data_loader_status", "attrep_changes1bac39e90926ea78"]


