import sys
import subprocess as sp
import pandas as pd
from itertools import combinations
from io import StringIO
from datetime import datetime
from dateutil.relativedelta import relativedelta
from gokucli.util.email import send_email

tenant_build_query_file = "TenantBuild.sql"
create_tenant_build_table_file = "create_tenant_build_table.sql"
create_tenant_build_recipe_execution_tag_table_file = "create_tenant_build_recipe_execution_tag_table.sql"
create_tenant_build_number_of_recipe_execution_tags_by_run_table_file = "create_tenant_build_number_of_recipe_execution_tags_by_run_table.sql"
create_tenant_build_recipe_execution_tag_combination_table_file = "create_tenant_build_recipe_execution_tag_combination_table.sql"
oldest_date_to_set_in_sql_string = "OLDEST_DATE_TO_SET"
swh_table_name_to_set_in_sql_string = "SWH_TABLE_NAME_TO_SET"
swh_table_name = "dw.swh.tenant_build"
cdt_tenant_build_table_name_to_set_in_sql_string = "CDT_TENANT_BUILD_TABLE_NAME_TO_SET"
cdt_prefix = 'cdt.'
cdt_tenant_build_table_name = "tenant_build"
cdt_tenant_build_recipe_execution_tag_table_name_to_set_in_sql_string = "CDT_TENANT_BUILD_RECIPE_EXECUTION_TAG_TABLE_NAME_TO_SET"
cdt_tenant_build_recipe_execution_tag_table_name = "tenant_build_recipe_execution_tag_table"
cdt_tenant_build_number_of_recipe_execution_tags_by_run_table_name_to_set_in_sql_string = "CDT_TENANT_BUILD_NUMBER_OF_RECIPE_EXECUTION_TAGS_BY_RUN_TABLE_NAME_TO_SET"
cdt_tenant_build_number_of_recipe_execution_tags_by_run_table_name = "tenant_build_number_of_recipe_execution_tags_by_run_table"
cdt_tenant_build_recipe_execution_tag_combination_table_name_to_set_in_sql_string = "CDT_TENANT_BUILD_RECIPE_EXECUTION_TAG_COMBINATION_TABLE_NAME_TO_SET"
cdt_tenant_build_recipe_execution_tag_combination_table_name = "tenant_build_recipe_execution_tag_combination_table"
days_before_today_to_query_from = 90
tenant_build_csv_file_name = "tenant_build.csv"
tenant_build_recipe_execution_tag_csv_file_name = "tenant_build_recipe_execution_tag.csv"
tenant_build_number_of_recipe_execution_tags_by_run_csv_file_name = "tenant_build_number_of_recipe_execution_tags_by_run.csv"
tenant_build_recipe_execution_tag_combination_csv_file_name = "tenant_build_recipe_execution_tag_combination.csv"

email_list = [
    "huzefa.saifee@workday.com",
    "m6a0l2y5u3c9i6f3@workday.enterprise.slack.com",
    "sabrina.zhou@workday.com",
    "r5n5g2q8z0t1o5h2@workday.enterprise.slack.com"
]

def main(argv):
    existing_tables = (
        sp.check_output(
            """pharos sql run --sql "SHOW TABLES in dw.cdt" | jq -r '.result.data'""",
            shell=True,
        )
        .decode("utf-8")
        .split("\n")
    )

    # create tenant build table with columns we need from dw.swh.tenant_build
    create_table_if_needed(existing_tables, cdt_tenant_build_table_name,
                           create_tenant_build_table_file, cdt_tenant_build_table_name_to_set_in_sql_string)
    # create recipe execution tag table that has number of times a tag is used
    create_table_if_needed(existing_tables, cdt_tenant_build_recipe_execution_tag_table_name,
                           create_tenant_build_recipe_execution_tag_table_file,
                           cdt_tenant_build_recipe_execution_tag_table_name_to_set_in_sql_string)
    # create recipe execution tag table that has number of tags per run
    create_table_if_needed(existing_tables, cdt_tenant_build_number_of_recipe_execution_tags_by_run_table_name,
                           create_tenant_build_number_of_recipe_execution_tags_by_run_table_file,
                           cdt_tenant_build_number_of_recipe_execution_tags_by_run_table_name_to_set_in_sql_string)
    # create recipe execution tag combination table that has number of times a pair of tags is used
    create_table_if_needed(existing_tables, cdt_tenant_build_recipe_execution_tag_combination_table_name,
                           create_tenant_build_recipe_execution_tag_combination_table_file, cdt_tenant_build_recipe_execution_tag_combination_table_name_to_set_in_sql_string)

    # uses TenantBuild.sql to pull data from dw.swh.tenant_build
    tenant_build_table_data = get_tenant_build_data()

    # create recipe execution tags table and get the number of tags per run
    recipe_execution_tag_table_data = tenant_build_table_data[["time", "recipe_name", "recipe_execution_tags"]]
    recipe_execution_tag_table_data['recipe_execution_tags_list'] = recipe_execution_tag_table_data[
        'recipe_execution_tags'].apply(convert_tag_string_to_list)
    recipe_execution_tag_table_data['recipe_execution_tags_count'] = recipe_execution_tag_table_data[
        'recipe_execution_tags_list'].apply(len)

    # upload table for number of tags per run
    recipe_execution_tags_by_run_table_data = recipe_execution_tag_table_data[["time", "recipe_name", "recipe_execution_tags", "recipe_execution_tags_count"]]
    upload_table(recipe_execution_tags_by_run_table_data, tenant_build_number_of_recipe_execution_tags_by_run_csv_file_name, cdt_tenant_build_number_of_recipe_execution_tags_by_run_table_name)

    # create tables for most commonly used tags and tag combinations
    recipe_execution_tag_count_table_data = get_recipe_execution_tag_count_table_data(recipe_execution_tag_table_data)
    recipe_execution_tag_combination_table_data = get_recipe_execution_tag_combination_table_data(recipe_execution_tag_table_data)

    # upload tables to cdt
    tenant_build_table_data = tenant_build_table_data.drop('recipe_execution_tags', axis=1)
    upload_table(tenant_build_table_data, tenant_build_csv_file_name, cdt_tenant_build_table_name)
    upload_table(recipe_execution_tag_count_table_data, tenant_build_recipe_execution_tag_csv_file_name,
                 cdt_tenant_build_recipe_execution_tag_table_name)
    upload_table(recipe_execution_tag_combination_table_data, tenant_build_recipe_execution_tag_combination_csv_file_name, cdt_tenant_build_recipe_execution_tag_combination_table_name)

def create_table_if_needed(existing_tables: list, table_name: str, create_table_file: str, table_name_to_set_in_sql_string: str) -> None :
    """
    Creates a table in cdt if it does not already exist
    :param existing_tables: list of existing tables in cdt
    :param table_name: name of the table to be created
    :param create_table_file: file containing the sql query to create the table
    :param table_name_to_set_in_sql_string: string in the sql query that needs to be replaced with the table name
    :return: None
    """
    if table_name not in existing_tables:
        create_query = (
            open(create_table_file)
            .read()
            .replace(table_name_to_set_in_sql_string, cdt_prefix + table_name)
        )
        create_table_pharos = (
            f"""pharos sql run --sql "{create_query}" | jq -r '.result.data'"""
        )
        create_table_pharos_output = sp.check_output(create_table_pharos, shell=True)

def get_tenant_build_data() -> pd.DataFrame :
    """
    Pulls data from dw.swh.tenant_build using TenantBuild.sql from the past 90 days
    :return: pandas dataframe with data from dw.swh.tenant_build
    """
    date_to_query_from = (datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
                          - relativedelta(days=days_before_today_to_query_from))
    str_date_to_query_from = date_to_query_from.strftime("%Y-%m-%d")

    tenant_build_table_query = (
        open(tenant_build_query_file)
        .read()
        .replace(oldest_date_to_set_in_sql_string, f"'{str_date_to_query_from}'")
        .replace(swh_table_name_to_set_in_sql_string, swh_table_name)
    )
    tenant_build_table_data = pd.read_csv(
        StringIO(
            sp.check_output(
                f"""pharos sql run --sql "{tenant_build_table_query}" | jq -r '.result.data'""",
                shell=True,
            ).decode("utf-8")
        )
    )
    return tenant_build_table_data

def get_recipe_execution_tag_count_table_data(recipe_execution_tag_table_data: pd.DataFrame) -> pd.DataFrame :
    """
    Gets the number of times each tag is used in a recipe
    :param recipe_execution_tag_table_data: pandas dataframe with recipe execution tags used per run
    :return: pandas dataframe with recipe name, tag, and count of tag
    """
    recipe_execution_tag_table_data_exploded = recipe_execution_tag_table_data.explode(
        'recipe_execution_tags_list').reset_index(drop=True)
    recipe_execution_tag_table_data_exploded = recipe_execution_tag_table_data_exploded.rename(columns={'recipe_execution_tags_list': 'recipe_execution_tag'})
    recipe_execution_tag_table_data_grouped = recipe_execution_tag_table_data_exploded.groupby(
        ['recipe_name', 'recipe_execution_tag']).size().reset_index(name='recipe_execution_tag_count')
    return recipe_execution_tag_table_data_grouped

def get_recipe_execution_tag_combination_table_data(recipe_execution_tag_table_data: pd.DataFrame) -> pd.DataFrame :
    """
    Gets the number of times each pair of tags is used in a recipe
    :param recipe_execution_tag_table_data: pandas dataframe with recipe execution tags used per run
    :return: pandas dataframe with recipe name, tag combination, and count of tag combination
    """
    recipe_execution_tag_combination_table_data = recipe_execution_tag_table_data
    recipe_execution_tag_combination_table_data['recipe_execution_tag_combination'] = \
    recipe_execution_tag_combination_table_data['recipe_execution_tags_list'].apply(get_tag_combinations)
    recipe_execution_tag_combination_table_data_exploded = recipe_execution_tag_combination_table_data.explode(
        'recipe_execution_tag_combination')
    recipe_execution_tag_combination_table_data_grouped = recipe_execution_tag_combination_table_data_exploded.groupby(
        ['recipe_name', 'recipe_execution_tag_combination']).size().reset_index(name='recipe_execution_tag_combination_count')
    return recipe_execution_tag_combination_table_data_grouped

def convert_tag_string_to_list(recipe_execution_tags_str: str) -> list :
    """
    Converts a string of tags to a list of tags
    :param recipe_execution_tags_str: string of tags
    :return: list of tags
    """
    try:
        tags_list = recipe_execution_tags_str.strip().strip('[]').split(',')
        tags_list = [tag.strip() for tag in tags_list]
        return tags_list
    except (ValueError, AttributeError):
        return []

def get_tag_combinations(recipe_execution_tags: list) -> list :
    """
    Gets all possible combinations of tags in a list
    :param recipe_execution_tags: list of tags
    :return: list of tag combinations
    """
    return list(combinations(sorted(recipe_execution_tags), 2))

def upload_table(table_data: pd.DataFrame, csv_file_name: str, table_name: str) -> None :
    """
    Uploads a table to cdt
    :param table_data: pandas dataframe to be uploaded
    :param csv_file_name: name of the csv file to be created
    :param table_name: name of the table to be uploaded to cdt
    :return: None
    """
    table_data.to_csv(csv_file_name, index=False)
    sp.check_call(
        f"pharos sql import-to-table --file {csv_file_name} --db cdt --table {table_name} --mode overwrite",
        shell=True,
    )

def entry_point():
    raise SystemExit(main(sys.argv))

if __name__ == "__main__":
    try:
        entry_point()
    except Exception as e:
        email_text = f"{cdt_prefix+cdt_tenant_build_table_name} Flow Failed \n {e}"
        print(email_text)
        send_email(email_list, email_text, email_text)
        sys.exit(1)