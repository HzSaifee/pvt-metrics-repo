import sys
import subprocess as sp
import pandas as pd
from io import StringIO
from datetime import datetime
from dateutil.relativedelta import relativedelta
from gokucli.util.email import send_email

sql_query_file = "TenantBuild.sql"
oldest_date_to_set_in_sql_string = "OLDEST_DATE_TO_SET"
main_table_name_to_set_in_sql_string = "MAIN_TABLE_NAME_TO_SET"
swh_table_name = "dw.swh.tenant_build"
days_before_today_to_query_from = 90
csv_file_name = "tenant_build.csv"
cdt_table_name = "tenant_build"
email_list = [
    huzefa.saifee @ workday.com,
    m6a0l2y5u3c9i6f3 @ workday.enterprise.slack.com,
    sabrina.zhou @ workday.com,
    r5n5g2q8z0t1o5h2 @ workday.enterprise.slack.com,
]


def main(argv):
    date_to_query_from = datetime.today().replace(
        hour=0, minute=0, second=0, microsecond=0
    ) - relativedelta(days=days_before_today_to_query_from)
    str_date_to_query_from = date_to_query_from.strftime("%Y-%m-%d")

    table_query = (
        open(sql_query_file)
        .read()
        .replace(oldest_date_to_set_in_sql_string, f"'{str_date_to_query_from}'")
        .replace(main_table_name_to_set_in_sql_string, swh_table_name)
    )

    table_data = sp.check_output(
        f"""pharos sql run --sql "{table_query}" | jq -r '.result.data'""",
        shell=True,
    ).decode("utf-8")
    table_data = StringIO(table_data)
    table_data = pd.read_csv(table_data)
    print(table_data)

    table_data.to_csv(csv_file_name, index=False)
    sp.check_call(
        f"pharos sql import-to-table --file {csv_file_name} --db cdt --table {cdt_table_name} --mode overwrite",
        shell=True,
    )


def entry_point():
    raise SystemExit(main(sys.argv))


if __name__ == "__main__":
    try:
        entry_point()
    except Exception as e:
        email_text = f"{cdt_table_name} Flow Failed \n {e}"
        print(email_text)
        send_email(email_list, email_text, email_text)
        sys.exit(1)
