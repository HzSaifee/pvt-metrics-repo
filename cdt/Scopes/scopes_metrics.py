from goku.util.context import GokuContext
from gokucli.util.email import send_email
import sys
from datetime import datetime
from dateutil.relativedelta import relativedelta
import subprocess as sp
from io import StringIO
import pandas as pd

month_value = "OLDEST_MONTH_VALUE_TO_SET"
str_month_to_query_from = (
    datetime.today().replace(day=1) - relativedelta(months=24)
).strftime("%Y-%m-%d")
email_list = [
    "huzefa.saifee@workday.com",
    "m6a0l2y5u3c9i6f3@workday.enterprise.slack.com",
]
email_subject = "Pharos Query for Scopes Failed"


def fetch_data(file_name):
    try:
        swh_query = (
            open(f"{file_name}.sql")
            .read()
            .replace(month_value, f"'{str_month_to_query_from}'")
        )
        data = pd.read_csv(
            StringIO(
                sp.check_output(
                    f"""pharos sql run --sql "{swh_query}" | jq -r '.result.data'""",
                    shell=True,
                ).decode("utf-8")
            )
        )
    except:
        email_body = f"{email_subject} on {file_name}.sql"
        print(email_body)
        # send_email(email_list, email_body, main_table_name)
        sys.exit()
    return data


def main(argv):
    data_list = []
    sql_files = [
        "metrics",
        "input_type_metrics",
        "selection_type_metrics",
        "validation_usages_metrics",
        "materialization_metrics",
    ]

    for sql_file in sql_files:
        data_list.append(dict({"name": sql_file, "df": fetch_data(sql_file)}))

    with GokuContext(argv) as ctx:
        for dt in data_list:
            ctx.write_report(
                dt['df'], filename=f"{dt['name']}.csv", index=False, encoding="utf-8"
            )


def entry_point():
    raise SystemExit(main(sys.argv))


if __name__ == "__main__":
    entry_point()
