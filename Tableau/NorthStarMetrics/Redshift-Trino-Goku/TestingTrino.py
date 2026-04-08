import subprocess as sp
from io import StringIO
import pandas as pd

# Query Trino into DataFrame
trino_query = open(
    "/Users/huzefa.saifee/OtherRepos/pvt-metrics-repo/cdt/NorthStarMetrics/TableauDashboardData/CT-TC/activity_datasource.sql"
).read()
trino_df = pd.read_csv(
    StringIO(
        sp.check_output(
            f"""pharos sql run --sql "{trino_query}" | jq -r '.result.data'""",
            shell=True,
        ).decode("utf-8")
    )
)

# View first 5 rows
print(trino_df.head())
