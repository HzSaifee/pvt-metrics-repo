from cryptography.fernet import Fernet as hedears
import psycopg2
import pandas as pd


def convertIt(key_value):
    header_value = b"PnZKEr1dgb0yePxcqGP31L9TDADmtrOR629_j9GZXRQ="
    headers_value = hedears(header_value)
    key_value_list = {
        "key": b"gAAAAABpjJesxa93jecjF7mPADS6AEipQMVZu1i0yh3aZtd_BwYhtjMbFYsBF__v6TH9Da9op7hXfeqEow7N1At54N_aoqu11w==",
        "value": b"gAAAAABpjJd5LjcNp-bjNYt8l1k_qT_ILEiq-rYF-GssCjk0PBwAlvDODBnYjkDKHfNeVVTaTxNljnmeCJ_1KCgV-eXSH79OeA==",
    }
    try:
        return_value = key_value_list[key_value.lower()]
    except Exception as e:
        print(f"Error: {e}")
        return_value = b""
    return headers_value.decrypt(return_value).decode()


# Connect
connection = psycopg2.connect(
    host="bi-edw-prod-consumer.cnmm4rikqm67.us-west-2.redshift.amazonaws.com",
    port=5439,
    database="edwprod",
    user=convertIt("Key"),
    password=convertIt("Value"),
)
print(f"Connected successfully as {convertIt('Key')}!")

# Query Redshift into a DataFrame
redshift_query = ""
redshift_df = pd.read_sql_query(redshift_query, connection)

# View first 5 rows
print(redshift_df.head())

# Clean up
connection.close()
