# write to full_files folder as needed from pmp-analytics xx_.py file
from io import BytesIO
from dotenv import load_dotenv
import os
from pathlib import Path
import polars as pl
import paramiko

ifp = Path('full_files/2025-6.csv')
df = (
    pl.read_csv(ifp, infer_schema=False)
    .filter(
        pl.col('pat_census_block').is_not_null()
    )
    .with_columns(
        pl.col('Quantity').str.replace_all(r',', '').str.to_decimal()
    )
    .with_columns(
        pl.when(pl.col('Quantity') < 10).then(pl.lit('<10')).otherwise(pl.col('Quantity')).alias('cleaned_q')
    )
    .drop('Quantity')
    .rename({'cleaned_q':'Quantity'})
)
print(df.glimpse())

load_dotenv()
sftp_host = os.environ['SFTP_HOST']
sftp_port = os.environ['SFTP_PORT']
sftp_user = os.environ['SFTP_USERNAME']
sftp_password = os.environ['SFTP_PASSWORD']

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(hostname=sftp_host, port=int(sftp_port), username=sftp_user, password=sftp_password, look_for_keys=False)
sftp = ssh.open_sftp()

sftp.chdir(os.environ['SFTP_REMOTE_PATH'])

csv_buffer = BytesIO()
df.write_csv(csv_buffer)
csv_buffer.seek(0)
print('writing sample to sftp...')
sftp.putfo(csv_buffer, remotepath='sample_2025-6.csv')
files = sftp.listdir()
print(files)

sftp.close()
ssh.close()

# no_drug_name = (
#     df
#     .group_by(
#         'Day of Filled At',
#         'Patient Gender',
#         'Quantity',
#         'age_band',
#         'pat_census_block',
#         'lte10?',
#     )
#     .sum()
# )
# print('\nno drug name:')
# print(no_drug_name['lte10?'].value_counts)




