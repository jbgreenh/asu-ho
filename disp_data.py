from datetime import date
import tempfile

import censusgeocode as cg
import polars as pl

today = date.today()

df = (
    pl.read_csv('disp_data.csv')
    .with_columns(
        # (pl.col('Orig Patient Address Line One') + ', ' + pl.col('Orig Patient City') + ', ' + pl.col('Orig Patient State Abbr')).alias('pat_addr'),
        # (pl.col('Orig Pharmacy Address Line One') + ', ' + pl.col('Orig Pharmacy City') + ', ' + pl.col('Orig Pharmacy State Abbr')).alias('pharm_addr'),
        pl.col(['Day of Filled At', 'Day of Patient Birthdate']).str.to_date('%B %d, %Y')
    )
    .with_columns(
        (
            today.year - pl.col('Day of Patient Birthdate').dt.year() -
            (pl.date(pl.col('Day of Patient Birthdate').dt.year(), today.month, today.day) < pl.col('Day of Patient Birthdate'))
        )
        .alias('age')
    )
    .with_columns(
        pl.col('age').cut([17, 34, 44, 64], labels=['<18', '18-34', '35-44', '45-64', '65+']).alias('age_band')
    )
    # .drop('age', 'Day of Patient Birthdate', 'Orig Patient Address Line One', 'Orig Patient City', 'Orig Patient State Abbr', 'Orig Pharmacy Address Line One', 'Orig Pharmacy City', 'Orig Pharmacy State Abbr')
    .drop('age', 'Day of Patient Birthdate')
    .with_row_index()
)

pat_addr_df = df.select('index', 'Orig Patient Address Line One', 'Orig Patient City', 'Orig Patient State Abbr', 'Orig Patient Zip')
pharm_addr_df = df.select('index', 'Orig Pharmacy Address Line One', 'Orig Pharmacy City', 'Orig Pharmacy State Abbr', 'Orig Pharmacy Zip')
with tempfile.NamedTemporaryFile(mode='w+', suffix='.csv') as tmp:
    pat_addr_df.write_csv(tmp.name, include_header=False)
    pat_response_df = pl.DataFrame(cg.addressbatch(tmp.name))
with tempfile.NamedTemporaryFile(mode='w+', suffix='.csv') as tmp:
    pharm_addr_df.write_csv(tmp.name, include_header=False)
    pharm_response_df = pl.DataFrame(cg.addressbatch(tmp.name))

linked = (
    df
    .join(pat_response_df.select(
        pl.col('id').cast(pl.Int32),
        pl.col('block')
        .alias('pat_census_block')), left_on='index', right_on='id', how='left', coalesce=True)
    .join(pharm_response_df.select(
        pl.col('id').cast(pl.Int32),
        pl.col('block')
        .alias('pharm_census_block')), left_on='index', right_on='id', how='left', coalesce=True)
    .drop('index', 'Orig Patient Address Line One', 'Orig Patient City', 'Orig Patient State Abbr', 'Orig Patient Zip', 'Orig Pharmacy Address Line One', 'Orig Pharmacy City', 'Orig Pharmacy State Abbr', 'Orig Pharmacy Zip' )
    .group_by((pl.col('Day of Filled At').dt.month().cast(pl.String) + '-' + pl.col('Day of Filled At').dt.year().cast(pl.String)).alias('filled_m_y'), 'pat_census_block', 'Patient Gender')
    .len()
)
linked.write_csv('linked.csv')

df.write_csv('age_bands.csv')
