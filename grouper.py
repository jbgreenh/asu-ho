from pathlib import Path
import polars as pl

years = range(2018, 2026)
months = range(1,13)
for year in years:
    for month in months:
        print(f'{year = } - {month = }')
        month_fn = f'combined/{year}-{month}.csv'
        month_df = pl.DataFrame()

        slices = Path('geo_files/').glob(f'{year}-{month}-*.csv')
        for slice in slices:
            print(f'{slice}')
            slice_df = pl.read_csv(slice, infer_schema=False)
            month_df = pl.concat([month_df, slice_df])
        month_df.write_csv(month_fn)


