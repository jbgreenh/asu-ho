from datetime import date, timedelta
import time

import censusgeocode as cg
import polars as pl

import tableau

start_time = time.perf_counter()
today = date.today()

asu_ho_luid = tableau.find_view_luid('disp', 'asu health observatory')

for year in range(2025, today.year+1):
    for month in range(1,13):
        if (year == 2025) and (month < 5):
            continue
        start_date = date(year=year, month=month, day=1)
        end_date = (start_date + timedelta(days=32)).replace(day=1) - timedelta(days=1)

        filters = {'start_date':start_date, 'end_date':end_date}
        print(f'pulling {year}-{month} data from tableau...')
        tab_data = tableau.lazyframe_from_view_id(asu_ho_luid, filters=filters, infer_schema=False)
        if tab_data is None:
            print(f'no data found for {year}-{month}')
            continue
        disp_fn = f'disp_data/dd{year}-{month}.csv'
        tab_data.collect().write_csv(disp_fn)
        print(f'wrote {disp_fn}')

        disp_data = (
            pl.read_csv(disp_fn, infer_schema=False)
            .with_columns(
                pl.col(['Day of Filled At', 'Day of Patient Birthdate']).str.to_date('%B %d, %Y'),
                pl.col(['Orig Patient Address Line One', 'Orig Patient City', 'Orig Patient State Abbr', 'Orig Patient Zip']).str.replace_all('\\', '', literal=True)
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
            .drop('age', 'Day of Patient Birthdate')
            .with_row_index()
        )

        pat_addr_df = disp_data.select('index', 'Orig Patient Address Line One', 'Orig Patient City', 'Orig Patient State Abbr', 'Orig Patient Zip')
        for idx, frame in enumerate(pat_addr_df.iter_slices()):
            slice_fn = f'disp_slices/{year}-{month}-{idx}_slice.csv'
            frame.write_csv(slice_fn, include_header=False)
            print(f'wrote {slice_fn}')

            check_height = 1
            while check_height == 1:
                print(f'geocoding {slice_fn}...')
                pat_response_df = pl.DataFrame(cg.addressbatch(slice_fn), infer_schema_length=None)
                check_height = pat_response_df.filter(pl.col('id').str.contains('While')).height
                if pat_response_df.filter(pl.col('id').str.contains('<html>')).height > 0:
                    check_height = 1
                if check_height == 1:
                    print('bad census response')
                    print(pat_response_df)
                    print('reattempting geocode...')

            pat_response_df = pat_response_df.with_columns(pl.col('id').cast(pl.UInt32))
            print(f'geocoded {slice_fn}')
            linked = (
                disp_data
                .join(pat_response_df
                    .select(
                        pl.col('id'),
                        pl.col('block').alias('pat_census_block')
                ), left_on='index', right_on='id', how='left', coalesce=True)
                .drop('index', 'Orig Patient Address Line One', 'Orig Patient City', 'Orig Patient State Abbr', 'Orig Patient Zip', 'Orig Pharmacy Address Line One', 'Orig Pharmacy City', 'Orig Pharmacy State Abbr', 'Orig Pharmacy Zip' )
            )

            geo_fn = f'geo_files/{year}-{month}-{idx}_geo.csv'
            linked.write_csv(geo_fn)
            print(f'wrote {geo_fn}')
            check_time = time.perf_counter()
            print(f'script has been running for: {check_time - start_time}s...')

end_time = time.perf_counter()
print(f'took {end_time - start_time}s')
