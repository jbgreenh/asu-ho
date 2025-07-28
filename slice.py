# from datetime import date

import censusgeocode as cg
import polars as pl

year = 2018
month = 3
idx = 14

# today = date.today()

# disp_fn = f'disp_data/dd{year}-{month}.csv'
# disp_data = (
#     pl.read_csv(disp_fn, infer_schema=False)
#     .with_columns(
#         pl.col(['Day of Filled At', 'Day of Patient Birthdate']).str.to_date('%B %d, %Y')
#     )
#     .with_columns(
#         (
#             today.year - pl.col('Day of Patient Birthdate').dt.year() -
#             (pl.date(pl.col('Day of Patient Birthdate').dt.year(), today.month, today.day) < pl.col('Day of Patient Birthdate'))
#         )
#         .alias('age')
#     )
#     .with_columns(
#         pl.col('age').cut([17, 34, 44, 64], labels=['<18', '18-34', '35-44', '45-64', '65+']).alias('age_band')
#     )
#     .drop('age', 'Day of Patient Birthdate')
#     .with_row_index()
# )

slice_fn = f'disp_slices/{year}-{month}-{idx}_slice.csv'
slice_df = pl.read_csv(slice_fn, infer_schema=False, has_header=False, new_columns=['id', 'street', 'city', 'state', 'zip']).with_columns(pl.col('id').cast(pl.Int32))
n = 10_000
letters = {'x':False, 'y':False}

while slice_df.height > 1:
    print(f'{n = }')
    x_n = n // 2
    print(f'{x_n = }')

    x_df = slice_df.head(x_n)
    x_df.write_csv(f'disp_slices/{year}-{month}-{idx}_x_slice.csv')

    y_df = slice_df.slice(x_n)
    y_df.write_csv(f'disp_slices/{year}-{month}-{idx}_y_slice.csv')

    for letter in letters:
        slice_fn = f'disp_slices/{year}-{month}-{idx}_{letter}_slice.csv'

        print(f'geocoding {slice_fn}...')
        pat_response_df = pl.DataFrame(cg.addressbatch(slice_fn), infer_schema_length=None)
        check_height = pat_response_df.height
        if pat_response_df.filter(pl.col('id').str.contains('While')).height == 1:
            print(f'{letter} bad census response')
            letters[letter] = True
        else:
            print(f'{letter} good census response')

    if letters['x'] ^ letters['y']:
        if letters['x']:
            slice_df = x_df
        else:
            slice_df = y_df
        n = slice_df.height
        letters['x'] = False
        letters['y'] = False
    else:
        print('both x and y bad')
print(slice_df)
slice_df.write_csv('bad_record.csv')



# pat_response_df = pat_response_df.with_columns(pl.col('id').cast(pl.UInt32))
# print(f'geocoded {slice_fn}')
# linked = (
#     disp_data
#     .join(pat_response_df
#         .select(
#             pl.col('id'),
#             pl.col('block').alias('pat_census_block')
#     ), left_on='index', right_on='id', how='left', coalesce=True)
#     .drop('index', 'Orig Patient Address Line One', 'Orig Patient City', 'Orig Patient State Abbr', 'Orig Patient Zip', 'Orig Pharmacy Address Line One', 'Orig Pharmacy City', 'Orig Pharmacy State Abbr', 'Orig Pharmacy Zip' )
# )
#
# geo_fn = f'geo_files/{year}-{month}-{idx}_geo.csv'
# linked.write_csv(geo_fn)
# print(f'wrote {geo_fn}')
