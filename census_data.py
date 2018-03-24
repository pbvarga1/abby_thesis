from multiprocessing import Pool

import requests
import pandas as pd


def get_zip_code_df_by_year(year):
    """Get median income by zipcode in Arizona by year"""
    url = 'https://api.census.gov/data/{0}'

    years_urls = {
        2011: {'url': url.format('2011/acs5'), 'name': 'B19013_001{0}'},
        2012: {'url': url.format('2012/acs5'), 'name': 'B19013_001{0}'},
        2013: {'url': url.format('2013/acs5'), 'name': 'B19013_001{0}'},
        2014: {'url': url.format('2014/acs5'), 'name': 'B19013_001{0}'},
        2015: {'url': url.format('2015/acs5'), 'name': 'B19013_001{0}'},
        2016: {
            'url': url.format('2016/acs/acs5/profile'),
            'name': 'DP03_0062{0}'
        }
    }

    url = years_urls[year]['url']
    name = years_urls[year]['name']

    keys = {'Total': 'E', 'Margin': 'M'}
    lowest_zip_code, highest_zip_code = 85001, 86556
    NAME = 'NAME,{0}'.format(name)
    query_params = {'for': 'zip code tabulation area:*'}

    dfs = []
    for col, key in keys.items():
        query_params['get'] = NAME.format(key)
        results = requests.get(url, params=query_params).json()
        columns = ['Name', col, 'Zip']
        df = pd.DataFrame(results[1:], columns=columns)
        df = df[['Zip', col]]
        df['Zip'] = df['Zip'].astype(int)
        df = df.rename(
            index=str,
            columns={'Zip': 'Zip', col: '{0} {1}'.format(col, year)}
        )
        dfs.append(df)

    total = dfs[0]
    margin = dfs[1]
    median_income = total.merge(margin, on='Zip')
    az_zips = (
        (median_income['Zip'] >= lowest_zip_code) &
        (median_income['Zip'] <= highest_zip_code)
    )
    az_median_income = median_income[az_zips].reset_index(drop=True)
    az_median_income.set_index('Zip', inplace=True)
    az_median_income.sort_index(inplace=True)

    return az_median_income


if __name__ == '__main__':
    with Pool(processes=8) as pool:
        dfs = pool.map(get_zip_code_df_by_year, range(2011, 2017))
    df = pd.concat(dfs, axis=1)
    with open('median_income_by_zip_code.csv', 'w') as stream:
        df.to_csv(stream, index=True)
