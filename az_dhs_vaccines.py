import re
import json
import subprocess
import itertools
from multiprocessing import Pool

import urllib
import pandas as pd
from bs4 import BeautifulSoup


def get_schools(county, year, grade):
    """Get all the schools in a county for a year and grade"""
    url = "https://app.azdhs.gov/IDRReportStats/Home/GetSchoolTable?{0}"
    query = {
        'bRegex': 'false',
        'bRegex_0': 'false',
        'bRegex_1': 'false',
        'bRegex_2': 'false',
        'bRegex_3': 'false',
        'bRegex_4': 'false',
        'bRegex_5': 'false',
        'bRegex_6': 'false',
        'bRegex_7': 'false',
        'bRegex_8': 'false',
        'bSearchable_0': 'false',
        'bSearchable_1': 'true',
        'bSearchable_2': 'false',
        'bSearchable_3': 'false',
        'bSearchable_4': 'false',
        'bSearchable_5': 'false',
        'bSearchable_6': 'true',
        'bSearchable_7': 'true',
        'bSearchable_8': 'false',
        'iColumns': '9',
        'iDisplayLength': '2000',
        'iDisplayStart': '0',
        'mDataProp_0': 'SCHOOL_YEAR',
        'mDataProp_1': 'SCHOOL_NAME',
        'mDataProp_2': 'SCHOOL_TYPE',
        'mDataProp_3': 'SCHOOL_GRADE',
        'mDataProp_4': 'ENROLLED',
        'mDataProp_5': 'ADDRESS',
        'mDataProp_6': 'CITY',
        'mDataProp_7': 'ZIP',
        'mDataProp_8': 'COUNTY',
        'sColumns': ',,,,,,,,',
        'sEcho': '1',
        'selectedCounty': county,
        'selectedGrade': grade,
        'selectedYear': year,
    }
    command = ['curl', url.format(urllib.parse.urlencode(query))]
    with subprocess.Popen(command, stdout=subprocess.PIPE) as proc:
            schools = json.loads(proc.communicate()[0].decode())['aaData']

    return schools


def get_data_from_table(table):
    """Put the html table into a dictionary"""
    soup = BeautifulSoup(table, 'html5lib')
    data = {
        'school type': {
            'SCHOOL_TYPE': 'N/A'
        },
        'enrolled': {
            'ENROLLED': 'N/A'
        },
        'medical': {
            'PCT_MEDICAL_EXEMPT': 'N/A'
        },
        'personal': {
            'PCT_PBE': 'N/A'
        },
        'every': {
            'PCT_PBE_EXEMPT_ALL': 'N/A'
        },
        'does': {
            'HAS_NURSE': 'N/A'
        },
        'nurse type': {
            'NURSE_TYPE': ''
        },
        'dtap': {
            'PCT_IMMUNE_DTAP': 'N/A',
            'PCT_EXEMPT_DTAP': 'N/A',
            'PCT_COMPLIANCE_DTAP': 'N/A'
        },
        'tdap': {
            'PCT_IMMUNE_TDAP': 'N/A',
            'PCT_EXEMPT_TDAP': 'N/A',
            'PCT_COMPLIANCE_TDAP': 'N/A'
        },
        'mcv': {
            'PCT_IMMUNE_MVMVC': 'N/A',
            'PCT_EXEMPT_MVMVC': 'N/A',
            'PCT_COMPLIANCE_MVMVC': 'N/A'
        },
        'polio': {
            'PCT_IMMUNE_POLIO': 'N/A',
            'PCT_EXEMPT_POLIO': 'N/A',
            'PCT_COMPLIANCE_POLIO': 'N/A'
        },
        'mmr': {
            'PCT_IMMUNE_MMR': 'N/A',
            'PCT_EXEMPT_MMR': 'N/A',
            'PCT_COMPLIANCE_MMR': 'N/A'
        },
        'hep b': {
            'PCT_IMMUNE_HEPB': 'N/A',
            'PCT_EXEMPT_HEPB': 'N/A',
            'PCT_COMPLIANCE_HEPB': 'N/A'
        },
        'hep a': {
            'PCT_IMMUNE_HEPA': 'N/A',
            'PCT_EXEMPT_HEPA': 'N/A',
            'PCT_COMPLIANCE_HEPA': 'N/A'
        },
        'hib': {
            'PCT_IMMUNE_HIB': 'N/A',
            'PCT_EXEMPT_HIB': 'N/A',
            'PCT_COMPLIANCE_HIB': 'N/A'
        },
        'var': {
            'PCT_IMMUNE_VAR': 'N/A',
            'PCT_EXEMPT_VAR': 'N/A',
            'PCT_COMPLIANCE_VAR': 'N/A'
        },
    }
    for row in soup.find_all('div', {'class': 'row'}):
        key = None
        children = list(row.children)
        if len(children) <= 1:
            continue
        key = children[1].text.lower()
        for k in data.keys():
            if re.search(k, key):
                break
        else:
            continue
        cols = data[k]
        col_names = list(cols.keys())
        index = 0
        for child in children[2:]:
            try:
                text = child.text.lower()
            except Exception:
                continue
            cols[col_names[index]] = text
            index += 1
            if index == len(col_names):
                break
        data[k] = cols
    return data


def get_school_data(school_name, address, grade, year, county, zipcode, city):
    """Get data for a school"""
    params = {
        'paramSelectedAddress': address,
        'paramSelectedCity': city,
        'paramSelectedGrade': grade,
        'paramSelectedSchool': school_name,
        'paramSelectedYear': year,
    }
    cmnd = [
        'curl',
        '-d',
        "{0}".format(urllib.parse.urlencode(params)),
        "https://app.azdhs.gov/IDRReportStats/Home/GetSchoolSpecifications",
    ]
    with subprocess.Popen(cmnd, stdout=subprocess.PIPE) as proc:
        table = proc.communicate()[0].decode()
    try:
        data = {
            'School': str(school_name),
            'Grade': str(grade),
            'Address': str(address),
            'School Year': str(year),
            'Zipcode': str(zipcode),
            'County': str(county),
            'City': str(city),
        }
        table_data = get_data_from_table(table)
        for value in table_data.values():
            data.update(value)
        return data
    except Exception:
        print(f'Failed: {county}, {year}, {grade}, {school_name}')
        raise


def to_csv(vaccines_df):
    """Convert the vaccines dataframe to csv files"""
    def create_file_name(n):
        return '_'.join(n).replace('-', '_') + '.csv'
    columns = {
        'Sixth': [
            'SCHOOL_NAME',
            'SCHOOL_TYPE',
            'SCHOOL_ADDRESS_ONE',
            'CITY',
            'COUNTY',
            'ZIP_CODE',
            'HAS_NURSE',
            'NURSE_TYPE',
            'ENROLLED',
            'PCT_IMMUNE_DTAP',
            'PCT_EXEMPT_DTAP',
            'PCT_COMPLIANCE_DTAP',
            'PCT_IMMUNE_TDAP',
            'PCT_EXEMPT_TDAP',
            'PCT_COMPLIANCE_TDAP',
            'PCT_IMMUNE_MVMVC',
            'PCT_EXEMPT_MVMVC',
            'PCT_COMPLIANCE_MVMVC',
            'PCT_IMMUNE_POLIO',
            'PCT_EXEMPT_POLIO',
            'PCT_COMPLIANCE_POLIO',
            'PCT_IMMUNE_MMR',
            'PCT_EXEMPT_MMR',
            'PCT_COMPLIANCE_MMR',
            'PCT_IMMUNE_HEPB',
            'PCT_EXEMPT_HEPB',
            'PCT_COMPLIANCE_HEPB',
            'PCT_IMMUNE_VAR',
            'PCT_EXEMPT_VAR',
            'PCT_COMPLIANCE_VAR',
            'PCT_PBE',
            'PCT_MEDICAL_EXEMPT',
            'PCT_PBE_EXEMPT_ALL',
        ],
        'Childcare': [
            'SCHOOL_NAME',
            'SCHOOL_TYPE',
            'SCHOOL_ADDRESS_ONE',
            'CITY',
            'COUNTY',
            'ZIP_CODE',
            'HAS_NURSE',
            'NURSE_TYPE',
            'ENROLLED',
            'PCT_IMMUNE_DTAP',
            'PCT_EXEMPT_DTAP',
            'PCT_COMPLIANCE_DTAP',
            'PCT_IMMUNE_POLIO',
            'PCT_EXEMPT_POLIO',
            'PCT_COMPLIANCE_POLIO',
            'PCT_IMMUNE_MMR',
            'PCT_EXEMPT_MMR',
            'PCT_COMPLIANCE_MMR',
            'PCT_IMMUNE_HIB',
            'PCT_EXEMPT_HIB',
            'PCT_COMPLIANCE_HIB',
            'PCT_IMMUNE_HEPA',
            'PCT_EXEMPT_HEPA',
            'PCT_COMPLIANCE_HEPA',
            'PCT_IMMUNE_HEPB',
            'PCT_EXEMPT_HEPB',
            'PCT_COMPLIANCE_HEPB',
            'PCT_IMMUNE_VAR',
            'PCT_EXEMPT_VAR',
            'PCT_COMPLIANCE_VAR',
            'PCT_PBE',
            'PCT_MEDICAL_EXEMPT',
            'PCT_PBE_EXEMPT_ALL'
        ],
        'Kindergarten': [
            'SCHOOL_NAME',
            'SCHOOL_TYPE',
            'SCHOOL_ADDRESS_ONE',
            'CITY',
            'COUNTY',
            'ZIP_CODE',
            'HAS_NURSE',
            'NURSE_TYPE',
            'ENROLLED',
            'PCT_IMMUNE_DTAP',
            'PCT_EXEMPT_DTAP',
            'PCT_COMPLIANCE_DTAP',
            'PCT_IMMUNE_POLIO',
            'PCT_EXEMPT_POLIO',
            'PCT_COMPLIANCE_POLIO',
            'PCT_IMMUNE_MMR',
            'PCT_EXEMPT_MMR',
            'PCT_COMPLIANCE_MMR',
            'PCT_IMMUNE_HEPB',
            'PCT_EXEMPT_HEPB',
            'PCT_COMPLIANCE_HEPB',
            'PCT_IMMUNE_VAR',
            'PCT_EXEMPT_VAR',
            'PCT_COMPLIANCE_VAR',
            'PCT_PBE',
            'PCT_MEDICAL_EXEMPT',
            'PCT_PBE_EXEMPT_ALL'
        ]
    }
    group_by = ['Grade', 'School Year']
    for name, group in vaccines_df.groupby(group_by):
        grade, year = name
        cols = columns[grade]
        df = pd.DataFrame(group)[cols]
        df.sort_values(by=['SCHOOL_NAME'], inplace=True)
        df.reset_index(drop=True, inplace=True)
        df.to_csv(create_file_name(name), index=False)


if __name__ == '__main__':
    grades = [
        'Childcare',
        'Kindergarten',
        'Sixth',
    ]
    years = [
        '2010-2011',
        '2011-2012',
        '2012-2013',
        '2013-2014',
        '2014-2015',
        '2015-2016',
        '2016-2017',
    ]
    counties = [
        'Apache',
        'Cochise',
        'Coconino',
        'Gila',
        'Graham',
        'Greenlee',
        'La Paz',
        'Maricopa',
        'Mohave',
        'Navajo',
        'Pima',
        'Pinal',
        'Santa Cruz',
        'Yavapai',
        'Yuma',
    ]
    with Pool(processes=7) as pool:
        all_schools = pool.starmap(
            get_schools,
            itertools.product(counties, years, grades),
        )
    schools = [school for school_list in all_schools for school in school_list]

    args = []
    for school in schools:
        school_name = school['SCHOOL_NAME']
        address = school['ADDRESS']
        grade = school['SCHOOL_GRADE']
        year = school['SCHOOL_YEAR']
        county = school['COUNTY']
        zipcode = school['ZIP']
        city = school['CITY']
        args.append((school_name, address, grade, year, county, zipcode, city))
    with Pool(processes=7) as pool:
        vaccines = pool.starmap(get_school_data, args)
    vaccines_df = pd.DataFrame(vaccines)
    names = {
        'Address': 'SCHOOL_ADDRESS_ONE',
        'City': 'CITY',
        'County': 'COUNTY',
        'School': 'SCHOOL_NAME',
        'Zipcode': 'ZIP_CODE'
    }
    vaccines_df.rename(index=str, columns=names, inplace=True)
