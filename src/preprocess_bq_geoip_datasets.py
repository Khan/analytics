"""
Preprocessing and export of geolocation data to enable association of IPv4
addresses with country/region/city information in bigquery.

Preprocessing is necessary because the general strategy for mapping IP
addresses to countries relies on a join of the data associated with IP
addresses and this table on the first byte of the IP address. Some IP address
ranges in the imported data contain multiple first byte values, which would
cause loss of data within these ranges.

This preprocessing will add an additional entry to the table for each initial
IP address byte within one of these ranges.

The general strategy was taken from:
http://stackoverflow.com/questions/19618105/geoip-calculation-in-bigquery-performance/20156781#20156781

Geographic IP data was taken from:
http://dev.maxmind.com/geoip/legacy/geolite/

Usage:
    - cd <directory containing geolite IP files>
    - python preprocess_bq_geoip_datasets.py

The output of each of the functions in this script is a CSV suitable for
upload to bigquery.  The output filenames will mimic the input filenames but
with _formatted before the .csv.

Biqquery schemas for the output:
    Country data: 
        ip_range_lower:integer,ip_range_upper:integer,country_code:string
    City/Region/Country data:
        ip_range_lower:integer,ip_range_upper:integer,country_code:string,
        region:string,city:string
"""
import csv

import pandas as pd


def generate_split_ranges(lower, upper):
    """
    Generate the lower and upper limits of IP ranges split such that they only
    span a single value of the first byte.

    Args:
        lower: the lower value (inclusive) of the unsplit IP range as an
            integer
        upper: the upper value (inclusive) of the unsplit IP range as an
            integer
    Yields:
        tuples containing inclusive lower, upper bounds of each split IP range
        as integers
    """
    lower_byte_0 = lower // (256 ** 3)
    upper_byte_0 = upper // (256 ** 3)

    last_lower = lower
    for byte_0 in xrange(lower_byte_0 + 1, upper_byte_0 + 1):
        next_lower = byte_0 * (256 ** 3)
        curr_upper = next_lower - 1
        yield (last_lower, curr_upper)
        last_lower = next_lower
    yield (last_lower, upper)


def process_country_ip_data():
    """Preprocess the table containing the mapping from IP addresses to
    countries.

    Input is a CSV file "GeoIPCountryWhois.csv" containing columns: dot
    notation ip range lower, dot notation ip range upper, integer ip range
    lower, integer ip range upper, country code, long country name.

    Output is a CSV file for upload to bigquery.  Schema is:
    ip_range_lower:integer,ip_range_upper:integer,country_code:string
    """
    #filename is as it appears in the downloaded geo-ip dataset
    country_ip_fn = 'GeoIPCountryWhois.csv'
    country_ip_fn_out = 'GeoIPCountryWhois_formatted.csv'

    with open(country_ip_fn) as f_in:
        with open(country_ip_fn_out, 'w') as f_out:
            r = csv.reader(f_in)
            w = csv.writer(f_out, quoting=csv.QUOTE_ALL)

            for row in r:
                for lower, upper in generate_split_ranges(
                        int(row[2]), int(row[3])):
                    w.writerow([lower, upper, row[4]])


def process_city_ip_data():
    """Preprocess the table containing the mapping from IP addresses to
    cities/regions.

    (@see #process_country_ip_data for why this is neccessary, and what this
    preprocessing does.)

    Additionally, the city/region information is contained in two tables --
    one containing a mapping from IP address to "location code", and another
    mapping "location code" to actual geographic location.  These are merged
    into a single table.

    Output is a CSV file for upload to bigquery.  Schema is:
    ip_range_lower:integer,ip_range_upper:integer,country_code:string,
    region:string,city:string
    """

    # filenames are as they appear in the downloaded geo-ip dataset
    # IP -> location code
    city_ip_fn_coded = 'GeoLiteCity-Blocks.csv'
    # location code -> physical location
    city_ip_fn_code_defs = 'GeoLiteCity-Location.csv'

    city_ip_fn_out = 'GeoIPCity_formatted.csv'

    ip_coded = pd.read_csv(city_ip_fn_coded, delimiter=",")
    ip_code_defs = pd.read_csv(city_ip_fn_code_defs, delimiter=",")

    ip_info = ip_coded.merge(ip_code_defs, on='locId', how='inner')

    ip_info['start_first_byte'] = ip_info['startIpNum'] // (256 ** 3)
    ip_info['end_first_byte'] = ip_info['endIpNum'] // (256 ** 3)

    #get the entries where an IP range wraps over the first byte
    wrapping_condition = (ip_info['start_first_byte'] 
        - ip_info['end_first_byte'] != 0)
    wrapping = ip_info[wrapping_condition]
    non_wrapping = ip_info[~wrapping_condition]

    #make a new row for each first byte contained in each range that wraps over
    #multiple first bytes
    new_rows = []
    for i, row in wrapping.iterrows():
        for lower, upper in generate_split_ranges(row['startIpNum'],
                                                  row['endIpNum']):
            new_row = row.copy()
            new_row['startIpNum'] = lower
            new_row['endIpNum'] = upper
            new_rows.append(new_row)

    #combine the newly constructed rows with the ranges containing only a
    #single first byte
    ip_info = non_wrapping.append(new_rows)

    #for entries without region or city info, put in an empty string rather
    #than "NaN"
    ip_info['region'][pd.isnull(ip_info['region'])] = ""
    ip_info['city'][pd.isnull(ip_info['city'])] = ""

    with open(city_ip_fn_out, 'w') as f:
        w = csv.writer(f, quoting=csv.QUOTE_ALL)

        for i, row in ip_info.iterrows():
            w.writerow([row['startIpNum'], row['endIpNum'], 
                row['country'], row['region'], row['city']])

if __name__ == '__main__':
    process_country_ip_data()
    process_city_ip_data()

