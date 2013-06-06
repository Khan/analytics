# -*- coding: utf-8 -*-
"""
Due to Hive inability to deal with python modules I (robert) have compiled
this repo: https://github.com/appliedsec/pygeoip into one file
It is almost copy paste into one file, however, it needs fixing of imports

Pure Python GeoIP API

The API is based on MaxMind's C-based Python API, but the code itself is
ported from the Pure PHP GeoIP API by Jim Winstead and Hans Lellelid.

@author: Jennifer Ennis <zaylea@gmail.com>

@license: Copyright(C) 2004 MaxMind LLC

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/lgpl.txt>.
"""


import os
import math
import socket
import mmap
import codecs
import socket
import binascii
from threading import Lock
from platform import python_version_tuple

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO, BytesIO


PY2 = python_version_tuple()[0] == '2'
PY3 = python_version_tuple()[0] == '3'

GEOIP_STANDARD = 0
GEOIP_MEMORY_CACHE = 1

DMA_MAP = {
    500: 'Portland-Auburn, ME',
    501: 'New York, NY',
    502: 'Binghamton, NY',
    503: 'Macon, GA',
    504: 'Philadelphia, PA',
    505: 'Detroit, MI',
    506: 'Boston, MA',
    507: 'Savannah, GA',
    508: 'Pittsburgh, PA',
    509: 'Ft Wayne, IN',
    510: 'Cleveland, OH',
    511: 'Washington, DC',
    512: 'Baltimore, MD',
    513: 'Flint, MI',
    514: 'Buffalo, NY',
    515: 'Cincinnati, OH',
    516: 'Erie, PA',
    517: 'Charlotte, NC',
    518: 'Greensboro, NC',
    519: 'Charleston, SC',
    520: 'Augusta, GA',
    521: 'Providence, RI',
    522: 'Columbus, GA',
    523: 'Burlington, VT',
    524: 'Atlanta, GA',
    525: 'Albany, GA',
    526: 'Utica-Rome, NY',
    527: 'Indianapolis, IN',
    528: 'Miami, FL',
    529: 'Louisville, KY',
    530: 'Tallahassee, FL',
    531: 'Tri-Cities, TN',
    532: 'Albany-Schenectady-Troy, NY',
    533: 'Hartford, CT',
    534: 'Orlando, FL',
    535: 'Columbus, OH',
    536: 'Youngstown-Warren, OH',
    537: 'Bangor, ME',
    538: 'Rochester, NY',
    539: 'Tampa, FL',
    540: 'Traverse City-Cadillac, MI',
    541: 'Lexington, KY',
    542: 'Dayton, OH',
    543: 'Springfield-Holyoke, MA',
    544: 'Norfolk-Portsmouth, VA',
    545: 'Greenville-New Bern-Washington, NC',
    546: 'Columbia, SC',
    547: 'Toledo, OH',
    548: 'West Palm Beach, FL',
    549: 'Watertown, NY',
    550: 'Wilmington, NC',
    551: 'Lansing, MI',
    552: 'Presque Isle, ME',
    553: 'Marquette, MI',
    554: 'Wheeling, WV',
    555: 'Syracuse, NY',
    556: 'Richmond-Petersburg, VA',
    557: 'Knoxville, TN',
    558: 'Lima, OH',
    559: 'Bluefield-Beckley-Oak Hill, WV',
    560: 'Raleigh-Durham, NC',
    561: 'Jacksonville, FL',
    563: 'Grand Rapids, MI',
    564: 'Charleston-Huntington, WV',
    565: 'Elmira, NY',
    566: 'Harrisburg-Lancaster-Lebanon-York, PA',
    567: 'Greenville-Spartenburg, SC',
    569: 'Harrisonburg, VA',
    570: 'Florence-Myrtle Beach, SC',
    571: 'Ft Myers, FL',
    573: 'Roanoke-Lynchburg, VA',
    574: 'Johnstown-Altoona, PA',
    575: 'Chattanooga, TN',
    576: 'Salisbury, MD',
    577: 'Wilkes Barre-Scranton, PA',
    581: 'Terre Haute, IN',
    582: 'Lafayette, IN',
    583: 'Alpena, MI',
    584: 'Charlottesville, VA',
    588: 'South Bend, IN',
    592: 'Gainesville, FL',
    596: 'Zanesville, OH',
    597: 'Parkersburg, WV',
    598: 'Clarksburg-Weston, WV',
    600: 'Corpus Christi, TX',
    602: 'Chicago, IL',
    603: 'Joplin-Pittsburg, MO',
    604: 'Columbia-Jefferson City, MO',
    605: 'Topeka, KS',
    606: 'Dothan, AL',
    609: 'St Louis, MO',
    610: 'Rockford, IL',
    611: 'Rochester-Mason City-Austin, MN',
    612: 'Shreveport, LA',
    613: 'Minneapolis-St Paul, MN',
    616: 'Kansas City, MO',
    617: 'Milwaukee, WI',
    618: 'Houston, TX',
    619: 'Springfield, MO',
    620: 'Tuscaloosa, AL',
    622: 'New Orleans, LA',
    623: 'Dallas-Fort Worth, TX',
    624: 'Sioux City, IA',
    625: 'Waco-Temple-Bryan, TX',
    626: 'Victoria, TX',
    627: 'Wichita Falls, TX',
    628: 'Monroe, LA',
    630: 'Birmingham, AL',
    631: 'Ottumwa-Kirksville, IA',
    632: 'Paducah, KY',
    633: 'Odessa-Midland, TX',
    634: 'Amarillo, TX',
    635: 'Austin, TX',
    636: 'Harlingen, TX',
    637: 'Cedar Rapids-Waterloo, IA',
    638: 'St Joseph, MO',
    639: 'Jackson, TN',
    640: 'Memphis, TN',
    641: 'San Antonio, TX',
    642: 'Lafayette, LA',
    643: 'Lake Charles, LA',
    644: 'Alexandria, LA',
    646: 'Anniston, AL',
    647: 'Greenwood-Greenville, MS',
    648: 'Champaign-Springfield-Decatur, IL',
    649: 'Evansville, IN',
    650: 'Oklahoma City, OK',
    651: 'Lubbock, TX',
    652: 'Omaha, NE',
    656: 'Panama City, FL',
    657: 'Sherman, TX',
    658: 'Green Bay-Appleton, WI',
    659: 'Nashville, TN',
    661: 'San Angelo, TX',
    662: 'Abilene-Sweetwater, TX',
    669: 'Madison, WI',
    670: 'Ft Smith-Fay-Springfield, AR',
    671: 'Tulsa, OK',
    673: 'Columbus-Tupelo-West Point, MS',
    675: 'Peoria-Bloomington, IL',
    676: 'Duluth, MN',
    678: 'Wichita, KS',
    679: 'Des Moines, IA',
    682: 'Davenport-Rock Island-Moline, IL',
    686: 'Mobile, AL',
    687: 'Minot-Bismarck-Dickinson, ND',
    691: 'Huntsville, AL',
    692: 'Beaumont-Port Author, TX',
    693: 'Little Rock-Pine Bluff, AR',
    698: 'Montgomery, AL',
    702: 'La Crosse-Eau Claire, WI',
    705: 'Wausau-Rhinelander, WI',
    709: 'Tyler-Longview, TX',
    710: 'Hattiesburg-Laurel, MS',
    711: 'Meridian, MS',
    716: 'Baton Rouge, LA',
    717: 'Quincy, IL',
    718: 'Jackson, MS',
    722: 'Lincoln-Hastings, NE',
    724: 'Fargo-Valley City, ND',
    725: 'Sioux Falls, SD',
    734: 'Jonesboro, AR',
    736: 'Bowling Green, KY',
    737: 'Mankato, MN',
    740: 'North Platte, NE',
    743: 'Anchorage, AK',
    744: 'Honolulu, HI',
    745: 'Fairbanks, AK',
    746: 'Biloxi-Gulfport, MS',
    747: 'Juneau, AK',
    749: 'Laredo, TX',
    751: 'Denver, CO',
    752: 'Colorado Springs, CO',
    753: 'Phoenix, AZ',
    754: 'Butte-Bozeman, MT',
    755: 'Great Falls, MT',
    756: 'Billings, MT',
    757: 'Boise, ID',
    758: 'Idaho Falls-Pocatello, ID',
    759: 'Cheyenne, WY',
    760: 'Twin Falls, ID',
    762: 'Missoula, MT',
    764: 'Rapid City, SD',
    765: 'El Paso, TX',
    766: 'Helena, MT',
    767: 'Casper-Riverton, WY',
    770: 'Salt Lake City, UT',
    771: 'Yuma, AZ',
    773: 'Grand Junction, CO',
    789: 'Tucson, AZ',
    790: 'Albuquerque, NM',
    798: 'Glendive, MT',
    800: 'Bakersfield, CA',
    801: 'Eugene, OR',
    802: 'Eureka, CA',
    803: 'Los Angeles, CA',
    804: 'Palm Springs, CA',
    807: 'San Francisco, CA',
    810: 'Yakima-Pasco, WA',
    811: 'Reno, NV',
    813: 'Medford-Klamath Falls, OR',
    819: 'Seattle-Tacoma, WA',
    820: 'Portland, OR',
    821: 'Bend, OR',
    825: 'San Diego, CA',
    828: 'Monterey-Salinas, CA',
    839: 'Las Vegas, NV',
    855: 'Santa Barbara, CA',
    862: 'Sacramento, CA',
    866: 'Fresno, CA',
    868: 'Chico-Redding, CA',
    881: 'Spokane, WA'
}

COUNTRY_CODES = (
    '',
    'AP', 'EU', 'AD', 'AE', 'AF', 'AG', 'AI', 'AL', 'AM', 'AN', 'AO', 'AQ',
    'AR', 'AS', 'AT', 'AU', 'AW', 'AZ', 'BA', 'BB', 'BD', 'BE', 'BF', 'BG',
    'BH', 'BI', 'BJ', 'BM', 'BN', 'BO', 'BR', 'BS', 'BT', 'BV', 'BW', 'BY',
    'BZ', 'CA', 'CC', 'CD', 'CF', 'CG', 'CH', 'CI', 'CK', 'CL', 'CM', 'CN',
    'CO', 'CR', 'CU', 'CV', 'CX', 'CY', 'CZ', 'DE', 'DJ', 'DK', 'DM', 'DO',
    'DZ', 'EC', 'EE', 'EG', 'EH', 'ER', 'ES', 'ET', 'FI', 'FJ', 'FK', 'FM',
    'FO', 'FR', 'FX', 'GA', 'GB', 'GD', 'GE', 'GF', 'GH', 'GI', 'GL', 'GM',
    'GN', 'GP', 'GQ', 'GR', 'GS', 'GT', 'GU', 'GW', 'GY', 'HK', 'HM', 'HN',
    'HR', 'HT', 'HU', 'ID', 'IE', 'IL', 'IN', 'IO', 'IQ', 'IR', 'IS', 'IT',
    'JM', 'JO', 'JP', 'KE', 'KG', 'KH', 'KI', 'KM', 'KN', 'KP', 'KR', 'KW',
    'KY', 'KZ', 'LA', 'LB', 'LC', 'LI', 'LK', 'LR', 'LS', 'LT', 'LU', 'LV',
    'LY', 'MA', 'MC', 'MD', 'MG', 'MH', 'MK', 'ML', 'MM', 'MN', 'MO', 'MP',
    'MQ', 'MR', 'MS', 'MT', 'MU', 'MV', 'MW', 'MX', 'MY', 'MZ', 'NA', 'NC',
    'NE', 'NF', 'NG', 'NI', 'NL', 'NO', 'NP', 'NR', 'NU', 'NZ', 'OM', 'PA',
    'PE', 'PF', 'PG', 'PH', 'PK', 'PL', 'PM', 'PN', 'PR', 'PS', 'PT', 'PW',
    'PY', 'QA', 'RE', 'RO', 'RU', 'RW', 'SA', 'SB', 'SC', 'SD', 'SE', 'SG',
    'SH', 'SI', 'SJ', 'SK', 'SL', 'SM', 'SN', 'SO', 'SR', 'ST', 'SV', 'SY',
    'SZ', 'TC', 'TD', 'TF', 'TG', 'TH', 'TJ', 'TK', 'TM', 'TN', 'TO', 'TL',
    'TR', 'TT', 'TV', 'TW', 'TZ', 'UA', 'UG', 'UM', 'US', 'UY', 'UZ', 'VA',
    'VC', 'VE', 'VG', 'VI', 'VN', 'VU', 'WF', 'WS', 'YE', 'YT', 'RS', 'ZA',
    'ZM', 'ME', 'ZW', 'A1', 'A2', 'O1', 'AX', 'GG', 'IM', 'JE', 'BL', 'MF',
    'BQ', 'SS'
)

COUNTRY_CODES3 = (
    '', 'AP', 'EU', 'AND', 'ARE', 'AFG', 'ATG', 'AIA', 'ALB', 'ARM', 'ANT',
    'AGO', 'AQ', 'ARG', 'ASM', 'AUT', 'AUS', 'ABW', 'AZE', 'BIH', 'BRB', 'BGD',
    'BEL', 'BFA', 'BGR', 'BHR', 'BDI', 'BEN', 'BMU', 'BRN', 'BOL', 'BRA',
    'BHS', 'BTN', 'BV', 'BWA', 'BLR', 'BLZ', 'CAN', 'CC', 'COD', 'CAF', 'COG',
    'CHE', 'CIV', 'COK', 'CHL', 'CMR', 'CHN', 'COL', 'CRI', 'CUB', 'CPV', 'CX',
    'CYP', 'CZE', 'DEU', 'DJI', 'DNK', 'DMA', 'DOM', 'DZA', 'ECU', 'EST',
    'EGY', 'ESH', 'ERI', 'ESP', 'ETH', 'FIN', 'FJI', 'FLK', 'FSM', 'FRO',
    'FRA', 'FX', 'GAB', 'GBR', 'GRD', 'GEO', 'GUF', 'GHA', 'GIB', 'GRL', 'GMB',
    'GIN', 'GLP', 'GNQ', 'GRC', 'GS', 'GTM', 'GUM', 'GNB', 'GUY', 'HKG', 'HM',
    'HND', 'HRV', 'HTI', 'HUN', 'IDN', 'IRL', 'ISR', 'IND', 'IO', 'IRQ', 'IRN',
    'ISL', 'ITA', 'JAM', 'JOR', 'JPN', 'KEN', 'KGZ', 'KHM', 'KIR', 'COM',
    'KNA', 'PRK', 'KOR', 'KWT', 'CYM', 'KAZ', 'LAO', 'LBN', 'LCA', 'LIE',
    'LKA', 'LBR', 'LSO', 'LTU', 'LUX', 'LVA', 'LBY', 'MAR', 'MCO', 'MDA',
    'MDG', 'MHL', 'MKD', 'MLI', 'MMR', 'MNG', 'MAC', 'MNP', 'MTQ', 'MRT',
    'MSR', 'MLT', 'MUS', 'MDV', 'MWI', 'MEX', 'MYS', 'MOZ', 'NAM', 'NCL',
    'NER', 'NFK', 'NGA', 'NIC', 'NLD', 'NOR', 'NPL', 'NRU', 'NIU', 'NZL',
    'OMN', 'PAN', 'PER', 'PYF', 'PNG', 'PHL', 'PAK', 'POL', 'SPM', 'PCN',
    'PRI', 'PSE', 'PRT', 'PLW', 'PRY', 'QAT', 'REU', 'ROU', 'RUS', 'RWA',
    'SAU', 'SLB', 'SYC', 'SDN', 'SWE', 'SGP', 'SHN', 'SVN', 'SJM', 'SVK',
    'SLE', 'SMR', 'SEN', 'SOM', 'SUR', 'STP', 'SLV', 'SYR', 'SWZ', 'TCA',
    'TCD', 'TF', 'TGO', 'THA', 'TJK', 'TKL', 'TLS', 'TKM', 'TUN', 'TON', 'TUR',
    'TTO', 'TUV', 'TWN', 'TZA', 'UKR', 'UGA', 'UM', 'USA', 'URY', 'UZB', 'VAT',
    'VCT', 'VEN', 'VGB', 'VIR', 'VNM', 'VUT', 'WLF', 'WSM', 'YEM', 'YT', 'SRB',
    'ZAF', 'ZMB', 'MNE', 'ZWE', 'A1', 'A2', 'O1', 'ALA', 'GGY', 'IMN', 'JEY',
    'BLM', 'MAF', 'BES', 'SSD'
)

COUNTRY_NAMES = (
    '', 'Asia/Pacific Region', 'Europe', 'Andorra', 'United Arab Emirates',
    'Afghanistan', 'Antigua and Barbuda', 'Anguilla', 'Albania', 'Armenia',
    'Netherlands Antilles', 'Angola', 'Antarctica', 'Argentina',
    'American Samoa', 'Austria', 'Australia', 'Aruba', 'Azerbaijan',
    'Bosnia and Herzegovina', 'Barbados', 'Bangladesh', 'Belgium',
    'Burkina Faso', 'Bulgaria', 'Bahrain', 'Burundi', 'Benin', 'Bermuda',
    'Brunei Darussalam', 'Bolivia', 'Brazil', 'Bahamas', 'Bhutan',
    'Bouvet Island', 'Botswana', 'Belarus', 'Belize', 'Canada',
    'Cocos (Keeling) Islands', 'Congo, The Democratic Republic of the',
    'Central African Republic', 'Congo', 'Switzerland', 'Cote D\'Ivoire',
    'Cook Islands', 'Chile', 'Cameroon', 'China', 'Colombia', 'Costa Rica',
    'Cuba', 'Cape Verde', 'Christmas Island', 'Cyprus', 'Czech Republic',
    'Germany', 'Djibouti', 'Denmark', 'Dominica', 'Dominican Republic',
    'Algeria', 'Ecuador', 'Estonia', 'Egypt', 'Western Sahara', 'Eritrea',
    'Spain', 'Ethiopia', 'Finland', 'Fiji', 'Falkland Islands (Malvinas)',
    'Micronesia, Federated States of', 'Faroe Islands', 'France',
    'France, Metropolitan', 'Gabon', 'United Kingdom', 'Grenada', 'Georgia',
    'French Guiana', 'Ghana', 'Gibraltar', 'Greenland', 'Gambia', 'Guinea',
    'Guadeloupe', 'Equatorial Guinea', 'Greece',
    'South Georgia and the South Sandwich Islands', 'Guatemala', 'Guam',
    'Guinea-Bissau', 'Guyana', 'Hong Kong',
    'Heard Island and McDonald Islands', 'Honduras', 'Croatia', 'Haiti',
    'Hungary', 'Indonesia', 'Ireland', 'Israel', 'India',
    'British Indian Ocean Territory', 'Iraq', 'Iran, Islamic Republic of',
    'Iceland', 'Italy', 'Jamaica', 'Jordan', 'Japan', 'Kenya', 'Kyrgyzstan',
    'Cambodia', 'Kiribati', 'Comoros', 'Saint Kitts and Nevis',
    'Korea, Democratic People\'s Republic of', 'Korea, Republic of', 'Kuwait',
    'Cayman Islands', 'Kazakhstan', 'Lao People\'s Democratic Republic',
    'Lebanon', 'Saint Lucia', 'Liechtenstein', 'Sri Lanka', 'Liberia',
    'Lesotho', 'Lithuania', 'Luxembourg', 'Latvia', 'Libya', 'Morocco',
    'Monaco', 'Moldova, Republic of', 'Madagascar', 'Marshall Islands',
    'Macedonia', 'Mali', 'Myanmar', 'Mongolia', 'Macau',
    'Northern Mariana Islands', 'Martinique', 'Mauritania', 'Montserrat',
    'Malta', 'Mauritius', 'Maldives', 'Malawi', 'Mexico', 'Malaysia',
    'Mozambique', 'Namibia', 'New Caledonia', 'Niger', 'Norfolk Island',
    'Nigeria', 'Nicaragua', 'Netherlands', 'Norway', 'Nepal', 'Nauru', 'Niue',
    'New Zealand', 'Oman', 'Panama', 'Peru', 'French Polynesia',
    'Papua New Guinea', 'Philippines', 'Pakistan', 'Poland',
    'Saint Pierre and Miquelon', 'Pitcairn Islands', 'Puerto Rico',
    'Palestinian Territory', 'Portugal', 'Palau', 'Paraguay', 'Qatar',
    'Reunion', 'Romania', 'Russian Federation', 'Rwanda', 'Saudi Arabia',
    'Solomon Islands', 'Seychelles', 'Sudan', 'Sweden', 'Singapore',
    'Saint Helena', 'Slovenia', 'Svalbard and Jan Mayen', 'Slovakia',
    'Sierra Leone', 'San Marino', 'Senegal', 'Somalia', 'Suriname',
    'Sao Tome and Principe', 'El Salvador', 'Syrian Arab Republic',
    'Swaziland', 'Turks and Caicos Islands', 'Chad',
    'French Southern Territories', 'Togo', 'Thailand', 'Tajikistan', 'Tokelau',
    'Turkmenistan', 'Tunisia', 'Tonga', 'Timor-Leste', 'Turkey',
    'Trinidad and Tobago', 'Tuvalu', 'Taiwan', 'Tanzania, United Republic of',
    'Ukraine', 'Uganda', 'United States Minor Outlying Islands',
    'United States', 'Uruguay', 'Uzbekistan', 'Holy See (Vatican City State)',
    'Saint Vincent and the Grenadines', 'Venezuela', 'Virgin Islands, British',
    'Virgin Islands, U.S.', 'Vietnam', 'Vanuatu', 'Wallis and Futuna', 'Samoa',
    'Yemen', 'Mayotte', 'Serbia', 'South Africa', 'Zambia', 'Montenegro',
    'Zimbabwe', 'Anonymous Proxy', 'Satellite Provider', 'Other',
    'Aland Islands', 'Guernsey', 'Isle of Man', 'Jersey', 'Saint Barthelemy',
    'Saint Martin', 'Bonaire, Sint Eustatius and Saba', 'South Sudan'
)

CONTINENT_NAMES = (
    '--', 'AS', 'EU', 'EU', 'AS', 'AS', 'NA', 'NA', 'EU', 'AS', 'NA', 'AF',
    'AN', 'SA', 'OC', 'EU', 'OC', 'NA', 'AS', 'EU', 'NA', 'AS', 'EU', 'AF',
    'EU', 'AS', 'AF', 'AF', 'NA', 'AS', 'SA', 'SA', 'NA', 'AS', 'AN', 'AF',
    'EU', 'NA', 'NA', 'AS', 'AF', 'AF', 'AF', 'EU', 'AF', 'OC', 'SA', 'AF',
    'AS', 'SA', 'NA', 'NA', 'AF', 'AS', 'AS', 'EU', 'EU', 'AF', 'EU', 'NA',
    'NA', 'AF', 'SA', 'EU', 'AF', 'AF', 'AF', 'EU', 'AF', 'EU', 'OC', 'SA',
    'OC', 'EU', 'EU', 'NA', 'AF', 'EU', 'NA', 'AS', 'SA', 'AF', 'EU', 'NA',
    'AF', 'AF', 'NA', 'AF', 'EU', 'AN', 'NA', 'OC', 'AF', 'SA', 'AS', 'AN',
    'NA', 'EU', 'NA', 'EU', 'AS', 'EU', 'AS', 'AS', 'AS', 'AS', 'AS', 'EU',
    'EU', 'NA', 'AS', 'AS', 'AF', 'AS', 'AS', 'OC', 'AF', 'NA', 'AS', 'AS',
    'AS', 'NA', 'AS', 'AS', 'AS', 'NA', 'EU', 'AS', 'AF', 'AF', 'EU', 'EU',
    'EU', 'AF', 'AF', 'EU', 'EU', 'AF', 'OC', 'EU', 'AF', 'AS', 'AS', 'AS',
    'OC', 'NA', 'AF', 'NA', 'EU', 'AF', 'AS', 'AF', 'NA', 'AS', 'AF', 'AF',
    'OC', 'AF', 'OC', 'AF', 'NA', 'EU', 'EU', 'AS', 'OC', 'OC', 'OC', 'AS',
    'NA', 'SA', 'OC', 'OC', 'AS', 'AS', 'EU', 'NA', 'OC', 'NA', 'AS', 'EU',
    'OC', 'SA', 'AS', 'AF', 'EU', 'EU', 'AF', 'AS', 'OC', 'AF', 'AF', 'EU',
    'AS', 'AF', 'EU', 'EU', 'EU', 'AF', 'EU', 'AF', 'AF', 'SA', 'AF', 'NA',
    'AS', 'AF', 'NA', 'AF', 'AN', 'AF', 'AS', 'AS', 'OC', 'AS', 'AF', 'OC',
    'AS', 'EU', 'NA', 'OC', 'AS', 'AF', 'EU', 'AF', 'OC', 'NA', 'SA', 'AS',
    'EU', 'NA', 'SA', 'NA', 'NA', 'AS', 'OC', 'OC', 'OC', 'AS', 'AF', 'EU',
    'AF', 'AF', 'EU', 'AF', '--', '--', '--', 'EU', 'EU', 'EU', 'EU', 'NA',
    'NA', 'NA', 'AF'
)

# storage / caching flags
STANDARD = 0
MEMORY_CACHE = 1
MMAP_CACHE = 8

# Database structure constants
COUNTRY_BEGIN = 16776960
STATE_BEGIN_REV0 = 16700000
STATE_BEGIN_REV1 = 16000000

STRUCTURE_INFO_MAX_SIZE = 20
DATABASE_INFO_MAX_SIZE = 100

# Database editions
COUNTRY_EDITION = 1
COUNTRY_EDITION_V6 = 12
REGION_EDITION_REV0 = 7
REGION_EDITION_REV1 = 3
CITY_EDITION_REV0 = 6
CITY_EDITION_REV1 = 2
CITY_EDITION_REV1_V6 = 30
ORG_EDITION = 5
ISP_EDITION = 4
ASNUM_EDITION = 9
ASNUM_EDITION_V6 = 21
# Not yet supported databases
PROXY_EDITION = 8
NETSPEED_EDITION = 11

# Collection of databases
IPV6_EDITIONS = (COUNTRY_EDITION_V6, ASNUM_EDITION_V6, CITY_EDITION_REV1_V6)
CITY_EDITIONS = (CITY_EDITION_REV0, CITY_EDITION_REV1, CITY_EDITION_REV1_V6)
REGION_EDITIONS = (REGION_EDITION_REV0, REGION_EDITION_REV1)
REGION_CITY_EDITIONS = REGION_EDITIONS + CITY_EDITIONS

SEGMENT_RECORD_LENGTH = 3
STANDARD_RECORD_LENGTH = 3
ORG_RECORD_LENGTH = 4
MAX_RECORD_LENGTH = 4
MAX_ORG_RECORD_LENGTH = 300
FULL_RECORD_LENGTH = 50

US_OFFSET = 1
CANADA_OFFSET = 677
WORLD_OFFSET = 1353
FIPS_RANGE = 360
ENCODING = 'iso-8859-1'

_country = {
    'AD': 'Europe/Andorra',
    'AE': 'Asia/Dubai',
    'AF': 'Asia/Kabul',
    'AG': 'America/Antigua',
    'AI': 'America/Anguilla',
    'AL': 'Europe/Tirane',
    'AM': 'Asia/Yerevan',
    'AO': 'Africa/Luanda',
    'AR': {
        '01': 'America/Argentina/Buenos_Aires',
        '02': 'America/Argentina/Catamarca',
        '03': 'America/Argentina/Tucuman',
        '04': 'America/Argentina/Rio_Gallegos',
        '05': 'America/Argentina/Cordoba',
        '06': 'America/Argentina/Tucuman',
        '07': 'America/Argentina/Buenos_Aires',
        '08': 'America/Argentina/Buenos_Aires',
        '09': 'America/Argentina/Tucuman',
        '10': 'America/Argentina/Jujuy',
        '11': 'America/Argentina/San_Luis',
        '12': 'America/Argentina/La_Rioja',
        '13': 'America/Argentina/Mendoza',
        '14': 'America/Argentina/Buenos_Aires',
        '15': 'America/Argentina/San_Luis',
        '16': 'America/Argentina/Buenos_Aires',
        '17': 'America/Argentina/Salta',
        '18': 'America/Argentina/San_Juan',
        '19': 'America/Argentina/San_Luis',
        '20': 'America/Argentina/Rio_Gallegos',
        '21': 'America/Argentina/Buenos_Aires',
        '22': 'America/Argentina/Catamarca',
        '23': 'America/Argentina/Ushuaia',
        '24': 'America/Argentina/Tucuman'
    },
    'AS': 'US/Samoa',
    'AT': 'Europe/Vienna',
    'AU': {
        '01': 'Australia/Canberra',
        '02': 'Australia/NSW',
        '03': 'Australia/North',
        '04': 'Australia/Queensland',
        '05': 'Australia/South',
        '06': 'Australia/Tasmania',
        '07': 'Australia/Victoria',
        '08': 'Australia/West'
    },
    'AW': 'America/Aruba',
    'AX': 'Europe/Mariehamn',
    'AZ': 'Asia/Baku',
    'BA': 'Europe/Sarajevo',
    'BB': 'America/Barbados',
    'BD': 'Asia/Dhaka',
    'BE': 'Europe/Brussels',
    'BF': 'Africa/Ouagadougou',
    'BG': 'Europe/Sofia',
    'BH': 'Asia/Bahrain',
    'BI': 'Africa/Bujumbura',
    'BJ': 'Africa/Porto-Novo',
    'BL': 'America/St_Barthelemy',
    'BM': 'Atlantic/Bermuda',
    'BN': 'Asia/Brunei',
    'BO': 'America/La_Paz',
    'BQ': 'America/Curacao',
    'BR': {
        '01': 'America/Rio_Branco',
        '02': 'America/Maceio',
        '03': 'America/Sao_Paulo',
        '04': 'America/Manaus',
        '05': 'America/Bahia',
        '06': 'America/Fortaleza',
        '07': 'America/Sao_Paulo',
        '08': 'America/Sao_Paulo',
        '11': 'America/Campo_Grande',
        '13': 'America/Belem',
        '14': 'America/Cuiaba',
        '15': 'America/Sao_Paulo',
        '16': 'America/Belem',
        '17': 'America/Recife',
        '18': 'America/Sao_Paulo',
        '20': 'America/Fortaleza',
        '21': 'America/Sao_Paulo',
        '22': 'America/Recife',
        '23': 'America/Sao_Paulo',
        '24': 'America/Porto_Velho',
        '25': 'America/Boa_Vista',
        '26': 'America/Sao_Paulo',
        '27': 'America/Sao_Paulo',
        '28': 'America/Maceio',
        '29': 'America/Sao_Paulo',
        '30': 'America/Recife',
        '31': 'America/Araguaina'
    },
    'BS': 'America/Nassau',
    'BT': 'Asia/Thimphu',
    'BW': 'Africa/Gaborone',
    'BY': 'Europe/Minsk',
    'BZ': 'America/Belize',
    'CA': {
        'AB': 'America/Edmonton',
        'BC': 'America/Vancouver',
        'MB': 'America/Winnipeg',
        'NB': 'America/Halifax',
        'NL': 'America/St_Johns',
        'NS': 'America/Halifax',
        'NT': 'America/Yellowknife',
        'NU': 'America/Rankin_Inlet',
        'ON': 'America/Rainy_River',
        'PE': 'America/Halifax',
        'QC': 'America/Montreal',
        'SK': 'America/Regina',
        'YT': 'America/Whitehorse'
    },
    'CC': 'Indian/Cocos',
    'CD': {
        '02': 'Africa/Kinshasa',
        '05': 'Africa/Lubumbashi',
        '06': 'Africa/Kinshasa',
        '08': 'Africa/Kinshasa',
        '10': 'Africa/Lubumbashi',
        '11': 'Africa/Lubumbashi',
        '12': 'Africa/Lubumbashi'
    },
    'CF': 'Africa/Bangui',
    'CG': 'Africa/Brazzaville',
    'CH': 'Europe/Zurich',
    'CI': 'Africa/Abidjan',
    'CK': 'Pacific/Rarotonga',
    'CL': 'Chile/Continental',
    'CM': 'Africa/Lagos',
    'CN': {
        '01': 'Asia/Shanghai',
        '02': 'Asia/Shanghai',
        '03': 'Asia/Shanghai',
        '04': 'Asia/Shanghai',
        '05': 'Asia/Harbin',
        '06': 'Asia/Chongqing',
        '07': 'Asia/Shanghai',
        '08': 'Asia/Harbin',
        '09': 'Asia/Shanghai',
        '10': 'Asia/Shanghai',
        '11': 'Asia/Chongqing',
        '12': 'Asia/Shanghai',
        '13': 'Asia/Urumqi',
        '14': 'Asia/Chongqing',
        '15': 'Asia/Chongqing',
        '16': 'Asia/Chongqing',
        '18': 'Asia/Chongqing',
        '19': 'Asia/Harbin',
        '20': 'Asia/Harbin',
        '21': 'Asia/Chongqing',
        '22': 'Asia/Harbin',
        '23': 'Asia/Shanghai',
        '24': 'Asia/Chongqing',
        '25': 'Asia/Shanghai',
        '26': 'Asia/Chongqing',
        '28': 'Asia/Shanghai',
        '29': 'Asia/Chongqing',
        '30': 'Asia/Chongqing',
        '31': 'Asia/Chongqing',
        '32': 'Asia/Chongqing',
        '33': 'Asia/Chongqing'
    },
    'CO': 'America/Bogota',
    'CR': 'America/Costa_Rica',
    'CU': 'America/Havana',
    'CV': 'Atlantic/Cape_Verde',
    'CW': 'America/Curacao',
    'CX': 'Indian/Christmas',
    'CY': 'Asia/Nicosia',
    'CZ': 'Europe/Prague',
    'DE': 'Europe/Berlin',
    'DJ': 'Africa/Djibouti',
    'DK': 'Europe/Copenhagen',
    'DM': 'America/Dominica',
    'DO': 'America/Santo_Domingo',
    'DZ': 'Africa/Algiers',
    'EC': {
        '01': 'Pacific/Galapagos',
        '02': 'America/Guayaquil',
        '03': 'America/Guayaquil',
        '04': 'America/Guayaquil',
        '05': 'America/Guayaquil',
        '06': 'America/Guayaquil',
        '07': 'America/Guayaquil',
        '08': 'America/Guayaquil',
        '09': 'America/Guayaquil',
        '10': 'America/Guayaquil',
        '11': 'America/Guayaquil',
        '12': 'America/Guayaquil',
        '13': 'America/Guayaquil',
        '14': 'America/Guayaquil',
        '15': 'America/Guayaquil',
        '17': 'America/Guayaquil',
        '19': 'America/Guayaquil',
        '20': 'America/Guayaquil',
        '22': 'America/Guayaquil'
    },
    'EE': 'Europe/Tallinn',
    'EG': 'Africa/Cairo',
    'EH': 'Africa/El_Aaiun',
    'ER': 'Africa/Asmera',
    'ES': {
        '07': 'Europe/Madrid',
        '27': 'Europe/Madrid',
        '29': 'Europe/Madrid',
        '31': 'Europe/Madrid',
        '32': 'Europe/Madrid',
        '34': 'Europe/Madrid',
        '39': 'Europe/Madrid',
        '51': 'Africa/Ceuta',
        '52': 'Europe/Madrid',
        '53': 'Atlantic/Canary',
        '54': 'Europe/Madrid',
        '55': 'Europe/Madrid',
        '56': 'Europe/Madrid',
        '57': 'Europe/Madrid',
        '58': 'Europe/Madrid',
        '59': 'Europe/Madrid'
    },
    'ET': 'Africa/Addis_Ababa',
    'FI': 'Europe/Helsinki',
    'FJ': 'Pacific/Fiji',
    'FK': 'Atlantic/Stanley',
    'FO': 'Atlantic/Faeroe',
    'FR': 'Europe/Paris',
    'GA': 'Africa/Libreville',
    'GB': 'Europe/London',
    'GD': 'America/Grenada',
    'GE': 'Asia/Tbilisi',
    'GF': 'America/Cayenne',
    'GG': 'Europe/Guernsey',
    'GH': 'Africa/Accra',
    'GI': 'Europe/Gibraltar',
    'GL': {
        '01': 'America/Thule',
        '02': 'America/Godthab',
        '03': 'America/Godthab'
    },
    'GM': 'Africa/Banjul',
    'GN': 'Africa/Conakry',
    'GP': 'America/Guadeloupe',
    'GQ': 'Africa/Malabo',
    'GR': 'Europe/Athens',
    'GS': 'Atlantic/South_Georgia',
    'GT': 'America/Guatemala',
    'GU': 'Pacific/Guam',
    'GW': 'Africa/Bissau',
    'GY': 'America/Guyana',
    'HK': 'Asia/Hong_Kong',
    'HN': 'America/Tegucigalpa',
    'HR': 'Europe/Zagreb',
    'HT': 'America/Port-au-Prince',
    'HU': 'Europe/Budapest',
    'ID': {
        '01': 'Asia/Pontianak',
        '02': 'Asia/Makassar',
        '03': 'Asia/Jakarta',
        '04': 'Asia/Jakarta',
        '05': 'Asia/Jakarta',
        '06': 'Asia/Jakarta',
        '07': 'Asia/Jakarta',
        '08': 'Asia/Jakarta',
        '09': 'Asia/Jayapura',
        '10': 'Asia/Jakarta',
        '11': 'Asia/Pontianak',
        '12': 'Asia/Makassar',
        '13': 'Asia/Makassar',
        '14': 'Asia/Makassar',
        '15': 'Asia/Jakarta',
        '16': 'Asia/Makassar',
        '17': 'Asia/Makassar',
        '18': 'Asia/Makassar',
        '19': 'Asia/Pontianak',
        '20': 'Asia/Makassar',
        '21': 'Asia/Makassar',
        '22': 'Asia/Makassar',
        '23': 'Asia/Makassar',
        '24': 'Asia/Jakarta',
        '25': 'Asia/Pontianak',
        '26': 'Asia/Pontianak',
        '30': 'Asia/Jakarta',
        '31': 'Asia/Makassar',
        '33': 'Asia/Jakarta'
    },
    'IE': 'Europe/Dublin',
    'IL': 'Asia/Jerusalem',
    'IM': 'Europe/Isle_of_Man',
    'IN': 'Asia/Calcutta',
    'IO': 'Indian/Chagos',
    'IQ': 'Asia/Baghdad',
    'IR': 'Asia/Tehran',
    'IS': 'Atlantic/Reykjavik',
    'IT': 'Europe/Rome',
    'JE': 'Europe/Jersey',
    'JM': 'America/Jamaica',
    'JO': 'Asia/Amman',
    'JP': 'Asia/Tokyo',
    'KE': 'Africa/Nairobi',
    'KG': 'Asia/Bishkek',
    'KH': 'Asia/Phnom_Penh',
    'KI': 'Pacific/Tarawa',
    'KM': 'Indian/Comoro',
    'KN': 'America/St_Kitts',
    'KP': 'Asia/Pyongyang',
    'KR': 'Asia/Seoul',
    'KW': 'Asia/Kuwait',
    'KY': 'America/Cayman',
    'KZ': {
        '01': 'Asia/Almaty',
        '02': 'Asia/Almaty',
        '03': 'Asia/Qyzylorda',
        '05': 'Asia/Qyzylorda',
        '06': 'Asia/Aqtau',
        '07': 'Asia/Oral',
        '08': 'Asia/Qyzylorda',
        '10': 'Asia/Qyzylorda',
        '11': 'Asia/Almaty',
        '12': 'Asia/Qyzylorda',
        '13': 'Asia/Aqtobe',
        '14': 'Asia/Qyzylorda',
        '15': 'Asia/Almaty',
        '16': 'Asia/Aqtobe',
        '17': 'Asia/Almaty'
    },
    'LA': 'Asia/Vientiane',
    'LB': 'Asia/Beirut',
    'LC': 'America/St_Lucia',
    'LI': 'Europe/Vaduz',
    'LK': 'Asia/Colombo',
    'LR': 'Africa/Monrovia',
    'LS': 'Africa/Maseru',
    'LT': 'Europe/Vilnius',
    'LU': 'Europe/Luxembourg',
    'LV': 'Europe/Riga',
    'LY': 'Africa/Tripoli',
    'MA': 'Africa/Casablanca',
    'MC': 'Europe/Monaco',
    'MD': 'Europe/Chisinau',
    'ME': 'Europe/Podgorica',
    'MF': 'America/Marigot',
    'MG': 'Indian/Antananarivo',
    'MK': 'Europe/Skopje',
    'ML': 'Africa/Bamako',
    'MM': 'Asia/Rangoon',
    'MN': 'Asia/Choibalsan',
    'MO': 'Asia/Macao',
    'MP': 'Pacific/Saipan',
    'MQ': 'America/Martinique',
    'MR': 'Africa/Nouakchott',
    'MS': 'America/Montserrat',
    'MT': 'Europe/Malta',
    'MU': 'Indian/Mauritius',
    'MV': 'Indian/Maldives',
    'MW': 'Africa/Blantyre',
    'MX': {
        '01': 'America/Mexico_City',
        '02': 'America/Tijuana',
        '03': 'America/Hermosillo',
        '04': 'America/Merida',
        '05': 'America/Mexico_City',
        '06': 'America/Chihuahua',
        '07': 'America/Monterrey',
        '08': 'America/Mexico_City',
        '09': 'America/Mexico_City',
        '10': 'America/Mazatlan',
        '11': 'America/Mexico_City',
        '12': 'America/Mexico_City',
        '13': 'America/Mexico_City',
        '14': 'America/Mazatlan',
        '15': 'America/Chihuahua',
        '16': 'America/Mexico_City',
        '17': 'America/Mexico_City',
        '18': 'America/Mazatlan',
        '19': 'America/Monterrey',
        '20': 'America/Mexico_City',
        '21': 'America/Mexico_City',
        '22': 'America/Mexico_City',
        '23': 'America/Cancun',
        '24': 'America/Mexico_City',
        '25': 'America/Mazatlan',
        '26': 'America/Hermosillo',
        '27': 'America/Merida',
        '28': 'America/Monterrey',
        '29': 'America/Mexico_City',
        '30': 'America/Mexico_City',
        '31': 'America/Merida',
        '32': 'America/Monterrey'
    },
    'MY': {
        '01': 'Asia/Kuala_Lumpur',
        '02': 'Asia/Kuala_Lumpur',
        '03': 'Asia/Kuala_Lumpur',
        '07': 'Asia/Kuala_Lumpur',
        '08': 'Asia/Kuala_Lumpur',
        '09': 'Asia/Kuala_Lumpur',
        '11': 'Asia/Kuching',
        '12': 'Asia/Kuala_Lumpur',
        '13': 'Asia/Kuala_Lumpur',
        '14': 'Asia/Kuala_Lumpur',
        '15': 'Asia/Kuching',
        '16': 'Asia/Kuching'
    },
    'MZ': 'Africa/Maputo',
    'NA': 'Africa/Windhoek',
    'NC': 'Pacific/Noumea',
    'NE': 'Africa/Niamey',
    'NF': 'Pacific/Norfolk',
    'NG': 'Africa/Lagos',
    'NI': 'America/Managua',
    'NL': 'Europe/Amsterdam',
    'NO': 'Europe/Oslo',
    'NP': 'Asia/Katmandu',
    'NR': 'Pacific/Nauru',
    'NU': 'Pacific/Niue',
    'NZ': {
        '85': 'Pacific/Auckland',
        'E7': 'Pacific/Auckland',
        'E8': 'Pacific/Auckland',
        'E9': 'Pacific/Auckland',
        'F1': 'Pacific/Auckland',
        'F2': 'Pacific/Auckland',
        'F4': 'Pacific/Auckland',
        'F5': 'Pacific/Auckland',
        'F7': 'Pacific/Chatham',
        'F8': 'Pacific/Auckland',
        'G1': 'Pacific/Auckland',
        'G2': 'Pacific/Auckland',
        'G3': 'Pacific/Auckland'
    },
    'OM': 'Asia/Muscat',
    'PA': 'America/Panama',
    'PE': 'America/Lima',
    'PF': 'Pacific/Marquesas',
    'PG': 'Pacific/Port_Moresby',
    'PH': 'Asia/Manila',
    'PK': 'Asia/Karachi',
    'PL': 'Europe/Warsaw',
    'PM': 'America/Miquelon',
    'PN': 'Pacific/Pitcairn',
    'PR': 'America/Puerto_Rico',
    'PS': 'Asia/Gaza',
    'PT': {
        '02': 'Europe/Lisbon',
        '03': 'Europe/Lisbon',
        '04': 'Europe/Lisbon',
        '05': 'Europe/Lisbon',
        '06': 'Europe/Lisbon',
        '07': 'Europe/Lisbon',
        '08': 'Europe/Lisbon',
        '09': 'Europe/Lisbon',
        '10': 'Atlantic/Madeira',
        '11': 'Europe/Lisbon',
        '13': 'Europe/Lisbon',
        '14': 'Europe/Lisbon',
        '16': 'Europe/Lisbon',
        '17': 'Europe/Lisbon',
        '18': 'Europe/Lisbon',
        '19': 'Europe/Lisbon',
        '20': 'Europe/Lisbon',
        '21': 'Europe/Lisbon',
        '22': 'Europe/Lisbon'
    },
    'PW': 'Pacific/Palau',
    'PY': 'America/Asuncion',
    'QA': 'Asia/Qatar',
    'RE': 'Indian/Reunion',
    'RO': 'Europe/Bucharest',
    'RS': 'Europe/Belgrade',
    'RU': {
        '01': 'Europe/Volgograd',
        '02': 'Asia/Irkutsk',
        '03': 'Asia/Novokuznetsk',
        '04': 'Asia/Novosibirsk',
        '05': 'Asia/Vladivostok',
        '06': 'Europe/Moscow',
        '07': 'Europe/Volgograd',
        '08': 'Europe/Samara',
        '09': 'Europe/Moscow',
        '10': 'Europe/Moscow',
        '11': 'Asia/Irkutsk',
        '13': 'Asia/Yekaterinburg',
        '14': 'Asia/Irkutsk',
        '15': 'Asia/Anadyr',
        '16': 'Europe/Samara',
        '17': 'Europe/Volgograd',
        '18': 'Asia/Krasnoyarsk',
        '20': 'Asia/Irkutsk',
        '21': 'Europe/Moscow',
        '22': 'Europe/Volgograd',
        '23': 'Europe/Kaliningrad',
        '24': 'Europe/Volgograd',
        '25': 'Europe/Moscow',
        '26': 'Asia/Kamchatka',
        '27': 'Europe/Volgograd',
        '28': 'Europe/Moscow',
        '30': 'Asia/Vladivostok',
        '31': 'Asia/Krasnoyarsk',
        '32': 'Asia/Omsk',
        '33': 'Asia/Yekaterinburg',
        '34': 'Asia/Yekaterinburg',
        '35': 'Asia/Yekaterinburg',
        '36': 'Asia/Anadyr',
        '37': 'Europe/Moscow',
        '38': 'Europe/Volgograd',
        '39': 'Asia/Krasnoyarsk',
        '40': 'Asia/Yekaterinburg',
        '41': 'Europe/Moscow',
        '42': 'Europe/Moscow',
        '43': 'Europe/Moscow',
        '45': 'Europe/Samara',
        '46': 'Europe/Samara',
        '47': 'Europe/Moscow',
        '48': 'Europe/Moscow',
        '49': 'Europe/Moscow',
        '50': 'Asia/Yekaterinburg',
        '51': 'Europe/Moscow',
        '52': 'Europe/Moscow',
        '53': 'Asia/Novosibirsk',
        '54': 'Asia/Omsk',
        '55': 'Europe/Samara',
        '56': 'Europe/Moscow',
        '57': 'Europe/Samara',
        '58': 'Asia/Yekaterinburg',
        '59': 'Asia/Vladivostok',
        '60': 'Europe/Kaliningrad',
        '61': 'Europe/Volgograd',
        '62': 'Europe/Moscow',
        '63': 'Asia/Yakutsk',
        '65': 'Europe/Samara',
        '66': 'Europe/Moscow',
        '68': 'Europe/Volgograd',
        '69': 'Europe/Moscow',
        '70': 'Europe/Volgograd',
        '71': 'Asia/Yekaterinburg',
        '72': 'Europe/Moscow',
        '73': 'Europe/Samara',
        '74': 'Asia/Krasnoyarsk',
        '75': 'Asia/Novosibirsk',
        '76': 'Europe/Moscow',
        '77': 'Europe/Moscow',
        '79': 'Asia/Irkutsk',
        '80': 'Asia/Yekaterinburg',
        '81': 'Europe/Samara',
        '82': 'Asia/Irkutsk',
        '84': 'Europe/Volgograd',
        '85': 'Europe/Moscow',
        '86': 'Europe/Moscow',
        '87': 'Asia/Novosibirsk',
        '88': 'Europe/Moscow',
        '89': 'Asia/Vladivostok'
    },
    'RW': 'Africa/Kigali',
    'SA': 'Asia/Riyadh',
    'SB': 'Pacific/Guadalcanal',
    'SC': 'Indian/Mahe',
    'SD': 'Africa/Khartoum',
    'SE': 'Europe/Stockholm',
    'SG': 'Asia/Singapore',
    'SH': 'Atlantic/St_Helena',
    'SI': 'Europe/Ljubljana',
    'SJ': 'Arctic/Longyearbyen',
    'SK': 'Europe/Bratislava',
    'SL': 'Africa/Freetown',
    'SM': 'Europe/San_Marino',
    'SN': 'Africa/Dakar',
    'SO': 'Africa/Mogadishu',
    'SR': 'America/Paramaribo',
    'ST': 'Africa/Sao_Tome',
    'SV': 'America/El_Salvador',
    'SX': 'America/Curacao',
    'SY': 'Asia/Damascus',
    'SZ': 'Africa/Mbabane',
    'TC': 'America/Grand_Turk',
    'TD': 'Africa/Ndjamena',
    'TF': 'Indian/Kerguelen',
    'TG': 'Africa/Lome',
    'TH': 'Asia/Bangkok',
    'TJ': 'Asia/Dushanbe',
    'TK': 'Pacific/Fakaofo',
    'TL': 'Asia/Dili',
    'TM': 'Asia/Ashgabat',
    'TN': 'Africa/Tunis',
    'TO': 'Pacific/Tongatapu',
    'TR': 'Asia/Istanbul',
    'TT': 'America/Port_of_Spain',
    'TV': 'Pacific/Funafuti',
    'TW': 'Asia/Taipei',
    'TZ': 'Africa/Dar_es_Salaam',
    'UA': {
        '01': 'Europe/Kiev',
        '02': 'Europe/Kiev',
        '03': 'Europe/Uzhgorod',
        '04': 'Europe/Zaporozhye',
        '05': 'Europe/Zaporozhye',
        '06': 'Europe/Uzhgorod',
        '07': 'Europe/Zaporozhye',
        '08': 'Europe/Simferopol',
        '10': 'Europe/Zaporozhye',
        '11': 'Europe/Simferopol',
        '13': 'Europe/Kiev',
        '14': 'Europe/Zaporozhye',
        '15': 'Europe/Uzhgorod',
        '16': 'Europe/Zaporozhye',
        '17': 'Europe/Simferopol',
        '18': 'Europe/Zaporozhye',
        '19': 'Europe/Kiev',
        '20': 'Europe/Simferopol',
        '21': 'Europe/Kiev',
        '22': 'Europe/Uzhgorod',
        '23': 'Europe/Kiev',
        '24': 'Europe/Uzhgorod',
        '25': 'Europe/Uzhgorod',
        '26': 'Europe/Zaporozhye',
        '27': 'Europe/Kiev'
    },
    'UG': 'Africa/Kampala',
    'US': {
        'AK': 'America/Anchorage',
        'AL': 'America/Chicago',
        'AR': 'America/Chicago',
        'AZ': 'America/Phoenix',
        'CA': 'America/Los_Angeles',
        'CO': 'America/Denver',
        'CT': 'America/New_York',
        'DC': 'America/New_York',
        'DE': 'America/New_York',
        'FL': 'America/New_York',
        'GA': 'America/New_York',
        'IA': 'America/Chicago',
        'ID': 'America/Denver',
        'IL': 'America/Chicago',
        'IN': 'America/Indianapolis',
        'KS': 'America/Chicago',
        'LA': 'America/Chicago',
        'MA': 'America/New_York',
        'MD': 'America/New_York',
        'ME': 'America/New_York',
        'MI': 'America/New_York',
        'MN': 'America/Chicago',
        'MO': 'America/Chicago',
        'MS': 'America/Chicago',
        'MT': 'America/Denver',
        'NC': 'America/New_York',
        'ND': 'America/Chicago',
        'NE': 'America/Chicago',
        'NJ': 'America/New_York',
        'NM': 'America/Denver',
        'NV': 'America/Los_Angeles',
        'NY': 'America/New_York',
        'OK': 'America/Chicago',
        'OR': 'America/Los_Angeles',
        'PA': 'America/New_York',
        'RI': 'America/New_York',
        'SC': 'America/New_York',
        'SD': 'America/Chicago',
        'TN': 'America/Chicago',
        'TX': 'America/Chicago',
        'UT': 'America/Denver',
        'VA': 'America/New_York',
        'VT': 'America/New_York',
        'WA': 'America/Los_Angeles',
        'WI': 'America/Chicago',
        'WY': 'America/Denver'
    },
    'UY': 'America/Montevideo',
    'UZ': {
        '01': 'Asia/Tashkent',
        '02': 'Asia/Samarkand',
        '03': 'Asia/Tashkent',
        '06': 'Asia/Tashkent',
        '07': 'Asia/Samarkand',
        '08': 'Asia/Samarkand',
        '09': 'Asia/Samarkand',
        '12': 'Asia/Samarkand',
        '13': 'Asia/Tashkent',
        '14': 'Asia/Tashkent'
    },
    'VA': 'Europe/Vatican',
    'VC': 'America/St_Vincent',
    'VE': 'America/Caracas',
    'VG': 'America/Tortola',
    'VI': 'America/St_Thomas',
    'VN': 'Asia/Phnom_Penh',
    'VU': 'Pacific/Efate',
    'WF': 'Pacific/Wallis',
    'WS': 'Pacific/Samoa',
    'YE': 'Asia/Aden',
    'YT': 'Indian/Mayotte',
    'YU': 'Europe/Belgrade',
    'ZA': 'Africa/Johannesburg',
    'ZM': 'Africa/Lusaka',
    'ZW': 'Africa/Harare'
 }


def time_zone_by_country_and_region(country_code, region_name=None):
    if country_code not in _country:
        return ''

    if not region_name or region_name == '00':
        region_name = None

    timezones = _country[country_code]
    if isinstance(timezones, str):
        return timezones

    if not region_name:
        return ''

    return timezones.get(region_name)


def ip2long(ip):
    """
    Wrapper function for IPv4 and IPv6 converters
    @param ip: IPv4 or IPv6 address
    @type ip: str
    """
    try:
        return int(binascii.hexlify(socket.inet_aton(ip)), 16)
    except socket.error:
        return int(binascii.hexlify(socket.inet_pton(socket.AF_INET6, ip)), 16)


STANDARD = STANDARD
MMAP_CACHE = MMAP_CACHE
MEMORY_CACHE = MEMORY_CACHE

ENCODING = ENCODING


class GeoIPError(Exception):
    pass


class GeoIPMetaclass(type):
    def __new__(cls, *args, **kwargs):
        """
        Singleton method to gets an instance without reparsing the db. Unique
        instances are instantiated based on the filename of the db. Flags are
        ignored for this, i.e. if you initialize one with STANDARD
        flag (default) and then try later to initialize with MEMORY_CACHE, it
        will still return the STANDARD one.
        """
        if not hasattr(cls, '_instances'):
            cls._instances = {}

        if len(args) > 0:
            filename = args[0]
        elif 'filename' in kwargs:
            filename = kwargs['filename']

        if filename not in cls._instances:
            cls._instances[filename] = type.__new__(cls, *args, **kwargs)

        return cls._instances[filename]


GeoIPBase = GeoIPMetaclass('GeoIPBase', (object,), {})


class GeoIP(GeoIPBase):
    def __init__(self, filename, flags=0):
        """
        Initialize the class.

        @param filename: Path to a geoip database.
        @type filename: str
        @param flags: Flags that affect how the database is processed.
            Currently supported flags are STANDARD (the default),
            MEMORY_CACHE (preload the whole file into memory) and
            MMAP_CACHE (access the file via mmap).
        @type flags: int
        """
        self._filename = filename
        self._flags = flags

        if self._flags & MMAP_CACHE:
            f = open(filename, 'rb')
            access = mmap.ACCESS_READ
            self._filehandle = mmap.mmap(f.fileno(), 0, access=access)
            f.close()

        elif self._flags & MEMORY_CACHE:
            f = open(filename, 'rb')
            self._memoryBuffer = f.read()
            iohandle = BytesIO if PY3 else StringIO
            self._filehandle = iohandle(self._memoryBuffer)
            f.close()

        else:
            self._filehandle = codecs.open(filename, 'rb', ENCODING)

        self._lock = Lock()
        self._setup_segments()

    def _setup_segments(self):
        """
        Parses the database file to determine what kind of database is
        being used and setup segment sizes and start points that will
        be used by the seek*() methods later.

        Supported databases:

        * COUNTRY_EDITION
        * COUNTRY_EDITION_V6
        * REGION_EDITION_REV0
        * REGION_EDITION_REV1
        * CITY_EDITION_REV0
        * CITY_EDITION_REV1
        * CITY_EDITION_REV1_V6
        * ORG_EDITION
        * ISP_EDITION
        * ASNUM_EDITION
        * ASNUM_EDITION_V6

        """
        self._databaseType = COUNTRY_EDITION
        self._recordLength = STANDARD_RECORD_LENGTH
        self._databaseSegments = COUNTRY_BEGIN

        self._lock.acquire()
        filepos = self._filehandle.tell()
        self._filehandle.seek(-3, os.SEEK_END)

        for i in range(STRUCTURE_INFO_MAX_SIZE):
            chars = chr(255) * 3
            delim = self._filehandle.read(3)

            if PY3 and type(delim) is bytes:
                delim = delim.decode(ENCODING)

            if PY2:
                chars = chars.decode(ENCODING)
                if type(delim) is str:
                    delim = delim.decode(ENCODING)

            if delim == chars:
                byte = self._filehandle.read(1)
                self._databaseType = ord(byte)

                # Compatibility with databases from April 2003 and earlier
                if (self._databaseType >= 106):
                    self._databaseType -= 105

                if self._databaseType == REGION_EDITION_REV0:
                    self._databaseSegments = STATE_BEGIN_REV0

                elif self._databaseType == REGION_EDITION_REV1:
                    self._databaseSegments = STATE_BEGIN_REV1

                elif self._databaseType in (CITY_EDITION_REV0,
                                            CITY_EDITION_REV1,
                                            CITY_EDITION_REV1_V6,
                                            ORG_EDITION,
                                            ISP_EDITION,
                                            ASNUM_EDITION,
                                            ASNUM_EDITION_V6):
                    self._databaseSegments = 0
                    buf = self._filehandle.read(SEGMENT_RECORD_LENGTH)

                    if PY3 and type(buf) is bytes:
                        buf = buf.decode(ENCODING)

                    for j in range(SEGMENT_RECORD_LENGTH):
                        self._databaseSegments += (ord(buf[j]) << (j * 8))

                    LONG_RECORDS = (ORG_EDITION, ISP_EDITION)
                    if self._databaseType in LONG_RECORDS:
                        self._recordLength = ORG_RECORD_LENGTH
                break
            else:
                self._filehandle.seek(-4, os.SEEK_CUR)

        self._filehandle.seek(filepos, os.SEEK_SET)
        self._lock.release()

    def _seek_country(self, ipnum):
        """
        Using the record length and appropriate start points, seek to the
        country that corresponds to the converted IP address integer.

        @param ipnum: result of ip2long conversion
        @type ipnum: int
        @return: offset of start of record
        @rtype: int
        """
        try:
            offset = 0
            seek_depth = 127 if len(str(ipnum)) > 10 else 31

            for depth in range(seek_depth, -1, -1):
                if self._flags & MEMORY_CACHE:
                    startIndex = 2 * self._recordLength * offset
                    endIndex = startIndex + (2 * self._recordLength)
                    buf = self._memoryBuffer[startIndex:endIndex]
                else:
                    startIndex = 2 * self._recordLength * offset
                    readLength = 2 * self._recordLength
                    self._lock.acquire()
                    self._filehandle.seek(startIndex, os.SEEK_SET)
                    buf = self._filehandle.read(readLength)
                    self._lock.release()

                if PY3 and type(buf) is bytes:
                    buf = buf.decode(ENCODING)

                x = [0, 0]
                for i in range(2):
                    for j in range(self._recordLength):
                        byte = buf[self._recordLength * i + j]
                        x[i] += ord(byte) << (j * 8)
                if ipnum & (1 << depth):
                    if x[1] >= self._databaseSegments:
                        return x[1]
                    offset = x[1]
                else:
                    if x[0] >= self._databaseSegments:
                        return x[0]
                    offset = x[0]
        except:
            pass

        raise GeoIPError('Corrupt database')

    def _get_org(self, ipnum):
        """
        Seek and return organization or ISP name for ipnum.
        @param ipnum: Converted IP address
        @type ipnum: int
        @return: org/isp name
        @rtype: str
        """
        seek_org = self._seek_country(ipnum)
        if seek_org == self._databaseSegments:
            return None

        read_length = (2 * self._recordLength - 1) * self._databaseSegments
        self._lock.acquire()
        self._filehandle.seek(seek_org + read_length, os.SEEK_SET)
        buf = self._filehandle.read(MAX_ORG_RECORD_LENGTH)
        self._lock.release()

        if PY3 and type(buf) is bytes:
            buf = buf.decode(ENCODING)

        return buf[:buf.index(chr(0))]

    def _get_region(self, ipnum):
        """
        Seek and return the region info (dict containing country_code
        and region_name).

        @param ipnum: Converted IP address
        @type ipnum: int
        @return: dict containing country_code and region_name
        @rtype: dict
        """
        region = ''
        country_code = ''
        seek_country = self._seek_country(ipnum)

        def get_region_name(offset):
            region1 = chr(offset // 26 + 65)
            region2 = chr(offset % 26 + 65)
            return ''.join([region1, region2])

        if self._databaseType == REGION_EDITION_REV0:
            seek_region = seek_country - STATE_BEGIN_REV0
            if seek_region >= 1000:
                country_code = 'US'
                region = get_region_name(seek_region - 1000)
            else:
                country_code = COUNTRY_CODES[seek_region]
        elif self._databaseType == REGION_EDITION_REV1:
            seek_region = seek_country - STATE_BEGIN_REV1
            if seek_region < US_OFFSET:
                pass
            elif seek_region < CANADA_OFFSET:
                country_code = 'US'
                region = get_region_name(seek_region - US_OFFSET)
            elif seek_region < WORLD_OFFSET:
                country_code = 'CA'
                region = get_region_name(seek_region - CANADA_OFFSET)
            else:
                index = (seek_region - WORLD_OFFSET) // FIPS_RANGE
                if index in COUNTRY_CODES:
                    country_code = COUNTRY_CODES[index]
        elif self._databaseType in CITY_EDITIONS:
            rec = self._get_record(ipnum)
            region = rec.get('region_name', '')
            country_code = rec.get('country_code', '')

        return {'country_code': country_code, 'region_name': region}

    def _get_record(self, ipnum):
        """
        Populate location dict for converted IP.

        @param ipnum: Converted IP address
        @type ipnum: int
        @return: dict with country_code, country_code3, country_name,
            region, city, postal_code, latitude, longitude,
            dma_code, metro_code, area_code, region_name, time_zone
        @rtype: dict
        """
        seek_country = self._seek_country(ipnum)
        if seek_country == self._databaseSegments:
            return {}

        read_length = (2 * self._recordLength - 1) * self._databaseSegments
        self._lock.acquire()
        self._filehandle.seek(seek_country + read_length, os.SEEK_SET)
        buf = self._filehandle.read(FULL_RECORD_LENGTH)
        self._lock.release()

        if PY3 and type(buf) is bytes:
            buf = buf.decode(ENCODING)

        record = {
            'dma_code': 0,
            'area_code': 0,
            'metro_code': '',
            'postal_code': ''
        }

        latitude = 0
        longitude = 0
        buf_pos = 0

        # Get country
        char = ord(buf[buf_pos])
        record['country_code'] = COUNTRY_CODES[char]
        record['country_code3'] = COUNTRY_CODES3[char]
        record['country_name'] = COUNTRY_NAMES[char]
        record['continent'] = CONTINENT_NAMES[char]

        buf_pos += 1

        def get_data(buf, buf_pos):
            offset = buf_pos
            char = ord(buf[offset])
            while (char != 0):
                offset += 1
                char = ord(buf[offset])
            if offset > buf_pos:
                return (offset, buf[buf_pos:offset])
            return (offset, '')

        offset, record['region_name'] = get_data(buf, buf_pos)
        offset, record['city'] = get_data(buf, offset + 1)
        offset, record['postal_code'] = get_data(buf, offset + 1)
        buf_pos = offset + 1

        for j in range(3):
            char = ord(buf[buf_pos])
            buf_pos += 1
            latitude += (char << (j * 8))

        for j in range(3):
            char = ord(buf[buf_pos])
            buf_pos += 1
            longitude += (char << (j * 8))

        record['latitude'] = (latitude / 10000.0) - 180.0
        record['longitude'] = (longitude / 10000.0) - 180.0

        if self._databaseType in (CITY_EDITION_REV1, CITY_EDITION_REV1_V6):
            dmaarea_combo = 0
            if record['country_code'] == 'US':
                for j in range(3):
                    char = ord(buf[buf_pos])
                    dmaarea_combo += (char << (j * 8))
                    buf_pos += 1

                record['dma_code'] = int(math.floor(dmaarea_combo / 1000))
                record['area_code'] = dmaarea_combo % 1000

        record['metro_code'] = DMA_MAP.get(record['dma_code'])
        params = (record['country_code'], record['region_name'])
        record['time_zone'] = time_zone_by_country_and_region(*params)

        return record

    def _gethostbyname(self, hostname):
        if self._databaseType in IPV6_EDITIONS:
            try:
                response = socket.getaddrinfo(hostname, 0, socket.AF_INET6)
                family, socktype, proto, canonname, sockaddr = response[0]
                address, port, flow, scope = sockaddr
                return address
            except socket.gaierror:
                return ''
        else:
            return socket.gethostbyname(hostname)

    def id_by_addr(self, addr):
        """
        Get the country index.
        Looks up the index for the country which is the key for
        the code and name.

        @param addr: The IP address
        @type addr: str
        @return: network byte order 32-bit integer
        @rtype: int
        """
        ipnum = ip2long(addr)
        if not ipnum:
            raise ValueError("Invalid IP address: %s" % addr)

        COUNTY_EDITIONS = (COUNTRY_EDITION, COUNTRY_EDITION_V6)
        if self._databaseType not in COUNTY_EDITIONS:
            message = 'Invalid database type, expected Country'
            raise GeoIPError(message)

        return self._seek_country(ipnum) - COUNTRY_BEGIN

    def country_code_by_addr(self, addr):
        """
        Returns 2-letter country code (e.g. 'US') for specified IP address.
        Use this method if you have a Country, Region, or City database.

        @param addr: IP address
        @type addr: str
        @return: 2-letter country code
        @rtype: str
        """
        try:
            VALID_EDITIONS = (COUNTRY_EDITION, COUNTRY_EDITION_V6)
            if self._databaseType in VALID_EDITIONS:
                ipv = 6 if addr.find(':') >= 0 else 4

                if ipv == 4 and self._databaseType != COUNTRY_EDITION:
                    message = 'Invalid database type; expected IPv6 address'
                    raise ValueError(message)
                if ipv == 6 and self._databaseType != COUNTRY_EDITION_V6:
                    message = 'Invalid database type; expected IPv4 address'
                    raise ValueError(message)

                country_id = self.id_by_addr(addr)
                return COUNTRY_CODES[country_id]
            elif self._databaseType in REGION_CITY_EDITIONS:
                return self.region_by_addr(addr).get('country_code')

            message = 'Invalid database type, expected Country, City or Region'
            raise GeoIPError(message)
        except ValueError:
            raise GeoIPError('Failed to lookup address %s' % addr)

    def country_code_by_name(self, hostname):
        """
        Returns 2-letter country code (e.g. 'US') for specified hostname.
        Use this method if you have a Country, Region, or City database.

        @param hostname: Hostname
        @type hostname: str
        @return: 2-letter country code
        @rtype: str
        """
        addr = self._gethostbyname(hostname)
        return self.country_code_by_addr(addr)

    def country_name_by_addr(self, addr):
        """
        Returns full country name for specified IP address.
        Use this method if you have a Country or City database.

        @param addr: IP address
        @type addr: str
        @return: country name
        @rtype: str
        """
        try:
            VALID_EDITIONS = (COUNTRY_EDITION, COUNTRY_EDITION_V6)
            if self._databaseType in VALID_EDITIONS:
                country_id = self.id_by_addr(addr)
                return COUNTRY_NAMES[country_id]
            elif self._databaseType in CITY_EDITIONS:
                return self.record_by_addr(addr).get('country_name')
            else:
                message = 'Invalid database type, expected Country or City'
                raise GeoIPError(message)
        except ValueError:
            raise GeoIPError('Failed to lookup address %s' % addr)

    def country_name_by_name(self, hostname):
        """
        Returns full country name for specified hostname.
        Use this method if you have a Country database.

        @param hostname: Hostname
        @type hostname: str
        @return: country name
        @rtype: str
        """
        addr = self._gethostbyname(hostname)
        return self.country_name_by_addr(addr)

    def org_by_addr(self, addr):
        """
        Lookup Organization, ISP or ASNum for given IP address.
        Use this method if you have an Organization, ISP or ASNum database.

        @param addr: IP address
        @type addr: str
        @return: organization or ISP name
        @rtype: str
        """
        try:
            ipnum = ip2long(addr)
            if not ipnum:
                raise ValueError('Invalid IP address')

            valid = (ORG_EDITION, ISP_EDITION, ASNUM_EDITION, ASNUM_EDITION_V6)
            if self._databaseType not in valid:
                message = 'Invalid database type, expected Org, ISP or ASNum'
                raise GeoIPError(message)

            return self._get_org(ipnum)
        except ValueError:
            raise GeoIPError('Failed to lookup address %s' % addr)

    def org_by_name(self, hostname):
        """
        Lookup the organization (or ISP) for hostname.
        Use this method if you have an Organization/ISP database.

        @param hostname: Hostname
        @type hostname: str
        @return: Organization or ISP name
        @rtype: str
        """
        addr = self._gethostbyname(hostname)
        return self.org_by_addr(addr)

    def record_by_addr(self, addr):
        """
        Look up the record for a given IP address.
        Use this method if you have a City database.

        @param addr: IP address
        @type addr: str
        @return: Dictionary with country_code, country_code3, country_name,
            region, city, postal_code, latitude, longitude, dma_code,
            metro_code, area_code, region_name, time_zone
        @rtype: dict
        """
        try:
            ipnum = ip2long(addr)
            if not ipnum:
                raise ValueError('Invalid IP address')

            if self._databaseType not in CITY_EDITIONS:
                message = 'Invalid database type, expected City'
                raise GeoIPError(message)

            return self._get_record(ipnum)
        except ValueError:
            raise GeoIPError('Failed to lookup address %s' % addr)

    def record_by_name(self, hostname):
        """
        Look up the record for a given hostname.
        Use this method if you have a City database.

        @param hostname: Hostname
        @type hostname: str
        @return: Dictionary with country_code, country_code3, country_name,
            region, city, postal_code, latitude, longitude, dma_code,
            metro_code, area_code, region_name, time_zone
        @rtype: dict
        """
        addr = self._gethostbyname(hostname)
        return self.record_by_addr(addr)

    def region_by_addr(self, addr):
        """
        Lookup the region for given IP address.
        Use this method if you have a Region database.

        @param addr: IP address
        @type addr: str
        @return: Dictionary containing country_code, region and region_name
        @rtype: dict
        """
        try:
            ipnum = ip2long(addr)
            if not ipnum:
                raise ValueError('Invalid IP address')

            if self._databaseType not in REGION_CITY_EDITIONS:
                message = 'Invalid database type, expected Region or City'
                raise GeoIPError(message)

            return self._get_region(ipnum)
        except ValueError:
            raise GeoIPError('Failed to lookup address %s' % addr)

    def region_by_name(self, hostname):
        """
        Lookup the region for given hostname.
        Use this method if you have a Region database.

        @param hostname: Hostname
        @type hostname: str
        @return: Dictionary containing country_code, region, and region_name
        @rtype: dict
        """
        addr = self._gethostbyname(hostname)
        return self.region_by_addr(addr)

    def time_zone_by_addr(self, addr):
        """
        Look up the time zone for a given IP address.
        Use this method if you have a Region or City database.

        @param addr: IP address
        @type addr: str
        @return: Time zone
        @rtype: str
        """
        try:
            ipnum = ip2long(addr)
            if not ipnum:
                raise ValueError('Invalid IP address')

            if self._databaseType not in CITY_EDITIONS:
                message = 'Invalid database type, expected City'
                raise GeoIPError(message)

            return self._get_record(ipnum).get('time_zone')
        except ValueError:
            raise GeoIPError('Failed to lookup address %s' % addr)

    def time_zone_by_name(self, hostname):
        """
        Look up the time zone for a given hostname.
        Use this method if you have a Region or City database.

        @param hostname: Hostname
        @type hostname: str
        @return: Time zone
        @rtype: str
        """
        addr = self._gethostbyname(hostname)
        return self.time_zone_by_addr(addr)
