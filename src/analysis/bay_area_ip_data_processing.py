"""Process GeoLite2 IPv4 address csv databases into a search tree for
determining if an IP address is in the bay area.

The tree output is pickled into a file called "bay_area_ip_data.pickle".

To use (from ipython), ensure that you're in the directory with the GeoLite2
CSV files, then:
%run ip_data_processing.py
run()

Then move the output file in place of learn_storm/bay_area_ip_data.pickle in
webapp.

(The GeoLite2 data was created by MaxMind and is available at
http://dev.maxmind.com/geoip/geoip2/geolite2.)

"""

import cPickle
import csv
import itertools


AREA_CODES = set([209, 310, 408, 415, 510, 520, 530, 559, 562, 619,
                  650, 707, 714, 805, 831, 858, 916, 925, 949])


ZIP_CODES = set([
    94501, 94502, 94536, 94537, 94538, 94539, 94540, 94541, 94542, 94543,
    94544, 94545, 94546, 94550, 94551, 94552, 94555, 94557, 94560, 94566,
    94568, 94577, 94578, 94579, 94580, 94586, 94587, 94588, 94601, 94602,
    94603, 94604, 94605, 94606, 94607, 94608, 94609, 94610, 94611, 94612,
    94613, 94614, 94615, 94617, 94618, 94619, 94620, 94621, 94622, 94623,
    94624, 94625, 94649, 94659, 94660, 94661, 94662, 94666, 94701, 94702,
    94703, 94704, 94705, 94706, 94707, 94708, 94709, 94710, 94712, 94720,
    94505, 94506, 94507, 94509, 94511, 94513, 94514, 94516, 94517, 94518,
    94519, 94520, 94521, 94522, 94523, 94524, 94525, 94526, 94527, 94528,
    94529, 94530, 94531, 94547, 94548, 94549, 94553, 94556, 94561, 94563,
    94564, 94565, 94569, 94570, 94572, 94575, 94582, 94583, 94595, 94596,
    94597, 94598, 94801, 94802, 94803, 94804, 94805, 94806, 94807, 94808,
    94820, 94850, 94901, 94903, 94904, 94912, 94913, 94914, 94915, 94920,
    94924, 94925, 94929, 94930, 94933, 94937, 94938, 94939, 94940, 94941,
    94942, 94945, 94946, 94947, 94948, 94949, 94950, 94956, 94957, 94960,
    94963, 94964, 94965, 94966, 94970, 94971, 94973, 94974, 94976, 94977,
    94978, 94979, 94998, 94503, 94508, 94515, 94558, 94559, 94562, 94567,
    94573, 94574, 94576, 94581, 94599, 94101, 94102, 94103, 94104, 94105,
    94106, 94107, 94108, 94109, 94110, 94111, 94112, 94114, 94115, 94116,
    94117, 94118, 94119, 94120, 94121, 94122, 94123, 94124, 94125, 94126,
    94127, 94128, 94129, 94130, 94131, 94132, 94133, 94134, 94135, 94136,
    94137, 94138, 94139, 94140, 94141, 94142, 94143, 94144, 94145, 94146,
    94147, 94150, 94151, 94152, 94153, 94154, 94155, 94156, 94158, 94159,
    94160, 94161, 94162, 94163, 94164, 94171, 94172, 94175, 94177, 94188,
    94199, 94002, 94005, 94010, 94011, 94013, 94014, 94015, 94016, 94017,
    94018, 94019, 94020, 94021, 94025, 94026, 94027, 94028, 94030, 94037,
    94038, 94044, 94060, 94061, 94062, 94063, 94064, 94065, 94066, 94070,
    94074, 94080, 94083, 94401, 94402, 94403, 94404, 94497, 94022, 94023,
    94024, 94035, 94039, 94040, 94041, 94042, 94043, 94085, 94086, 94087,
    94088, 94089, 94301, 94302, 94303, 94304, 94305, 94306, 94309, 95002,
    95008, 95009, 95011, 95013, 95014, 95015, 95020, 95021, 95026, 95030,
    95031, 95032, 95033, 95035, 95036, 95037, 95038, 95042, 95044, 95046,
    95050, 95051, 95052, 95053, 95054, 95055, 95056, 95070, 95071, 95101,
    95103, 95106, 95108, 95109, 95110, 95111, 95112, 95113, 95115, 95116,
    95117, 95118, 95119, 95120, 95121, 95122, 95123, 95124, 95125, 95126,
    95127, 95128, 95129, 95130, 95131, 95132, 95133, 95134, 95135, 95136,
    95138, 95139, 95140, 95141, 95148, 95150, 95151, 95152, 95153, 95154,
    95155, 95156, 95157, 95158, 95159, 95160, 95161, 95164, 95170, 95172,
    95173, 95190, 95191, 95192, 95193, 95194, 95196, 85611, 85621, 85624,
    85628, 85640, 85645, 85646, 85648, 85662, 95001, 95003, 95005, 95006,
    95007, 95010, 95017, 95018, 95019, 95041, 95060, 95061, 95062, 95063,
    95064, 95065, 95066, 95067, 95073, 95076, 95077, 94510, 94512, 94533,
    94534, 94535, 94571, 94585, 94589, 94590, 94591, 94592, 95620, 95625,
    95687, 95688, 95696, 94922, 94923, 94926, 94927, 94928, 94931, 94951,
    94952, 94953, 94954, 94955, 94972, 94975, 94999, 95401, 95402, 95403,
    95404, 95405, 95406, 95407, 95409, 95412, 95416, 95419, 95421, 95425,
    95430, 95431, 95433, 95436, 95439, 95441, 95442, 95444, 95446, 95448,
    95450, 95452, 95462, 95465, 95471, 95472, 95473, 95476, 95480, 95486,
    95487, 95492, 95497])


CITY_NAMES = set([
    "Alameda", "Fremont", "Hayward", "Castro Valley",
    "Livermore", "Newark", "Pleasanton", "Dublin", "San Leandro",
    "San Lorenzo",
    "Sunol", "Union City", "Oakland", "Emeryville", "Piedmont", "Berkeley",
    "Albany", "Discovery Bay", "Danville",
    "Alamo", "Antioch", "Bethel Island", "Brentwood", "Byron", "Canyon",
    "Clayton", "Concord", "Pleasant Hill", "Crockett",
    "Diablo", "El Cerrito", "Hercules", "Knightsen", "Lafayette", "Martinez",
    "Moraga", "Oakley", "Orinda", "Pinole",
    "Pittsburg", "Port Costa", "Rodeo", "San Ramon", "Walnut Creek",
    "Richmond",
    "El Sobrante", "San Pablo", "San Rafael",
    "Greenbrae", "Kentfield", "Belvedere Tiburon", "Bolinas", "Corte Madera",
    "Dillon Beach", "Fairfax", "Forest Knolls",
    "Inverness", "Lagunitas", "Larkspur", "Marshall", "Mill Valley", "Novato",
    "Nicasio", "Olema", "Point Reyes Station",
    "Ross", "San Anselmo", "San Geronimo", "San Quentin", "Sausalito",
    "Stinson Beach", "Tomales", "Woodacre",
    "American Canyon", "Angwin", "Calistoga", "Napa", "Oakville",
    "Pope Valley",
    "Rutherford", "Saint Helena", "Deer Park",
    "Yountville", "San Francisco", "Belmont", "Brisbane", "Burlingame",
    "Daly City", "El Granada", "Half Moon Bay",
    "La Honda", "Loma Mar", "Menlo Park", "Atherton", "Portola Valley",
    "Millbrae", "Montara", "Moss Beach", "Pacifica",
    "Pescadero", "Redwood City", "San Bruno", "San Carlos", "San Gregorio",
    "South San Francisco", "San Mateo", "Los Altos",
    "Mountain View", "Sunnyvale", "Palo Alto", "Stanford", "Alviso",
    "Campbell",
    "Coyote", "Cupertino", "Gilroy",
    "Holy City", "Los Gatos", "Milpitas", "Morgan Hill", "New Almaden",
    "Redwood Estates", "San Martin", "Santa Clara",
    "Saratoga", "San Jose", "Mount Hamilton", "Elgin", "Nogales", "Patagonia",
    "Tumacacori", "Amado", "Tubac", "Rio Rico",
    "Aptos", "Ben Lomond", "Boulder Creek", "Brookdale", "Capitola",
    "Davenport",
    "Felton", "Freedom", "Mount Hermon",
    "Santa Cruz", "Scotts Valley", "Soquel", "Watsonville", "Benicia",
    "Birds Landing", "Fairfield", "Travis Afb",
    "Rio Vista", "Suisun City", "Vallejo", "Dixon", "Elmira", "Vacaville",
    "Bodega", "Bodega Bay", "Rohnert Park", "Cotati",
    "Penngrove", "Petaluma", "Valley Ford", "Santa Rosa", "Annapolis",
    "Boyes Hot Springs", "Camp Meeker", "Cazadero",
    "Cloverdale", "Duncans Mills", "Eldridge", "El Verano", "Forestville",
    "Fulton", "Geyserville", "Glen Ellen", "Graton",
    "Guerneville", "Healdsburg", "Jenner", "Kenwood", "Monte Rio",
    "Occidental", "Rio Nido", "Sebastopol", "Sonoma",
    "Stewarts Point", "Villa Grande", "Vineburg", "Windsor", "The Sea Ranch"])


RAW_LOCATIONS_INPUT_FN = "GeoLite2-City-Locations-en.csv"
RAW_SUBNET_INPUT_FN = "GeoLite2-City-Blocks-IPv4.csv"
OUTPUT_FN = "bay_area_ip_data.pickle"


def parse_location(loc_array):
    return {
        'id': loc_array[0],
        'country': loc_array[4],
        'state': loc_array[6],
        'city': loc_array[10],
        'area-code': loc_array[11],
    }


def parse_subnet_info(subnet_array):
    return {
        'subnet': subnet_array[0],
        'id': subnet_array[1],
        'postalcode': subnet_array[6],
        'latitude': subnet_array[7],
        'longitude': subnet_array[8],
    }


def get_us_subnet_data():
    with open(RAW_LOCATIONS_INPUT_FN) as f:
        reader = csv.reader(f)
        locations = {
            us_loc['id']: us_loc
            for us_loc in (loc for loc in map(parse_location, reader)
                           if loc['country'] == 'US')}

    with open(RAW_SUBNET_INPUT_FN) as f:
        reader = csv.reader(f)
        subnet_data = [
            dict(itertools.chain(
                locations[subnet['id']].iteritems(),
                subnet.iteritems()))
            for subnet in (sub for sub in map(parse_subnet_info, reader)
                           if sub['id'] in locations)
        ]
    return subnet_data


def dotted_ipv4_to_integer(ip_addr):
    addr_bytes = (int(b) for b in ip_addr.split('.'))
    return reduce(lambda a, (i, e): a + (2 ** (24 - 8 * i)) * e,
                  enumerate(addr_bytes),
                  0)


def ipv4_cidr_to_range(ip_cidr_string):
    """Map address/subnet (like 192.168.1.1/24) to [start, end) int range."""
    (start_addr, mask) = ip_cidr_string.split('/')
    addr = dotted_ipv4_to_integer(start_addr)
    range_size = 2 ** (32 - int(mask))
    return (addr, addr + range_size)


def process_subnet_data_into_ranges(subnet_data):
    for v in subnet_data:
        (start, end) = ipv4_cidr_to_range(v['subnet'])
        v['start'] = start
        v['end'] = end


def bay_area_filter_fn(item):
    try:
        return (item['state'] == 'CA' and
                (item['city'] in CITY_NAMES
                 # this inline hack for ignoring empty strings requires that
                 # 0 is not in the list of area or zip codes
                 or int(len(item['area-code'])
                        and item['area-code']) in AREA_CODES
                 or int(len(item['postalcode'])
                        and item['postalcode']) in ZIP_CODES))
    except Exception:
        # Strangely, there are some addresses in Maine that don't have a
        # standard US zipcode listed as the postal code (instead, something
        # like "E3L") These fail to parse to integers.  Ignore them.
        return False


def filter_bay_area_data(data):
    return (d for d in data
            if bay_area_filter_fn(d))


def make_leaves(data):
    return sorted(
        # Tuple representation is (start addr, end addr, left, right)
        ((d['start'], d['end'], None, None) for d in data),
        key=lambda elt: elt[0])


def chunk(an_iter, chunk_size):
    while True:
        # Might raise StopIteration, which is ok, since this means the chunk
        # iterator is done too.  Let it pass through.
        fst = an_iter.next()

        def grab_next():
            try:
                return an_iter.next()
            except StopIteration:
                return None

        rest = [grab_next() for i in xrange(chunk_size - 1)]

        yield tuple([fst] + rest)


def make_tree(current_tree_level):
    if len(current_tree_level) == 1:
        # Root node
        return current_tree_level[0]
    else:
        return make_tree(
            [(left[0],
              max(left[1], right and right[1]),
              left,
              right)
             for left, right in chunk(iter(current_tree_level), 2)])


def write_output(tree, fn):
    with open(fn, 'w') as f:
        cPickle.dump(tree, f, cPickle.HIGHEST_PROTOCOL)


def run():
    us_ip_subnets = get_us_subnet_data()
    process_subnet_data_into_ranges(us_ip_subnets)
    filtered_data = filter_bay_area_data(us_ip_subnets)
    leaves = make_leaves(filtered_data)
    tree = make_tree(leaves)
    write_output(tree, OUTPUT_FN)
