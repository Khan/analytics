#!/usr/bin/python
"""Library for the user defined functions used in hive for Khan.
   Typically ths script will be called like the following:
   ka_udf.py <func_name> <extra args>
   Example:
       python ka_udf.py split topic_string_keys "<tab>" key,title 0
"""
import codecs
import json
import sys
sys.stdout = codecs.getwriter('utf8')(sys.stdout)


def split(split_field, delimiter, selected, 
            output_json=False, split_field_required=True):
    """Running split on the split_field from the json string.

       Split based on the split_field and delimiter from a json string from
       sys.stdin. Select extra json fields based on "selected" where fields are
       seprated by ",".  Output the original json string if output_json = True.

       If the split_field is not found in the json document, an error will be
       thrown if split_field_required==True, otherwise the line is skipped.
    """
    if delimiter == '<tab>':
        # Have to do this to get around hive oddness
        delimiter = '\t'
    for line in sys.stdin:
        line = line.strip()
        doc = json.loads(line)

        if split_field not in doc and not split_field_required:
            continue

        split_f = doc[split_field]
        exploded = split_f.split(delimiter)
        selected_fields = []
        selected_keys = selected.split(",")
        for key in selected_keys:
            if key in doc:
                data = doc[key]
                selected_fields.append(data)
            else:
                selected_fields.append("")
        if output_json:
            selected_fields.append(line)
        f_delim = '\t'
        selected_str = f_delim.join(selected_fields)
        for key in exploded:
            output = "%s\t%s\n" % (key, selected_str)
            sys.stdout.write(output)


def explode(key_fields, explode_field):
    """Running the explode function on the explode field.
       Example: key_fields='a,b' explode_field = 'c'
       input: {"a":"def", "b":"ghi", "c": [1,2,3]}
       output: 
            def\tghi\t1
            def\tghi\t2
            def\tghi\t3
    """
    for line in sys.stdin:
        line = line.strip()
        doc = json.loads(line)
        exploded = None
        if explode_field in doc:
            exploded = doc[explode_field]
        selected_keys = key_fields.split(",")
        selected_fields = []
        for key in selected_keys:
            if key in doc:
                data = str(doc[key])
                selected_fields.append(data)
            else:
                selected_fields.append("")
        f_delim = '\t'
        selected_str = f_delim.join(selected_fields)
        if not exploded:
            continue
        for value in exploded:
            output = "%s\t%s\n" % (selected_str, value)
            sys.stdout.write(output)


def rank(key_field_index, rank_field_index, reverse=True, delimiter="\t"):
    """This reducer takes lines of delimited values that must be already
    sorted by the values in column <key_field_index>. It ranks within each
    group by the values in column <rank_field_index>.  The output is the
    original lines with an additional column for the numerical in-group
    rank appended as the last column.  Note that the ranks start at 1 for the 
    top value.
    """
    def process_group(lines):
        lines.sort(key=lambda l: l[rank_field_index], reverse=reverse)
        for i, vals in enumerate(lines, start=1):
            vals.append(str(i))
            sys.stdout.write("\t".join(vals) + "\n")

    prev_key = None
    group = []
    for line in sys.stdin:
        line = line.strip().split(delimiter)
        
        key = line[key_field_index]
        if key != prev_key:
            process_group(group)
            group = []

        group.append(line)
        prev_key = key

    process_group(group)


def ip_to_country(ip_field_index, delimiter="\t"):
    """This reducer takes lines of delimited values with an ip address string
    in column <ip_field_index>.  It outputs the same lines while appending
    a new column that contains the country code, or "NULL" if the country
    can't be determined.

    NOTE: the hive caller must ADD FILE for both pygeoip.py and the database.
    """
    sys.path.append(".")
    import pygeoip
    
    geo_ip = pygeoip.Database('GeoIP.dat')

    for line in sys.stdin:
        line = line.strip().split(delimiter)
        ip = line[ip_field_index]
        try: 
            country = geo_ip.lookup(ip).country or "NULL"
        except:
            country = "NULL"
        line.append(country)
        sys.stdout.write("\t".join(line) + "\n")


def main():
    if len(sys.argv) <= 1:
        print >> sys.stderr, "Usage: ka_udf.py <func_name> <extra args>"
        print >> sys.stderr, "Please specify a function to use."
        exit(1)

    if sys.argv[1] == "split":
        split_usage_str = ("Usage: ka_udf.py split " +
            "<split_field> <delim> <extra_fields> <output_json: 0 or 1>"
            " [split_field_required: 0 or 1]")
        if len(sys.argv) == 6:
            split(sys.argv[2], sys.argv[3], sys.argv[4],
                    bool(int(sys.argv[5])))
        elif len(sys.argv) == 7:
            split(sys.argv[2], sys.argv[3], sys.argv[4],
                    bool(int(sys.argv[5])), bool(int(sys.argv[6])))
        else:
            print >> sys.stderr, split_usage_str
            exit(1)
        exit(0)

    if sys.argv[1] == "explode":
        explode_usage_str = ("Usage: ka_udf.py  exploded" +
            "<output_fields_seprated_by_comma> <explode_field>")
        if len(sys.argv) != 4:
            print >> sys.stderr, explode_usage_str
            exit(1)
        explode(sys.argv[2], sys.argv[3])
        exit(0)

    if sys.argv[1] == "rank":
        rank_usage_str = ("Usage: ka_udf.py rank " +
                "<key_field_index> <rank_field_index> <ASC|DESC>")
        if len(sys.argv) != 5:
            print >> sys.stderr, rank_usage_str
            exit(1)
        rank(int(sys.argv[2]), int(sys.argv[3]), sys.argv[4] == "DESC")
        exit(0)

    if sys.argv[1] == "ip_to_country":
        ip_usage_str = "Usage: ka_udf.py ip_to_country <ip_field_index>"
        if len(sys.argv) != 3:
            print >> sys.stderr, ip_usage_str
            exit(1)
        ip_to_country(int(sys.argv[2]))
        exit(0)

    #Unknown 
    print >> sys.stderr, "Unknown function %s. Exiting!" % sys.argv[1]
    exit(1)


if __name__ == '__main__':
    main()
