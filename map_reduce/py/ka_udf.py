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


def split(split_field, delimiter, selected, output_json=False):
    """Running split on the split_field from the json string.

       Split based on the split_field and delimiter from a json string from
       sys.stdin. Select extra json fields based on "selected" where fields are
       seprated by ",".  Output the original json string if output_json = True.
    """
    if delimiter == '<tab>':
        # Have to do this to get around hive oddness
        delimiter = '\t'
    for line in sys.stdin:
        line = line.strip()
        doc = json.loads(line)
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


def main():
    if len(sys.argv) <= 1:
        print >> sys.stderr, "Usage: ka_udf.py <func_name> <extra args>"
        print >> sys.stderr, "Please specify a function to use."
        exit(1)
    if sys.argv[1] == "split":
        split_usage_str = ("Usage: ka_udf.py split " +
            "<split_field> <delim> <extra_fields> <output_json>")
        if len(sys.argv) != 6:
            print >> sys.stderr, split_usage_str
            exit(1)
        split(sys.argv[2], sys.argv[3], sys.argv[4], int(sys.argv[5]))
    if sys.argv[1] == "explode":
        explode_usage_str = ("Usage: ka_udf.py  exploded" +
            "<output_fields_seprated_by_comma> <explode_field>")
        if len(sys.argv) != 4:
            print >> sys.stderr, explode_usage_str
            exit(1)
        explode(sys.argv[2], sys.argv[3])
    else:
        print >> sys.stderr, "Unknown function %s. Exiting!" % sys.argv[1]
        exit(1)


if __name__ == '__main__':
    main()
