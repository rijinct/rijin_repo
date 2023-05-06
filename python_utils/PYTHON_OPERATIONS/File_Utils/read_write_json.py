import json
import re
import argparse

def read_json(file):
    f = open(file)
    data = json.load(f)
    f.close()
    return data

def write_json(data, out_file):
    with open(out_file, "w") as outfile:
        json.dump(data, outfile, indent=4)

def get_instrument(string, interval):

    pattern = r"(?i)[^.]*\bcollectionQuery\b[^)]*(\('[^']*)"
    pattern2= r"(?i)[^.]*\bcollectionQuery\b[^)]*(\['[^\]]*)"
    result = re.search(pattern, string)
    if result is not None:
        return (result.group().replace('collectionQuery','').replace("'",'').replace("[",'').replace('(',''))
    else:
        return re.search(pattern2, string).group().replace('collectionQuery','').replace("'",'').replace("[",'').replace('(','')

def cmdline_args():
        # Make parser object
    p = argparse.ArgumentParser(description="""
                    Arguments to handle different mapping or matching functions
                    """,
                                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("-i", "--interval", required=True)
    p.add_argument("-s", "--map_file", required=True)
    return (p.parse_args())


if __name__ == "__main__":
    args = cmdline_args()
    json_file = args.map_file
    #data = read_json('data.json')
    data = read_json(json_file)
    day_sourceQuery = '''  cts.andQuery([\n    cts.collectionQuery('{}'),\n    cts.jsonPropertyRangeQuery(\"load_date\", \">\", new Date(Date.now() - 86400 * 1000).toISOString())\n  ])'''
    default_sourceQuery = '''
    cts.collectionQuery(['{}'])
    '''
    print(data["sourceQuery"])
    interval = args.interval
    instrument = get_instrument(data["sourceQuery"], interval)
    if interval == "daily":
        mod_sourceQuery = day_sourceQuery.format(instrument)
    else:
        mod_sourceQuery = default_sourceQuery.format(instrument)
    # data["sourceQuery"] = mod_sourceQuery
    # data["properties"]["visit_details"]["properties"]["source_document"]["sourcedFrom"] = "hubURI('Visit')"
    # print(mod_sourceQuery)
    # print(data["sourceQuery"])

    ##pregSumm changes
    data["uriExpression"] = "concat(\"/pregnancySummary/\",generate-id())"
    data["properties"]["status_update"]["properties"]["source_document"]["sourcedFrom"] = "concat(\"/pregnancySummary/\",generate-id(),\".json\")"
    write_json(data, json_file)
