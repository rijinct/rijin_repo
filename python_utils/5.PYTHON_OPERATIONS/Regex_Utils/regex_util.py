import re

string = '''
cts.andQuery([\n    cts.collectionQuery('child_consent_routine_data_participation_and_routi'),\n    cts.jsonPropertyRangeQuery(\"load_date\", \">\", new Date(Date.now() - 86400 * 1000).toISOString())\n  ])
'''
pattern = r"(?i)[^.]*\bcollectionQuery\b[^)]*(\('[^']*)"

result = re.search(pattern, string)
print(result.group().replace('collectionQuery(\'',''))