import json
from typing import Dict
import pandas as pd


src = 'MetaData.csv'
df = pd.read_csv(src)

items = []

def swap_keyvalue(_dict: Dict) -> Dict:
	return { value: key for key, value in _dict.items() }

def to_json(_dict: Dict):
	return json.dumps([entry.__dict__ for entry in _dict])

class TxItem:
	"""Hold the structure of translation items"""
	def __init__(self, fielname, filetype, stationname, stationid, columnnames):
		self.filename = filename
		self.filetype = filetype
		self.stationname = stationname
		self.stationid = stationid
		self.columnnames = columnnames

for index, row in df.iterrows():
	all_records = {}

	for columnname, cellvalue in row.iteritems():
		if str(cellvalue) != 'nan':
			fromcol = cellvalue
			targetcolumn = columnname
			all_records[fromcol] = targetcolumn

	if bool(all_records):
		columnnames_records = swap_keyvalue(all_records)
		columnnames_records.pop('SourceFilename')
		columnnames_records.pop('stationname')
		columnnames_records.pop('stationId')
		columnnames_records.pop('Filetype')

		columnnames = swap_keyvalue(columnnames_records)

		recordes = swap_keyvalue(all_records)

		item = TxItem(
			records['SourceFilename'],
			records['Filetype'],
			records['stationname'],
			int(records['stationId']),
			columnnames
		)
		items.append(item)

jsonStr = to_json(items)
output_json = json.loads(jsonStr)
translations = {'translations': output_json}

with open('MetaData.json', 'w') as outfile:
	json.dump(translations, outfile, indent=4)