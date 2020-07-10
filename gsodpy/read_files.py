
from gsodpy.epw_converter import clean_df, epw_convert
from gsodpy.constants import WEATHER_DIR
import json


def output_files(type_of_output, hdd_threshold, cdd_threshold)

	if type_of_output == 'EPW':
		# run epw_converter.py
	    for root, dirs, files in os.walk(WEATHER_DIR + '/isd_full'):
	        for file in files:
	            if file.endswith("xlsx"):
	                df_path = os.path.join(root, file)
	                df = pd.read_excel(df_path, index_col=0)
	                df = clean_df(df, file)
	                epw_convert(df, root, file)

	if type_of_output == 'CSV':
		# grouping by daily and monthly
		# calculate hdd and cdd

	if type_of_output == 'JSON':
		# grouping by daily and monthly
		# calculate hdd and cdd


# class Parser(object):

# 	def __init__(self, file_name):

#         self.file_name = file_name
#         self.variables = self.read_variables()

#     def read_variables(self):


def read_json(data):

    with open(data) as json_file:
        args = json.load(json_file)

    assert isinstance(args, dict)

    if set(list(args.keys())).issubset(['type_of_output',
    									'hdd_threshold',
    									'cdd_threshold']):
        type_of_output = args['type_of_output']
        hdd_threshold = args['hdd_threshold']
        cdd_threshold = args['cdd_threshold']



    else:
        raise ValueError("The json file does not contain the right keys. \n"
                         "Expecting: 'type_of_output', 'hdd_threshold', \n"
                         "'cdd_threshold'")

    return type_of_output, hdd_threshold, cdd_threshold