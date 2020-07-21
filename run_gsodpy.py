"""Module to launch in the docker."""
import json
import os

if __name__ == '__main__':

	folder_input = 'input/'
	files = os.listdir(folder_input)

    list_json_files = []
    for f in files:
        fname, ext = os.path.splitext(f)
        if ext == ".json":
            list_json_files.append((fname, ext))

	file_name_input = os.path.join(folder_input,
                                   '{}{}'.format(fname, ext))

    with open(file_name_input) as json_file:
    	args = json.load(json_file)

    print(args)

