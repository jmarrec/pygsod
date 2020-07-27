"""Module to launch in the docker."""
import json
import os
from gsodpy.output import GetOneStation
# from gsodpy.noaadata import NOAAData
# from gsodpy.utils import DataType
# from gsodpy.ish_full import parse_ish_file
# from gsodpy.tmy_download import TMY

if __name__ == '__main__':

    folder_input = 'input/'
    files = os.listdir(folder_input)

    list_json_files = []

    for f in files:
        fname, ext = os.path.splitext(f)
        if ext == ".json":
            list_json_files.append((fname, ext))

    for fname, ext in list_json_files:

        file_name_input = os.path.join(folder_input,
                                       '{}{}'.format(fname, ext))

        with open(file_name_input) as json_file:
            args = json.load(json_file)

        station = GetOneStation(args)
        station.run()

        # if args['type_of_file'] == 'historical':
        #     # download isd_full
        #     isd_full = NOAAData(data_type=DataType.isd_full)
        #     isd_full.set_years_range(
        #         start_year=args['start_year'], end_year=args['end_year'])
        #     isd_full.get_stations_from_user_input(args=args)
        #     isd_full.get_all_data()
        #     parse_ish_file(isd_full.ops_files)
        #
        #     # output files
        #     for op_file in isd_full.ops_files:
        #         o = Output(op_file, args['type_of_output'], args[
        #                    'hdd_threshold'], args['cdd_threshold'])
        #         o.output_files()
        # elif args['type_of_file'] == 'TMY':
        #     # download weather data from EP+ website
        #     tmy_data = TMY(args['country'], args[
        #                    'state'], args['station_name'])
        #
        #     o = Output(tmy_data.filename, args['type_of_output'], args[
        #         'hdd_threshold'], args['cdd_threshold'])
        #     o.output_files_from_epw()
