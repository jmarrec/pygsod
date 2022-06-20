import streamlit as st
import numpy as np
import pandas as pd
import datetime
from gsodpy.constants import SUPPORT_DIR
import os
from zipfile import ZipFile
from gsodpy.output import GetOneStation

ISDHISTORY_PATH = os.path.join(SUPPORT_DIR, 'isd-history.csv')

def read_isd_history():
    df_isd = pd.read_csv(ISDHISTORY_PATH, sep=",", parse_dates=[9, 10])
    return df_isd

st.set_page_config(layout="centered")


st.title("Download Temperature Data")

type_of_file = st.selectbox(
     'Select the type of temperature data',
     ('Historical Temperatures', 'TMY')
     ).replace(
        'Historical Temperatures', 'historical'
        )

type_of_output = st.selectbox(
     'Select the output file format',
     ('CSV', 'XLSX'))

col1, col2 = st.columns(2)

if type_of_file == "historical":

    with col1:
        start_year = int(st.selectbox(
            'Select the start year',
            (str(i) for i in range(2010, 2023))
        ))
    with col2:
        end_year = int(st.selectbox(
            'Select the end year',
            (str(i) for i in range(2010, 2023)),
            12
        ))


    st.markdown(
        "### Select the weather station"
    )

    df_isd = read_isd_history()

    col1, col2 = st.columns(2)

    with col1:
        country = st.selectbox(
            'Country code',
            (df_isd["CTRY"].fillna('N/A').sort_values().unique()),
            ).replace('N/A', '')
    with col2:
        if country == '':
            list_states = ['N/A']
        else :
            list_states = (
                df_isd[df_isd.CTRY == country]["STATE"].fillna('N/A').sort_values().unique()
                )
        state = st.selectbox(
            'State',
            list_states,
            ).replace('N/A', '')

    lat_long = st.checkbox('I prefer to enter the longitude and latitude')

    if lat_long:
        col1, col2 = st.columns(2)

        with col1:
            lat = st.number_input('Latitude', 
                                min_value=float('-90'),
                                max_value=90.0,
                                value=0.0,
                                step=1.0)
        with col2:
            long = st.number_input('Longitude', 
                                min_value=float('-180'),
                                max_value=180.0,
                                value=0.0,
                                step=1.0)

    if not lat_long:
        list_ws = df_isd[(df_isd.CTRY == country) & (df_isd.STATE == state)][
        'STATION NAME'
        ].sort_values().unique()

        ws = st.selectbox(
                'Weather Station',
                list_ws,
                )
        lat = np.nan
        long = np.nan

else:

    lat = np.nan
    long = np.nan
    start_year = 2010
    end_year = 2020
    country = 'USA'
    state = 'NY'

    st.markdown(
        "### Select the weather station"
    )

    st.markdown('Use this link https://energyplus.net/weather to browse your weather station')

    st.markdown('Enter the name of the weather station as it appears on the website.\
    No need to write AP XXX or (TMY).')

    ws = st.text_input('Weather Station', 'New York-LaGuardia')
     

args = {	
	"type_of_file": type_of_file,
	"type_of_output": type_of_output, 
	"hdd_threshold": 70, 
	"cdd_threshold": 80,

	"start_year": start_year,
	"end_year": end_year,

	"country": country,
	"state": state,
	"station_name": ws,

	"latitude": lat,
	"longitude": long
}
print(country, state, ws)
freq = st.selectbox(
                'Select the frequency of the data',
                ['Hourly', 'Daily', 'Monthly'],
                )

#hourly = st.checkbox('Hourly', value=True)
#daily = st.checkbox('Daily', value=True)
#monthly = st.checkbox('Monthly', value=True)


#form = st.form("test_form")
submit = st.button('Download from the API')

if submit:
    st.write('Downloading...')
    station = GetOneStation(args)
    station.run()
    downloaded = True
    if freq == 'Hourly':
        df= station.o.df_hourly
    elif freq == 'Daily':
        df = station.o.df_daily
    else :
        df = station.o.df_monthly
    st.balloons()
    st.success('Successfully downloaded')

    @st.cache
    def convert_df(df):
        # IMPORTANT: Cache the conversion to prevent computation on every rerun
        return df.to_csv()

    st.download_button(
        label="Save Temperature Files",
        data=convert_df(df),
        file_name=station.o.op_file_name + '-' + freq.lower() + '.' + type_of_output.lower()
        )


else:
    st.write('Click download to start')