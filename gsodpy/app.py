import streamlit as st
import numpy as np
import pandas as pd
import datetime
from gsodpy.constants import SUPPORT_DIR
import os

ISDHISTORY_PATH = os.path.join(SUPPORT_DIR, 'isd-history.csv')

def read_isd_history():
    df_isd = pd.read_csv(ISDHISTORY_PATH, sep=",", parse_dates=[9, 10])
    return df_isd

st.markdown(
    "# Download Temperature Data"
)

type_of_file = st.selectbox(
     'Select the type of temperature data',
     ('Historical Temperatures', 'TMY'))

type_of_output = st.selectbox(
     'Select the outfil file format',
     ('CSV', 'XLSX'))

if type_of_file == "Historical Temperatures":
    start_year = st.selectbox(
        'Select the start year',
        (str(i) for i in range(2010, 2023))
    )
    end_year = st.selectbox(
        'Select the end year',
        (str(i) for i in range(2010, 2023)),
        12
    )


st.write(
    "Select the weather station"
)

df_isd = read_isd_history()

country = st.selectbox(
     'Country code',
     (df_isd["CTRY"].sort_values().unique()),
     )

state = st.selectbox(
     'Country code',
     (df_isd[df_isd.CTRY == country]["STATE"].fillna('N/A').sort_values().unique()),
     ).replace('N/A', '')

