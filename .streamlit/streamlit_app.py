from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st

from gsodpy.constants import SUPPORT_DIR
from gsodpy.output import GetOneStation
from gsodpy.isdhistory import ISDHistory

EPHISTORY_PATH = Path(__file__).resolve().parent / "ep_weather_stations.xlsx"

def read_isd_history():
    # This will download or update the isd-history.csv as needed
    isd = ISDHistory()

    return isd.df


def read_ep_ws():
    df_ep = pd.read_excel(EPHISTORY_PATH)
    # same format than df_isd
    df_ep.columns = ["idx", "CTRY", "STATE", "STATION NAME"]
    return df_ep


def change_status():
    if "downloaded" not in st.session_state.keys():
        st.session_state["downloaded"] = True
    else:
        st.write(st.session_state["downloaded"])
        if not st.session_state["downloaded"]:
            st.session_state["downloaded"] = True
        else:
            st.session_state["downloaded"] = False


def set_to_false():
    st.session_state["downloaded"] = False


st.set_page_config(layout="centered")

st.title("Download Temperature Data")

type_of_file = st.selectbox(
    "Select the type of temperature data",
    ("Historical Temperatures", "TMY"),
    on_change=set_to_false,
).replace("Historical Temperatures", "historical")

type_of_output = st.selectbox(
    "Select the output file format", ("CSV", "XLSX"), on_change=set_to_false
)


if type_of_file == "historical":

    st.markdown("### Select time window")
    col1, col2 = st.columns(2)

    with col1:
        start_year = int(
            st.selectbox(
                "Select the start year",
                (str(i) for i in range(2010, 2023)),
                on_change=set_to_false,
            )
        )
    with col2:
        end_year = int(
            st.selectbox(
                "Select the end year",
                (str(i) for i in range(2010, 2023)),
                12,
                on_change=set_to_false,
            )
        )

    df_dropdown = read_isd_history()


else:

    df_dropdown = read_ep_ws()

st.markdown("### Select the weather station")

if type_of_file == "historical":
    lat_long = st.checkbox("I prefer to enter the longitude and latitude")

    if lat_long:
        col1, col2 = st.columns(2)

        with col1:
            lat = st.number_input(
                "Latitude",
                min_value=float("-90"),
                max_value=90.0,
                value=0.0,
                step=1.0,
                on_change=set_to_false,
            )
        with col2:
            long = st.number_input(
                "Longitude",
                min_value=float("-180"),
                max_value=180.0,
                value=0.0,
                step=1.0,
                on_change=set_to_false,
            )
        ws = None
        state = None
        country = None

    if not lat_long:
        lat = np.nan
        long = np.nan


else:
    lat_long = False
    lat = np.nan
    long = np.nan
    start_year = 2010
    end_year = 2020

if not lat_long:

    col1, col2 = st.columns(2)

    with col1:
        country = st.selectbox(
            "Country code",
            (df_dropdown["CTRY"].fillna("N/A").sort_values().unique()),
            on_change=set_to_false,
        ).replace("N/A", "")
    with col2:
        if country == "":
            list_states = ["N/A"]
        else:
            list_states = (
                df_dropdown[df_dropdown.CTRY == country]["STATE"]
                .fillna("N/A")
                .sort_values()
                .unique()
            )
        state = st.selectbox(
            "State", list_states, on_change=set_to_false
        ).replace("N/A", "")

    if state == "":
        state = None

    if state == None:
        list_ws = (
            df_dropdown[(df_dropdown.CTRY == country)]["STATION NAME"]
            .sort_values()
            .unique()
        )
    else:
        list_ws = (
            df_dropdown[
                (df_dropdown.CTRY == country) & (df_dropdown.STATE == state)
            ]["STATION NAME"]
            .sort_values()
            .unique()
        )

    ws = st.selectbox("Weather Station", list_ws, on_change=set_to_false)


st.markdown("### Select CDD and HDD threshold")

col1, col2 = st.columns(2)

with col1:
    hdd = st.number_input(
        "HDD Threshold",
        min_value=30,
        max_value=100,
        value=65,
        step=1,
        on_change=set_to_false,
    )
with col2:
    cdd = st.number_input(
        "CDD Threshold",
        min_value=30,
        max_value=100,
        value=65,
        step=1,
        on_change=set_to_false,
    )

args = {
    "type_of_file": type_of_file,
    "type_of_output": type_of_output,
    "hdd_threshold": hdd,
    "cdd_threshold": cdd,
    "start_year": start_year,
    "end_year": end_year,
    "country": country,
    "state": state,
    "station_name": ws,
    "latitude": lat,
    "longitude": long,
}

st.markdown("### Download")

submit = st.button("Download from the API", on_click=change_status)

if submit:
    st.spinner("Downloading...")
    st.write("Downloading... Please wait")
    st.session_state["station"] = GetOneStation(args)
    st.session_state["station"].get_one_dataframe()
    st.balloons()

if "downloaded" in st.session_state.keys() and st.session_state["downloaded"]:
    st.success("Successfully downloaded")
    st.markdown("### Save on your computer")

    if type_of_file == "historical":
        freq = st.selectbox(
            "Select the frequency of the data", ["Hourly", "Daily", "Monthly"]
        )

        if freq == "Hourly":
            df = st.session_state["station"].df_hourly
        elif freq == "Daily":
            df = st.session_state["station"].df_daily
        else:
            df = st.session_state["station"].df_monthly

    else:
        df = st.session_state["station"].df_hourly
        freq = "Hourly"

    @st.cache
    def convert_df(df):
        # IMPORTANT: Cache the conversion to prevent computation on every rerun
        return df.to_csv()

    st.download_button(
        label="Save Temperature Files",
        data=convert_df(df),
        file_name=(
            st.session_state["station"].o.op_file_name
            + "-"
            + freq.lower()
            + "."
            + type_of_output.lower()
        ),
    )


else:
    st.write("Click download to start")
