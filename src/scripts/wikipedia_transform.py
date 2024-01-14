import json

import pandas as pd
from geopy.geocoders import Nominatim


def get_lat_lon(municipality: str, province: str) -> tuple:
    latitude = None
    longitude = None

    geolocator = Nominatim(user_agent="geoapiEnrichment")
    location = geolocator.geocode(f"{municipality}, {province}")

    if location:
        latitude = location.latitude
        longitude = location.longitude
    return latitude, longitude


def transform_data(**kwargs):
    ti = kwargs['ti']
    population_data_json = ti.xcom_pull(key='population_data', task_ids='extract_data')
    population_data = json.loads(population_data_json)
    population_data_df = pd.DataFrame(population_data)

    population_density_data_json = ti.xcom_pull(key='population_density_data', task_ids='extract_data')
    population_density_data = json.loads(population_density_data_json)
    population_density_data_df = pd.DataFrame(population_density_data)

    surface_area_data_json = ti.xcom_pull(key='surface_area_data', task_ids='extract_data')
    surface_area_data = json.loads(surface_area_data_json)
    surface_area_data_df = pd.DataFrame(surface_area_data)

    df = population_data_df.merge(population_density_data_df, on=['municipality', 'province'])
    df = df.merge(surface_area_data_df, on=['municipality', 'province'])
    df["coordinates"] = df.apply(lambda x: get_lat_lon(x['municipality'], x['province']), axis=1)
    ti.xcom_push(key='data', value=df.to_csv(index=False))

    return "OK"
