import time

import requests
import json
import os
import pandas as pd
from bs4 import BeautifulSoup
from airflow.models import TaskInstance


def get_wikipedia_page(url: str) -> str:
    result = {}
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.text
    except Exception as ex:
        print(f"An error occurred: {ex}")


def extract_data_from_tables(rows: list, metric_name: str) -> list:
    result = []
    for row in rows:
        tds = row.find_all(name="td")
        if len(tds) == 4:
            values = {
                "municipality": tds[1].text.strip(),
                "province": tds[2].text.strip(),
                metric_name: float(tds[3].text.strip().replace(",",""))
            }
            result.append(values)
    return result


def get_wikipedia_data(html: str, ti: TaskInstance) -> str:
    soup = BeautifulSoup(html, features='html.parser')
    tables = soup.find_all(name='table', class_="wikitable sortable")

    # Extract Population data
    table_rows = tables[0].find_all(name="tr")
    population_data = extract_data_from_tables(table_rows, "population")
    population_json = json.dumps(population_data)
    ti.xcom_push(key='population_data', value=population_json)

    # Extract Population Density
    table_rows = tables[1].find_all(name="tr")
    population_density_data = extract_data_from_tables(table_rows, "population_density")
    population_density_json = json.dumps(population_density_data)
    ti.xcom_push(key='population_density_data', value=population_density_json)

    # Extract Surface Area
    table_rows = tables[1].find_all(name="tr")
    surface_area_data = extract_data_from_tables(table_rows, "surface")
    surface_area_json = json.dumps(surface_area_data)
    ti.xcom_push(key='surface_area_data', value=surface_area_json)


def extract_wikipedia_data(**kwargs):
    url = kwargs['url']
    ti = kwargs['ti']
    html = get_wikipedia_page(url)
    get_wikipedia_data(html, ti)
    return "OK"



