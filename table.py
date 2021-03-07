import hashlib
import sqlite3 as sql
import time
from collections import defaultdict
from json import loads

import pandas as pd

from bd import Database
from get_data import get_data


class Table:

    def __init__(self):
        self.database = Database()
        self.regions_df = None
        self.countries_df = None
        self.table_df = None
        self.countries = []
        self.times = defaultdict(lambda: 0)
        self.languages = defaultdict(lambda: '')

    def build_data(self):
        self.get_regions()

        self.get_country_by_region()

        self.concat_coutries()

        self.set_rows_language()

        self.set_times()

        self.set_languages()

        self.build_table()


    def get_regions(self):
        """ Obtener todos los paises con su región
        """
        regions_str = get_data("regions")
        regions = loads(regions_str)
        self.regions_df = pd.DataFrame(regions)

    def get_country_by_region(self):
        """ Se obtienen regiones de los paises obtenidos, por cada una se consultan 
        los paises y se toma el primero y se guarda un dataframe en self.countries
        """
        if self.regions_df is None or self.regions_df.empty:
            return None
        region_list = self.regions_df["region"].value_counts().keys().to_list()
        for region in region_list:
            start_time = time.time()

            if not region: continue

            countries_str = get_data("countries", region=region)
            countries = loads(countries_str)
            self.countries.append(pd.DataFrame([countries[0]]))
            self.times[region] += time.time() - start_time

    def set_rows_language(self):
        """ Por cada país se consulta en 
        https://restcountries.eu/rest/v2/name/{country_code}?fields=languages
        sus lenguajes y se guardan en self.languages por región.
        
        Se acumula el tiempo que toma consultar el primer lenguaje de cada 
        país y guardar el nombre  en countries_df en self.times
        """
        if self.countries_df is None or self.countries_df.empty:
            return None
        columns = list(self.countries_df)
        for index, row in self.countries_df.iterrows():
            start_time = time.time()
            language_str = get_data("language", country_code=row['alpha2Code'])
            language = loads(language_str)
            if language:
                language = language[0]["languages"][0]["name"]
                language = hashlib.sha1(language.encode('UTF-8')).hexdigest()
                self.languages[row['region']] = language
            else:
                self.languages[row['region']] = ''
            self.times[row['region']] += time.time() - start_time

    def concat_coutries(self):
        """ Se une el dataframe de cada país en self.countries_df 
        (self.countries es una lista con dataframes)
            
        Se acumula el tiempo que toma consultar cada país en self.times
        """
        self.countries_df = pd.concat(self.countries)

    def start_database(self, name):
        self.database.open(name)

    def create_db_table(self):
        fields = [
            '`index` INTEGER PRIMARY KEY AUTOINCREMENT',
            '`region` TEXT',
            '`name` TEXT',
            '`language_sha1` TEXT',
            '`time` REAL',
        ]
        self.database.create_table('countries', fields)

    def insert_dataframe_db(self):
        self.database.datraframe_to_db(
            'database.db', 'countries', self.table_df)

    def generate_json_file(self):
        self.table_df.to_json(r'data.json', orient='records')

    def set_times(self):
        """ Se agrega el tiempo a cada fila en self.countries_df
        """
        self.countries_df['time'] = self.countries_df['region'].map(self.times)

    def set_languages(self):
        """ Se agrega el lenguaje encriptado con SHA1 a cada fila en self.countries_df
        """
        self.countries_df['language_sha1'] = self.countries_df['region'].map(
            self.languages)

    def build_table(self):
        """ Apartir de self.countries_df se arma un dataframe con los datos requeridos: 
        región, nombre del país, lenguaje y tiempo que toma armar cada fila en
        """
        self.table_df = self.countries_df[[
            'region', 'name', 'language_sha1', 'time']].copy()
        self.table_df.reset_index(drop=True, inplace=True)

    def show_times(self):
        """ Muestra el tiempo total, promedio, minimo y maximo con funciones de pandas
        """
        if self.table_df is None or self.table_df.empty:
            return None

        times_str = ""\
            "-------------- Tiempos --------------\n"\
            "Tiempo total: {total}\n"\
            "Tiempo promedio: {mean}\n"\
            "Tiempo minimo: {minimum}\n"\
            "Tiempo maximo: {maximum}".format(
            total=self.table_df['time'].sum(),
            mean=self.table_df['time'].mean(),
            minimum=self.table_df['time'].min(),
            maximum=self.table_df['time'].max()
        )
        print(times_str)
