data_apis = {
    "regions": {
        "url": "https://restcountries-v1.p.rapidapi.com/all",
        "headers": {
            'x-rapidapi-key': "e2120effc4msh265ce2773d64c1dp1d71f2jsna101665a09f1",
            'x-rapidapi-host': "restcountries-v1.p.rapidapi.com"
        }
    },
    "countries": {
        "url": "https://restcountries.eu/rest/v2/region/{region}"
    },
    "language": {
        "url": "https://restcountries.eu/rest/v2/name/{country_code}?fields=languages"
    }
}
