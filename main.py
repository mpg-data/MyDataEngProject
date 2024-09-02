# %%
import data_sources as dsc
import yaml

from typing import LiteralString

source_name = "accidentalidad_baq"

with open('./config/api_sources.yml', 'r') as file:
    api_url: LiteralString = (yaml.safe_load(file)[source_name]["api_url"])


query_params = dsc.SoQLQueryParams(limit=None,
                                   offset=1000,
                                   # where="fecha_accidente <= '2024-05-01T:00:00:00.000'",
                                   order="fecha_accidente ASC")
source = dsc.SocrataAPISource(api_url)
source.get_data(query_params)
