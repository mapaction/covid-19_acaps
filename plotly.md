---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.2'
      jupytext_version: 1.4.2
  kernelspec:
    display_name: covid-19_acaps
    language: python
    name: covid-19_acaps
---

```python
import os

import plotly.express as px
import pandas as pd
import geopandas as gpd
```

```python
CRASH_MOVE_MAIN_DIR = os.path.join('2020-03-16-global-covid-19-response-group', 'GIS')
CRASH_MOVE_INPUT_DIR = '1_Original_Data'
ACAPS_DIR = 'ACAPS_Govt_Measures'
CMF_PATH = '/home/turnerm/remote_shared/charlie/'
NATURAL_EARTH_DIR = 'NaturalEarth'
NATURAL_EARTH_ZIP_FILENAME = 'NE_admin_wld'
NATURAL_EARTH_FILENAME = 'ne_10m_admin_0_countries_lakes'
```

```python
acaps_dir = os.path.join(CMF_PATH, CRASH_MOVE_MAIN_DIR, CRASH_MOVE_INPUT_DIR, ACAPS_DIR)
filename = sorted(os.listdir(acaps_dir))[-1]
df_acaps = pd.read_excel(os.path.join(acaps_dir, filename), sheet_name='Database',
                             usecols=['REGION', 'COUNTRY', 'ISO', 'CATEGORY', 'MEASURE', 'DATE_IMPLEMENTED'])
```

```python
input_filename = os.path.join(CMF_PATH, CRASH_MOVE_MAIN_DIR, CRASH_MOVE_INPUT_DIR,
                                  NATURAL_EARTH_DIR, NATURAL_EARTH_ZIP_FILENAME)
df_naturalearth = gpd.read_file(f'zip://{input_filename}.zip!{NATURAL_EARTH_FILENAME}.shp')
```

```python
df_naturalearth
```

```python
df_naturalearth['tmp'] = df_naturalearth['NAME'].apply(lambda x: len(x))
```

```python
fig = px.choropleth( geojson=df_naturalearth, locations='NAME', color='tmp',
                           color_continuous_scale="Viridis",
                           range_color=(0, 12),
                           #scope="usa",
                           #labels={'unemp':'unemployment rate'}
                          )
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
fig.show()

```

```python
from urllib.request import urlopen

import json
with urlopen('https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json') as response:
    counties = json.load(response)

import pandas as pd
df = pd.read_csv("https://raw.githubusercontent.com/plotly/datasets/master/fips-unemp-16.csv",
                   dtype={"fips": str})

import plotly.express as px

fig = px.choropleth(df, geojson=counties, locations='fips', color='unemp',
                           color_continuous_scale="Viridis",
                           range_color=(0, 12),
                           scope="usa",
                           labels={'unemp':'unemployment rate'}
                          )
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
fig.show()
```

```python
counties
```

```python

```
