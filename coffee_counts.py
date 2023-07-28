from pathlib import Path
import duckdb
import pandas as pd
import geopandas as gpd


data_dir = Path('~/data/overturemaps')

#----------------
# Setup the Natural Earth admin level 2 dataset as a parquet file
# with a geometry column of wkb. This way it is easily usable
# alongside the overture data.
#----------------

ne_admin2 = gpd.read_file('https://naciscdn.org/naturalearth/10m/cultural/ne_10m_admin_2_counties.zip')
ne_admin2 = ne_admin2.query('ADMIN=="United States of America"')
# USA codes, aka the fips code to uniquely identify each county.
ne_admin2['fips'] = ne_admin2['CODE_LOCAL']
# A non geodataframe to store as a parquet.
us_counties = ne_admin2[['fips']]
us_counties['geometry'] = ne_admin2.geometry.to_wkb()
us_counties.to_parquet(data_dir.joinpath('usa_counties.parquet'))


#--------------------
# download the places parquet files with
# aws s3 cp --region us-west-2 --no-sign-request --recursive s3://overturemaps-us-west-2/release/2023-07-26-alpha.0/theme=places <DESTINATION>
# here I have them in ~/data/overturemaps/places
#--------------------

duckdb.sql('install spatial;')
duckdb.sql('load spatial;')

# Create temporary tables with the needed columns
duckdb.sql("""
create temp table coffee_shops as
select 
    categories.main as main_category,
    ST_GeomFromWkb(geometry) as geom 
from read_parquet('~/data/overturemaps/places/*')
where main_category = 'coffee_shop'
;
""")

duckdb.sql("""
create temp table counties as 
select 
    fips, 
    ST_GeomFromWkb(geometry) as geom 
from read_parquet('~/data/overturemaps/usa_counties.parquet');
""")

# The spatial join, where each Point gets assigned the FIPS county
# ID in which it resides. This takes a few minutes.
duckdb.sql("""
create temp table places_with_counties as select 
    coffee_shops.main_category as main_category,
    counties.fips as fips,
from coffee_shops
join counties 
on ST_Within(coffee_shops.geom, counties.geom)
;           
""")

# Finally a groupby count for coffee shops per county and export to pandas
coffee_shop_counts = duckdb.sql("""
select 
    fips, 
    count(*) as n_coffee_shops 
from places_with_counties 
group by 
    fips
;
""").df()

#----------------------
# Load up population data from census.gov. FIPS identifiers are not
# explicitly in here, but can be derived from the numeric state/county codes.

census_data = pd.read_csv('https://www2.census.gov/programs-surveys/popest/datasets/2020-2022/counties/totals/co-est2022-alldata.csv')
census_data['state_fips'] = census_data.STATE.astype(str).str.pad(2,fillchar='0')
census_data['county_fips'] = census_data.COUNTY.astype(str).str.pad(3,fillchar='0')
census_data['fips'] =  census_data['state_fips'] + census_data['county_fips']
census_data = census_data.query('COUNTY>0') # county code of 0 means it is the total state population data
county_population = (
    census_data[['fips','STNAME','CTYNAME','POPESTIMATE2022']]
    .rename(columns={
        'POPESTIMATE2022':'population_2022',
        'STNAME':'state',
        'CTYNAME':'county',
        })
)

county_population = county_population.merge(coffee_shop_counts, how='left', on='fips')
county_population['coffee_shops_per_100k'] = county_population.n_coffee_shops / (county_population.population_2022/100000)

county_population_geo = ne_admin2[['fips','geometry']].merge(county_population, how='left', on='fips')

# Save results to GeoPackage for viewing in any gis software.
county_population_geo.to_file('./coffee_shops_per_capita.gpkg', driver='GPKG')






