import geopandas as gpd
import google.cloud.bigquery
import time
import tempfile
import os
import requests

start_time = time.time()
bqclient = google.cloud.bigquery.Client()


url = "http://d2ad6b4ur7yvpq.cloudfront.net/naturalearth-3.3.0/ne_110m_land.zip"
url = "zip:///Users/jatorre/Downloads/NFDB_point.zip"
url = "https://download.geofabrik.de/europe/albania-latest-free.shp.zip"

destination_table = 'cartobq.temp_tables.osm_albania2'
ensure_spherical = False

#{'DXF': 'rw', 'CSV': 'raw', 'OpenFileGDB': 'raw', 'ESRIJSON': 'r', 'ESRI Shapefile': 'raw', 'FlatGeobuf': 'raw', 'GeoJSON': 'raw', 'GeoJSONSeq': 'raw', 'GPKG': 'raw', 'GML': 'rw', 'OGR_GMT': 'rw', 'GPX': 'rw', 'MapInfo File': 'raw', 'DGN': 'raw', 'S57': 'r', 'SQLite': 'raw', 'TopoJSON': 'r'}
driver_to_use = None

#If layer specific it will only load that layer, if not specified it will load all layers   
layer = None

print(f"Importing {url} into {destination_table} ensuring spherical: {ensure_spherical}")

project, dataset, base_table = destination_table.split('.')
created_tables = []


def import_layer(url, layer, destination_table):

    if driver_to_use is not None:
        gdf = gpd.read_file(url, layer=layer, driver=driver_to_use)
    else:
        gdf = gpd.read_file(url, layer=layer)

    if gdf.crs is None:
        gdf.set_crs(epsg=4326, inplace=True)
    elif gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    gdf['geometry'] = gdf['geometry'].force_2d()
    gdf['geometry'] = gdf['geometry'].make_valid()

    job_config = google.cloud.bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

    if ensure_spherical:
        gdf['geom_wkb'] = gdf['geometry'].apply(lambda x: x.wkb)
        gdf = gdf.drop(columns=['geometry'])
        job = bqclient.load_table_from_dataframe(gdf, f'{destination_table}_temp', job_config=job_config)
        job.result()
        query = f"""
        CREATE OR REPLACE TABLE {destination_table} CLUSTER BY geom AS
        SELECT * EXCEPT (geom_wkb),
        ST_GEOGFROMWKB(geom_wkb,planar=>true, make_valid=>true) AS geom
        FROM {destination_table}_temp; DROP TABLE {destination_table}_temp;
        """
        query_job = bqclient.query(query)
        query_job.result()

    else:
        gdf.rename(columns={'geometry': 'geom'}, inplace=True)
        job = bqclient.load_table_from_dataframe(gdf, destination_table, job_config=job_config)
        job.result()
    created_tables.append({'layer': layer, 
                           'destination_table': destination_table,
                           'num_features': len(gdf)})

# Download the url to a temporary file
with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(url)[-1]) as tmp_file:
    with requests.get(url, stream=True) as r:
        for chunk in r.iter_content(chunk_size=8192):
            tmp_file.write(chunk)


layers = gpd.list_layers(tmp_file.name)

if layer is not None:
    import_layer(tmp_file.name, layer, destination_table)
elif len(layers) > 1:
    for index, row in layers.iterrows():
        import_layer(tmp_file.name, row['name'], f"{destination_table}_{row['name']}")
else:
    import_layer(tmp_file.name, layers['name'].values[:1][0], destination_table)

print(created_tables)
print(f"Total time taken: {time.time() - start_time:.2f} seconds")
