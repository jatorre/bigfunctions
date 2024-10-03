import json
import uuid
import time
import ee
from google.cloud import bigquery, storage


table_name = 'cartodb-gcp-solutions-eng-team.gee_demo.palm_oil_plots'
reducer = 'SUM'
image_expression = f"image=ee.ImageCollection('BIOPAMA/GlobalOilPalm/v1').select('classification').mosaic()"
band = 'classification'
scale = 100
table_destination = 'cartobq.temp_tables.palm_oil_plots_enriched'
overwrite = True
append = False

# Setup credentials and environment variables
ee.Initialize()
client = bigquery.Client()
gcs_client = storage.Client()

def export_bq_to_gcs(table_name, bucket_name, file_path, overwrite='TRUE', timeout=300):
    gcs_uri = f'gs://{bucket_name}/{file_path}*.csv'
    
    task = client.query(f'''
        EXPORT DATA
          OPTIONS (
            uri='{gcs_uri}',
            format='CSV',
            overwrite={overwrite},
            header=TRUE
          ) AS
        SELECT * FROM {table_name}
    ''')
    
    start_time = time.time()
    while True:
        if time.time() - start_time > timeout:
            raise TimeoutError(f"Export job timed out after {timeout} seconds")
        
        task.reload()
        if task.state == 'DONE':
            if task.error_result:
                raise RuntimeError(f"Export job failed with error: {task.error_result}")
            return  # Success
        elif task.state not in ['PENDING', 'RUNNING']:
            raise ValueError(f"Unexpected job state: {task.state}")
        time.sleep(2)

def list_gcs_files(bucket_name, file_path):
    bucket = gcs_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=file_path)
    gcs_urls = [f'gs://{bucket_name}/{blob.name}' for blob in blobs if blob.name.endswith('.csv')]
    if not gcs_urls:
        raise ValueError(f"No CSV files found in gs://{bucket_name}/{file_path}")
    return gcs_urls

def import_to_gee(asset_id, gcs_urls, timeout=300):
    params = {
        "name": asset_id,
        "sources":[{"uris": [uri], "charset": "UTF-8"} for uri in gcs_urls]
    }
    task = ee.data.startTableIngestion(
        request_id='import-task-' + str(uuid.uuid4()),
        params=params,
        allow_overwrite=True
    )
    
    task_id = f'projects/earthengine-legacy/operations/{task["id"]}'
    
    start_time = time.time()
    while True:
        if time.time() - start_time > timeout:
            raise TimeoutError(f"Import to GEE timed out after {timeout} seconds")
        
        status = ee.data.getOperation(task_id)
        if 'done' in status and status['done']:
            if 'error' in status:
                raise RuntimeError(f"Import to GEE failed with error: {status['error']['message']}")
            return  # Success
        time.sleep(3)

def enrich_with_reduce_regions(fc_asset_id, image_expression, band, reducer, scale=30):
    reducers = {
        'MEAN': ee.Reducer.mean(),
        'SUM': ee.Reducer.sum(),
        'MIN': ee.Reducer.min(),
        'MAX': ee.Reducer.max(),
        'MEDIAN': ee.Reducer.median(),
        'STD_DEV': ee.Reducer.stdDev(),
        'VARIANCE': ee.Reducer.variance(),
        'COUNT': ee.Reducer.count(),
        'FIRST': ee.Reducer.first(),
        'LAST': ee.Reducer.last(),
        'PROD': ee.Reducer.product(),
        'ALL': ee.Reducer.allNonZero(),
        'ANY': ee.Reducer.anyNonZero()
    }
    
    reducer = reducers.get(reducer.upper())
    if not reducer:
        raise ValueError(f"Reducer {reducer} is not supported. Please choose from {list(reducers.keys())}.")

    fc = ee.FeatureCollection(fc_asset_id)
    
    expression = image_expression.strip()
    print(expression)
    exec_locals = {'ee': ee}  # Ensure 'ee' is passed into the local context
    exec(expression, globals(), exec_locals)
    image = exec_locals.get('image')

    if image is None:
        raise NameError("The variable 'image' was not created by the exec() call.")
    
    enriched_fc = image.select(band).reduceRegions(
        collection=fc,
        reducer=reducer,
        scale=scale,
        tileScale=2 
    )
    
    return enriched_fc

def export_to_bigquery(fc, table_destination, append=False, overwrite=False, timeout=600):
    task = ee.batch.Export.table.toBigQuery(
        collection=fc,
        table=table_destination,
        description='put_my_data_in_bigquery',
        append=append,
        overwrite=overwrite
    )
    task.start()
    
    start_time = time.time()
    while task.active():
        if time.time() - start_time > timeout:
            raise TimeoutError(f"Export to BigQuery timed out after {timeout} seconds")
        time.sleep(10)
        
    if task.status()['state'] == 'COMPLETED':
        return  # Success
    else:
        error_message = task.status().get('error_message', 'Unknown error')
        raise RuntimeError(f'Export to BigQuery failed: {error_message}')


try:
    unique_suffix = str(uuid.uuid4()).replace("-", "")
    BUCKET_NAME = 'bq_exports_for_gee'
    GCS_PATH = f"gee_extension_package/bigquery_export/{unique_suffix}"
    GEE_PATH = f"projects/cartodb-gcp-solutions-eng-team/assets/gee_enrich_bq/{unique_suffix}"
    
    # Step 1: Export BQ to GCS
    export_bq_to_gcs(table_name, BUCKET_NAME, GCS_PATH) 
    print('Exported from BQ.')
    
    # Step 2: List GCS files
    gcs_urls = list_gcs_files(BUCKET_NAME, GCS_PATH) 
    
    # Step 3: Import to GEE
    import_to_gee(GEE_PATH, gcs_urls) 
    print('Imported into GEE.')
    
    # Step 4: Enrich with Reduce Regions
    enriched_fc = enrich_with_reduce_regions(GEE_PATH, image_expression, band, reducer, scale=scale) 
    
    # Step 5: Export to BigQuery
    export_to_bigquery(enriched_fc, table_destination, overwrite=overwrite, append=append)
    print('Export to BQ successful.')
    
    print( json.dumps({"replies": ['GEE Enrichment successful']}))

except Exception as ex:
    error_message = f'Error type: {type(ex).__name__}. Error message: {str(ex)}'
    print(error_message)
    print( {"errorMessage": error_message})