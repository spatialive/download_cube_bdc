import os
from os.path import exists
from urllib.parse import urlparse
import requests
from pystac import Asset
from tqdm import tqdm
import pystac_client
import geopandas as gpd
from joblib import Parallel, delayed
import multiprocessing


NAME_SHP = 'GRID_Teste_BDC_reduced'
INPUT_SHAPEFILE = 'INPUT/' + NAME_SHP +'.shp'
OUTPUT_FOLDER = 'OUTPUT/'

def download(asset: Asset, directory: str = None, **request_options) -> str:
    """Smart download STAC Item asset.

    This method uses a checksum validation and a progress bar to monitor download status.
    """
    if directory is None:
        directory = ''

    response = requests.get(asset.href, stream=True, **request_options)
    total_bytes = int(response.headers.get('content-length', 0))
    output_file = os.path.join(directory, urlparse(asset.href)[2].split('/')[-1])
    #print('------ START FILE -> ' + output_file)
    if exists(output_file):
        filesize = os.path.getsize(output_file)
        if total_bytes == filesize:
            print('------ FILE EXISTS -> ' + output_file)
        else:
            print('Response SIZE: ' + str(total_bytes))
            print('Local File SIZE: ' + str(filesize))
            os.remove(output_file)
            downloadFile(directory, output_file,response, total_bytes)

    else:
        downloadFile(directory, output_file, response, total_bytes)

def downloadFile(directory, output_file, response, total_bytes, chunk_size: int = 1024 * 16):
    os.makedirs(directory, exist_ok=True)
    with tqdm.wrapattr(open(output_file, 'wb'), 'write', miniters=1, total=total_bytes, desc=os.path.basename(output_file)) as fout:
        for chunk in response.iter_content(chunk_size=chunk_size):
            fout.write(chunk)
    print('------ FINISHED FILE -> ' + output_file)

def parallelProcess(item):
    skipAssets = ['B01','CLEAROB','TOTALOB','PROVENANCE','thumbnail']
    assets = item.assets
    for asset in assets:
        if asset not in skipAssets:
            DEST_FILE = OUTPUT_FOLDER + NAME_SHP + '/' + tiles.iloc[i].id + '/' + asset + '/'
            #os.makedirs(DEST_FILE, exist_ok=True)
            download(assets[asset], DEST_FILE)



if __name__ == "__main__":
    parameters = dict(access_token='BDC_AUTH_KEY')
    service = pystac_client.Client.open('https://brazildatacube.dpi.inpe.br/stac/', parameters=parameters)

    collection = service.get_collection('S2-SEN2COR_10_16D_STK-1')

    tiles = gpd.read_file(INPUT_SHAPEFILE)

    for i in range(0,len(tiles)):
      print('BEGIN TILE ID: ' + tiles.iloc[i].id)
      item_search = service.search(bbox=(tiles.iloc[i]['geometry'].buffer(-0.1).bounds),
                datetime='2017-01-01/2020-12-31',
                collections=['S2-SEN2COR_10_16D_STK-1'])
      
      #num_cores = 1
      num_cores = os.cpu_count()
      Parallel(n_jobs=num_cores)(delayed(parallelProcess)(item) for item in item_search.get_items())
      
      #for item in item_search.get_items():
        #print(item)
