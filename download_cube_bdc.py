import os
from urllib.parse import urlparse
import requests
from pystac import Asset
from tqdm import tqdm
import pystac_client
import geopandas as gpd

NAME_SHP = 'GRID_Teste_BDC'
INPUT_SHAPEFILE = 'INPUT/' + NAME_SHP +'.shp'
OUTPUT_FOLDER = 'OUTPUT/'

def download(asset: Asset, directory: str = None, chunk_size: int = 1024 * 16, **request_options) -> str:
    """Smart download STAC Item asset.

    This method uses a checksum validation and a progress bar to monitor download status.
    """
    if directory is None:
        directory = ''

    response = requests.get(asset.href, stream=True, **request_options)
    output_file = os.path.join(directory, urlparse(asset.href)[2].split('/')[-1])
    os.makedirs(directory, exist_ok=True)
    total_bytes = int(response.headers.get('content-length', 0))
    with tqdm.wrapattr(open(output_file, 'wb'), 'write', miniters=1, total=total_bytes, desc=os.path.basename(output_file)) as fout:
        for chunk in response.iter_content(chunk_size=chunk_size):
            fout.write(chunk)

if __name__ == "__main__":
    parameters = dict(access_token='BDC_AUTH_KEY')
    service = pystac_client.Client.open('https://brazildatacube.dpi.inpe.br/stac/', parameters=parameters)

    collection = service.get_collection('S2-SEN2COR_10_16D_STK-1')

    tiles = gpd.read_file(INPUT_SHAPEFILE)

    for i in range(0,len(tiles)):
      item_search = service.search(bbox=(tiles.iloc[i]['geometry'].bounds),
                                 datetime='2017-01-01/2020-12-31',
                                 collections=['S2-SEN2COR_10_16D_STK-1'])
      #print(item_search)
      
      for item in item_search.get_items():
        print(item)
        assets = item.assets
        DEST_FILE = OUTPUT_FOLDER + NAME_SHP + '/' + tiles.iloc[0].id + '/'
        for asset in assets.values():
            download(asset, DEST_FILE)
