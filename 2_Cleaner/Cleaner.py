import glob
import pandas
import numpy
import os
import datetime
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient


def nancheck(data):
    check_nan = data.isnull().any().any()

    if check_nan==True:
        total_nans = data.isnull().sum().sum()
        print(f'{total_nans} NaNs im DataFrame.')
    else:
        print('Keine NaNs im DataFrame.')

    return check_nan


def ausbrecher_check(daten):
    Q1 = numpy.percentile(daten, 15)
    Q3 = numpy.percentile(daten, 85)
    low_lim = Q1 - 1.5 * (Q3 - Q1)
    up_lim = Q3 + 1.5 * (Q3 - Q1)

    return low_lim, up_lim


def anz_ausbrecher(daten, low_lim, up_lim):
    anz = ((daten < low_lim) | (daten > up_lim)).sum()

    if anz == 0:
        print('Keine Ausbrecher vorhanden')
    else:
        print(f'{anz} Ausbrecher vorhanden')


def ausbrecher_corr(daten, art, low_lim, up_lim):
    daten[art] = daten[art].clip(low_lim, up_lim)

    return daten


def data_cleaner():
    files = glob.glob('Daten/*.csv')
    print(files)
    datafiles = [pandas.read_csv(f, sep=',', decimal='.') for f in files]
    rawdata = pandas.concat(datafiles, ignore_index=True) 
    rawdata = rawdata.drop(rawdata[rawdata['id'] == 'id'].index)
    rawdata = rawdata.drop(rawdata[rawdata['isOpen'] == 'False'].index)
    rawdata = rawdata.drop_duplicates(subset = ['id', 'time']).reset_index(drop=True)
    rawdata['e5'] = rawdata['e5'].astype(float)
    rawdata['e10'] = rawdata['e10'].astype(float)
    rawdata['diesel'] = rawdata['diesel'].astype(float)

    print(f'Anzahl im DataFrame: {len(rawdata)}')

    if nancheck(rawdata)==True:
        rawdata.dropna(subset=['diesel', 'e5', 'e10'], how='all')
        rawdata.ffill(limit=2, inplace=True)
        rawdata['e10'] = rawdata['e10'].fillna(rawdata['e5'])
        rawdata['e5'] = rawdata['e5'].fillna(rawdata['e10'])

        if nancheck(rawdata)==True:
            fehler_nan='True'
            print('Nicht alle NaN-Werte wurden beseitigt.')

    low_e5, up_e5 = ausbrecher_check(rawdata['e5'])
    print(f'E5: 15Q: {low_e5:.3f}, 85Q: {up_e5:.3f}')
    anz_ausbrecher(rawdata['e5'], low_e5, up_e5)
    rawdata = ausbrecher_corr(rawdata, 'e5', low_e5, up_e5)
    anz_ausbrecher(rawdata['e5'], low_e5, up_e5)

    low_e10, up_e10 = ausbrecher_check(rawdata['e10'])
    print(f'E10: 15Q: {low_e10:.3f}, 85Q: {up_e10:.3f}')
    anz_ausbrecher(rawdata['e10'], low_e10, up_e10)
    rawdata = ausbrecher_corr(rawdata, 'e10', low_e10, up_e10)
    anz_ausbrecher(rawdata['e10'], low_e10, up_e10)

    low_diesel, up_diesel = ausbrecher_check(rawdata['diesel'])
    print(f'Diesel: 15Q: {low_diesel:.3f}, 85Q: {up_diesel:.3f}')
    anz_ausbrecher(rawdata['diesel'], low_diesel, up_diesel)
    rawdata = ausbrecher_corr(rawdata, 'diesel', low_diesel, up_diesel)
    anz_ausbrecher(rawdata['diesel'], low_diesel, up_diesel)

    print(f'Verbleibende Anzahl im DataFrame: {len(rawdata)}')

    dir = os.path.dirname(os.path.realpath(__file__))
    file = datetime.date.today().isoformat() + '_cleaned.csv'
    path = os.path.join(dir, 'Bereinigte_Daten', file)
    rawdata.to_csv(path, sep=',', encoding='utf-8', float_format='%.3f', index=False)


def EnvHolen():
        global raw_container_name
        global cleaned_container_name
        global connection_string

        raw_container_name= os.environ['RAW_CONTAINER']
        cleaned_container_name= os.environ['CLEANED_CONTAINER']
        connection_string = os.environ['AZURE_CONNECTION_STRING']

        if raw_container_name == '' or cleaned_container_name == '': 
            print('Bitte füllen Sie alle Variablen')


def upload_refineddata():

    blob_service_up_client = BlobServiceClient.from_connection_string(connection_string)
    count = 0

    for root,dirs,files in os.walk('Bereinigte_Daten/'):        
        if files:
            for file in files:
                file_path_on_local = os.path.join(root,file)
                blob_client = blob_service_up_client.get_blob_client(container=cleaned_container_name, blob=file)
                with open(file_path_on_local, 'rb') as data:
                    blob_client.upload_blob(data)
                    print(f'Datei hochgeladen: {file}')
                    count += 1

    print(f'Verarbeitung abgeschlossen. {count} Dateien hochgeladen.')


def checkFileExistance (file_name):
    try:
        with open(os.path.join('Daten/' + file_name + '.csv'), 'r') as file:
            return True
    except IOError as file_not:
        return False
    except FileNotFoundError as file_not:
        return False
    

def download_rawdata():

    blob_service_down_client =  BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_down_client.get_container_client(raw_container_name)

    blob_list = container_client.list_blobs()
    print(f"Raw-Download gestartet.")
    for blob in blob_list:
        blob_client = container_client.get_blob_client(blob)
        blob_props = blob_client.get_blob_properties()
        download_path = os.path.join("Daten", blob.name.split("/")[-1])
        with open(download_path, "ab") as download_file:
            download_stream = blob_client.download_blob()
            download_file.write(download_stream.readall()) 
    print(f"Raw-Download beendet.")


EnvHolen()

download_rawdata()

data_cleaner()

upload_refineddata()