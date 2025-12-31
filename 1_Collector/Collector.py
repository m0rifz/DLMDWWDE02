import requests
import csv
import datetime
from datetime import datetime
import time
import os
from azure.storage.blob import BlobServiceClient


def collect_data():

        count_abfragen = 0
        abfrage_sekunden = 3600 / int(haeufigkeit)

        while count_abfragen < int(anzahl):   
            data = apiRequest(latitude, longitude, radius)
            DateiAnlegen(data, str(datetime.now())[:-7])

            count_abfragen += 1

            print(f'Abfrage {count_abfragen}/{anzahl} durchgeführt. {len(data['stations'])} Stationen abgefragt.')
            time.sleep(abfrage_sekunden)
        print(f'Beendet: {anzahl} Anfragen durchgeführt')


def requestInt(haeufigkeit):
    abfrage_sekunden = 3600 / haeufigkeit

    return(abfrage_sekunden)

def EnvHolen():
        global radius
        global latitude
        global longitude
        global haeufigkeit
        global anzahl
        global container_name
        global connection_string
        global tanker_api_key

        radius = os.environ['RADIUS']
        latitude = os.environ['LATITUDE']
        longitude = os.environ['LONGITUDE']
        haeufigkeit = os.environ['HAEUFIGKEIT']
        anzahl = os.environ['ANZAHL']
        container_name = os.environ['CONTAINER']
        connection_string = os.environ['AZURE_CONNECTION_STRING']
        tanker_api_key = os.environ['TANKER_API_KEY']

        if haeufigkeit == '' or radius == '' or longitude == '' or latitude == '' or anzahl == '' or container_name == '': 
            print('Bitte füllen Sie alle Variablen')



def apiRequest(latitude, longitude, radius):
    url = f'https://creativecommons.tankerkoenig.de/json/list.php?lat={latitude}&lng={longitude}&rad={radius}&sort=dist&type=all&apikey={tanker_api_key}'
    data = requests.get(url).json()

    return (data)



def checkFileExistance (file_name):
    try:
        with open(os.path.join('Daten/' + file_name + '.csv'), 'r') as file:
            return True
    except IOError as file_not:
        return False
    except FileNotFoundError as file_not:
        return False
    


def DateiAnlegen(data, time_now):
    count = 0
    anzahlStationen = (len(data['stations']))

    while count < anzahlStationen:
        data_name = (data['stations'][count]['id'])
        data_value = (data['stations'][count])
        data_value.update(time=time_now)
        data_value_labels = data_value.keys()
        data_value_value = data_value.values()


        file_exist = checkFileExistance(data_name)
        if file_exist == False:
            with open(os.path.join('Daten/' + data_name + '.csv'), 'w', encoding='utf-8') as new_file:
                writer = csv.writer(new_file)
                writer.writerow(data_value_labels)
                writer.writerow(data_value_value)
                new_file.close()


        elif file_exist == True:
            with open(os.path.join('Daten/' + data_name + '.csv'), 'a', encoding='utf-8') as existing_file:
                writer = csv.writer(existing_file)
                writer.writerow(data_value_value)
                existing_file.close()

        count += 1


def upload_rawdata():

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    count = 0

    for root,dirs,files in os.walk('Daten/'):        
        if files:
            for file in files:
                file_path_on_azure = str(latitude)+'_'+str(longitude)+'_'+str(datetime.now())[:-16]+'/'+file
                file_path_on_local = os.path.join(root,file)
                blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_path_on_azure)
                with open(file_path_on_local, 'rb') as data:
                    try:
                        blob_client.upload_blob(data)
                        print(f'Datei hochgeladen: {file}')
                        count += 1
                    except Exception as e:
                        print(f'Fehler im Dateiupload: {file}')
                        print(e.message)

    print(f'Verarbeitung abgeschlossen. {count} Dateien hochgeladen.')



EnvHolen()

collect_data()

upload_rawdata()