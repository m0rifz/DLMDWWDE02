# Batch-basierte Auswertung von Tankstellenpreisen
Mit diesem Projekt im Rahmen der Kurses **Projekt: Data Engineering (DLMDWWDE02)** wird eine zuverlässige, skalierbare und wartbare Datenarchitektur zur Batch-basierten Verarbeitung von Tankstellenpreisen implementiert.
Die Architektur basiert dabei aus Python-Skripten als auch Cloud-Services von Microsoft Azure. 

---

## Übersicht der Architektur:

**Ablauf der Batchverarbeitung:**
1. CSV-Dateien durch Abfragen und Sammeln von Tankstellenpreisen (`Collector.py`) inkl. Upload der Dateien nach Microsoft Azure in Storage Blobs
2. Transformation der Daten durch ein weiteres Python-Skript (`Cleaner.py`) inkl. Down- und Upload aus bestehendem Azure Storage Blob in neues Storage Blob
3. Überleitung der bereinigten CSV-Dateien in SQL-Pool mittels Azure Synapse Analytics

**Verwendete Komponenten:**
- Python 3.12 (Numpy 2.3.5 + Pandas 2.3.3 + Azure-Storage-Blob 12.27.1 + Requests 2.32.5)
- Azure Storage (flacher Dateispeicher mit Blobs)
- Azure Synapse Analytics (Analysedienst inkl. SQL-Warehousing)
- Docker zur Containerisierung und Reproduzierbarkeit
- Azure Verschlüsselung und MS IAM zur Datensicherheit

---

## Verwendung des Repositorys: 

**1. Repository herunterladen**

`git clone <REPO_URL>
cd DLMDWWDE02`

**2. Umgebung aufbauen**

Bei Ausführung mittels Docker werden alle benötigten Komponenten beim Imagebau automatisch durch die mitgelieferte `requirements.txt` bereitgestellt und installiert.
Bei lokaler Ausführung müssen die einzelenn Komponenten, welche in den Dateien `requirements.txt` der jeweiligen Schritte liegen, manuell installiert werden.

**3. Bereitlegen der Umgebungsparametern**

Zum Ausführen der `Collector.py` sind folgende Umgebungsparameter zu übergeben: 
- RadiusInKm (`RADIUS`)
- Breitengrad (`LATITUDE`)
- Längengrad (`LONGITUDE`) 
- Abfragehäufigkeit pro Stunde (`HAEUFIGKEIT`)
- Anzahl der Abfragen bis Beendigung (`ANZAHL`)
- API-Key für Azure Storage (`AZURE_CONNECTION_STRING`)
- Azure Storage Blob Containername (`CONTAINER`)
- API-Key für Tanker-API (`TANKER_API_KEY`)

Zum Ausführen der `Cleaner.py` sind folgende Umgebungsparameter zu übergeben: 
- API-Key für Azure Storage (`AZURE_CONNECTION_STRING`)
- Azure Storage Blob Containername mit Rohdaten (`RAW_CONTAINER`)
- Azure Storage Blob Containername mit bereigiten Daten (`CLEANED_CONTAINER`)

**4. Ausführen der Datensammlung**

Docker-Image erstellen: 

`docker build -t dockerfile/collector .`

Dockerimage ausführen:

```
docker run --rm --env RADIUS=<RadiusInKm> --env LATITUDE=<Breitengrad> --env LONGITUDE=<Längengrad> --env HAEUFIGKEIT=<HäufigkeitProH> --env ANZAHL=<MengeAnAnfragen> --env AZURE_CONNECTION_STRING=<AzureConnectionString> --env CONTAINER=<AzureContainername> --env TANKER_API_KEY=<TankerAPIKey> --name Collector dockerfile/collector python -u Collector.py
```


**5. Ausführen der Datenbereinigung**

Docker-Image erstellen: 

`docker build -t dockerfile/cleaner .`

Dockerimage ausführen: 

```
docker run --rm --env AZURE_CONNECTION_STRING=<AzureConnectionString> --env RAW_CONTAINER=<RawContainer> --env CLEANED_CONTAINER=<CleanedContainer> --name Cleaner dockerfile/cleaner python -u Cleaner.py  
```


**6. Prüfen der Ergebnisse**
