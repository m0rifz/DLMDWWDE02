# Batch-basierte Auswertung von Tankstellenpreisen
Mit diesem Projekt im Rahmen der Kurses **Projekt: Data Engineering (DLMDWWDE02)** wurde eine zuverlässige, skalierbare und wartbare Datenarchitektur zur Batch-basierten Verarbeitung von Tankstellenpreisen implementiert.
Die Architektur basiert dabei aus Python-Skripten als auch Cloud-Services von Microsoft Azure. 

---

## Übersicht über die Architektur

**Ablauf der Batchverarbeitung:**
1. Erstellen von CSV-Dateien durch Abfragen und Sammeln von Tankstellenpreisen (`Collector.py`) inkl. Upload der Dateien nach Microsoft Azure in einen Storage Blob
2. Transformation der Daten durch ein weiteres Python-Skript (`Cleaner.py`) inkl. Down- und Upload aus einem bestehenden Azure Storage Blob in ein neues Storage Blob
3. Überleitung und Abfrage der bereinigten CSV-Dateien in eine SQL-Datenbank mittels Azure Synapse Analytics mit anschließender Auswertung

**Verwendete Komponenten:**
- Python 3.12 (Numpy 2.3.5 + Pandas 2.3.3 + Azure-Storage-Blob 12.27.1 + Requests 2.32.5)
- Azure Storage (flacher Dateispeicher mit Blobs)
- Azure Synapse Analytics (Analysedienst inkl. SQL-Warehousing)
- Docker zur Containerisierung und Reproduzierbarkeit
- Azure Verschlüsselung und MS IAM zur Datensicherheit

---

## Verwendung des Repositorys: 

### **1. Repository herunterladen**

`git clone https://github.com/m0rifz/DLMDWWDE02.git DLMDWWDE02`

### **2. Umgebung aufbauen**

Bei **Ausführung mittels Docker** werden alle benötigten Komponenten beim Imagebau automatisch durch die mitgelieferte `requirements.txt` bereitgestellt und installiert.

Bei **lokaler Ausführung** müssen die einzelnen Komponenten, welche in den Dateien `requirements.txt` der jeweiligen Schritte liegen, manuell installiert werden.

### **3. Bereitlegen der Umgebungsparameter**

Zum Ausführen der `Collector.py` sind folgende Umgebungsparameter zu übergeben: 
- Radius in Kilometern (`RADIUS`)
- Breitengrad (`LATITUDE`)
- Längengrad (`LONGITUDE`) 
- Abfragehäufigkeit pro Stunde (`HAEUFIGKEIT`)
- Anzahl der Abfragen bis Beendigung (`ANZAHL`)
- API-Key für Azure Storage (`AZURE_CONNECTION_STRING`)
- Azure Storage Blob Containername (`CONTAINER`)
- API-Key für Tanker-API (`TANKER_API_KEY`) (Zugriff auf die freie Tankerkönig-Spritpreis-API. Für eigenen Key bitte unter https://creativecommons.tankerkoenig.de registrieren.)

Zum Ausführen der `Cleaner.py` sind folgende Umgebungsparameter zu übergeben: 
- API-Key für Azure Storage (`AZURE_CONNECTION_STRING`)
- Azure Storage Blob Containername mit Rohdaten (`RAW_CONTAINER`)
- Azure Storage Blob Containername für bereinigte Daten (`CLEANED_CONTAINER`)

### **4. Ausführen der Datensammlung**

Docker-Image erstellen: 

```
docker build -t dockerfile/collector .
```

Dockerimage ausführen:

```
docker run --rm --env RADIUS=<RadiusInKm> --env LATITUDE=<Breitengrad> --env LONGITUDE=<Längengrad> --env HAEUFIGKEIT=<HäufigkeitProH> --env ANZAHL=<MengeAnAnfragen> --env AZURE_CONNECTION_STRING=<AzureConnectionString> --env CONTAINER=<AzureContainername> --env TANKER_API_KEY=<TankerAPIKey> --name Collector dockerfile/collector python -u Collector.py
```


Der Datensammler kann mehrmals durch die Containerisierung parallel gestartet und laufen gelassen werden mit jeweils unterschiedlichen Koordinaten, um die Menge an abgefragten Tankstellen zu erhöhen. Der Radius der abgefragten Tankstellen-API ist limitiert, sodass ein groß gewählter Radius beschnitten werden würde. 

### **5. Ausführen der Datenbereinigung**

Docker-Image erstellen: 

```
docker build -t dockerfile/cleaner .
```

Dockerimage ausführen: 

```
docker run --rm --env AZURE_CONNECTION_STRING=<AzureConnectionString> --env RAW_CONTAINER=<RawContainer> --env CLEANED_CONTAINER=<CleanedContainer> --name Cleaner dockerfile/cleaner python -u Cleaner.py  
```


Die Datenbereinigung lädt die im Blob gespeicherten Rohdaten herunter und aggregiert diese. Dabei werden diverse Gesichtspunkte behandelt. Unter anderem werden geschlossene Tankstellen von der weiteren Verarbeitung ausgeschlossen, Spritpreise ohne Wert, sogenannte NaN (Not a Number)-Werte, korrigiert und Ausreißer aus der Normalverteilung über den eingelesenen Datensatz geglättet. Anschließend wird eine CSV-Datei mit den bereinigten Datensätzen in einen Blob, welcher die bereinigte Datensätze enthält, hochgeladen. 

### **6. Prüfen der Ergebnisse**

Die CSV-Dateien werden in Intervallen in Azure Synapse Analytics und die damit verbundene SQL-Datenbank eingelesen. Neue Datensätze werden dabei der entsprechenden Datenbanktabelle angehangen. Anschließend kann die Datenbank abgefragt werden. Mögliche Anfragen werden folgend vorgestellt.

Anzahl der Datensätze: 
```
SELECT COUNT(*) FROM cleaneddata.
```

Teuerster Spritpreis inkl. Marke, Ort und Zeit für E5 für die gesamte Aufzeichnungszeit: 
```
SELECT brand, place, time, MAX(e5) FROM cleaneddata.
```

Niedrigster Spritpreis inkl. Marke, Ort und Zeit für E10 für den 21.12.2025: 
```
SELECT brand, place, time, MIN(e10) FROM cleaneddata WHERE time = '21.12.2025'.
```

---

## Wartbarkeit, Skalierbarkeit und Verlässlichkeit

- In diesem Git-Repository liegt der gesamte Programmcode versioniert vor
- Funktionsweise und Anwendungshinweise sind hier übersichtlich aufgeführt und dokumentiert
- Durch Docker kann mittels Containerisierung eine gleiche Laufzeitumgebung auf jeder Plattform geschaffen werden
- Mehrere Docker-Images können parallel betrieben werden, um die dichte der Datenabfrage zu optimieren

## Datensicherheit, -schutz und Governance 

- Clouddienste über Azure liegen hinter einem IAM auf Basis von Microsoft AD
- Nur Berechtigte User haben Zugriff auf die Infrastruktur 
- API-Keys sind rollierend und nicht öffentlich einsehbar
- Azure Storage Blobs sind nicht öffentlich zugänglich
- Daten sind in den Cloud Services verschlüsselt
- Up- und Download der Daten erfolgt über eine verschlüsselte Verbindung 

