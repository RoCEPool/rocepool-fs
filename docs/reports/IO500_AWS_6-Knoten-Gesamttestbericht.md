# Gesamttestbericht: 6 × i3en.12xlarge auf AWS (Lauf 40999521729)

| | |
|---|---|
| Berichtsdatum | 2026-10-31 |
| Gegenstand | RoCEPool (verteiltes Dateisystem auf Basis eines RDMA-Speicherpools), Commit 0f85b7b auf main |
| Plattform | Amazon EC2 us-east-2, 6 × i3en.12xlarge (pro Knoten: 48 vCPU, 384 GiB RAM, 4 × 7,5 TB NVMe, 50 Gb/s EFA), Ubuntu, Kernel 7.0.0-1014-aws, libfabric-Provider efa |
| Lauf | GitHub-Actions-Lauf 40999521729, 2026-10-30 03:49–05:48 UTC. Rohdaten im Branch `aws-results/gh-40999521729` |
| Gesamturteil | **WARN.** Alle Prüfungen bestanden. Das 300-Sekunden-IO500 erreichte 111,26, IO500 wertete es aber wegen einer Phase als INVALID (Abschnitt 5) |
| Verwandt | IO500-Vergleich mit Vendor W: [IO500_AWS_6-Knoten-Ergebnisbericht.md](IO500_AWS_6-Knoten-Ergebnisbericht.md) · englische Fassung dieses Berichts: [IO500_AWS_6NODE_FULL_RUN_REPORT.md](IO500_AWS_6NODE_FULL_RUN_REPORT.md) · weitere Sprachen: [日本語](IO500_AWS_6ノード全項目テストレポート.md) · [Español](IO500_AWS_Informe_de_prueba_completa_de_6_nodos.md) · [繁體中文](IO500_AWS_6節點全項目測試報告.md) |

## 1. Zusammenfassung

Der gesamte Ablauf, vom Start der sechs Instanzen bis zu ihrer Löschung, lief automatisch. Dauer 1 Stunde 59 Minuten, Kosten etwa 67 $.

- **Alle Funktions- und Lasttests bestanden** (Kopfknoten). Alle sechs Knoten bestanden Vorabprüfung und Build.
- **IO500, 60-Sekunden-Lauf:** 124,79 (Bandbreite 17,09 GiB/s, Metadaten 911,2 kIOPS).
- **IO500, 300-Sekunden-Lauf:** **111,26** (Bandbreite 14,81 GiB/s, Metadaten 835,9 kIOPS). Das liegt über den 105,94 von Vendor W mit 28 Maschinen. Weil mdtest-easy-write seine Dateien nach 207 Sekunden fertig angelegt hatte, wertete IO500 den Lauf als INVALID. Die übrigen 11 Phasen erfüllen die Regeln. Die Ursache ist behoben (PR #35).
- **Ein früherer Lauf am selben Tag** (40984482522) stoppte vor IO500 wegen einer Wettlaufsituation beim gleichzeitigen Kopieren des Baums auf fünf Peers. Das wurde vor diesem Lauf behoben (PR #33), dazu kam ein Test (PR #34).

| Schritt | Urteil | Ergebnis |
|---|---|---|
| Vorabprüfung (Kopf + 5 Peers) | WARN | Alles bestanden. WARN, weil EFA auf i3en kein Hardware-RDMA-Read hat (Emulation, wie erwartet) |
| Peers | PASS | SSH, root, Adressen und eindeutige Hostnamen auf allen fünf geprüft |
| Build (Kopf + 5 Peers) | PASS | Transport (libfabric/EFA), FUSE-Client, KV-Bibliothek, LD_PRELOAD-Client, cuFile-Bibliothek |
| Transport | PASS | 92,8 Sekunden Transporttests über EFA |
| Busy-Polling-Abstimmung | PASS | 50 µs gewählt |
| Funktionstests | PASS | smoke, pool, MPI, HPC, stream |
| Nebenläufiger Lasttest | PASS | bis zu 500 Clients, 0 % Fehler, keine hängenden oder ausgefallenen Server |
| IOR + mdtest (FUSE) | PASS | 5 IOR-Muster und mdtest |
| ior-hard + mdtest-hard | PASS | die hard-Phasen von IO500 im Umfang einer Funktionsprüfung |
| KV-API | PASS | nativer und Python-Client |
| GPUDirect Storage | PASS | dieser Instanztyp hat keine GPU; durch Unit-Tests geprüft |
| Wiederverwendung des KV-Caches | PASS | TTFT 1,36-mal schneller als Neuberechnung |
| Clustervorbereitung | WARN | Baum und MPI/IO500-Builds auf alle fünf kopiert. WARN der Peer-Vorabprüfung wie oben |
| NVMe-Vorabprüfung | PASS | NVMe-Schicht auf allen Hosts, O_DIRECT-Schreiben 1.971–2.488 MiB/s |
| IO500, 60 s | PASS | 124,79 |
| IO500, 300 s | WARN | 111,26, als INVALID gewertet (Abschnitt 5) |

## 2. Zeitablauf

| Zeit (UTC) | Ereignis |
|---|---|
| 03:49:42 | Job gestartet. Kostengrenze geprüft: höchstens 100,89 $ für 3 Stunden |
| 03:50:17 | vCPU-Kontingent 300 ≥ benötigte 288 |
| 03:50:29 | Sechs Instanzen gestartet |
| 03:54:10 | Alle sechs mit EFA bereit |
| 03:54:16 | Lokaler Ablauf auf dem Kopfknoten gestartet: Abhängigkeiten, Build, Prüfungen |
| ca. 04:43 | IO500-60-Sekunden-Lauf (696 Sekunden) |
| ca. 04:55 | IO500-300-Sekunden-Lauf (3.112 Sekunden) |
| 05:47:58 | Berichte eingesammelt |
| 05:48:33 | Alle Ressourcen freigegeben |

## 3. Prüfungen auf dem Kopfknoten

Diese laufen auf einem Host mit kleinen Datenmengen. Sie prüfen Funktion und Stabilität, nicht die Leistung des Clusters.

### 3.1 Transport und Busy-Polling-Abstimmung

Die Transporttests (Verbindungspool, Erholung von veralteten Verbindungen, Drain, empfangsseitiger Gegendruck, Obergrenze für registrierten Speicher, MDT/OST-Übereinstimmung, Atomarität von compare-and-swap) bestanden in 92,8 Sekunden.

| Busy-Polling | 1 Thread (req/s) | p50-Latenz | 8 Threads (req/s) |
|---|---|---|---|
| 0 µs | 16.010 | 62 µs | 28.970 |
| 20 µs | 12.221 | 82 µs | 24.057 |
| **50 µs (gewählt)** | **26.645** | **37 µs** | **38.518** |
| 100 µs | 26.864 | 37 µs | 39.502 |
| 200 µs | 26.648 | 37 µs | 39.106 |
| 400 µs | 26.713 | 37 µs | 39.206 |

### 3.2 Nebenläufiger Lasttest

Die Zahl der Clients stieg von 1 auf 500. In jeder Stufe: 0 % Fehler, 14 Server-Threads, keine TIME_WAIT-Sockets, kein listen-Überlauf, keine hängenden oder ausgefallenen Server.

| Clients | Metadaten (ops/s) | Metadaten im gemeinsamen Verzeichnis (ops/s) | Daten (ops/s) | Schlechtestes p99 (Daten) |
|---|---|---|---|---|
| 1 | 6.702 | 6.737 | 505 | 3,9 ms |
| 10 | 10.823 | 10.597 | 1.347 | 19,6 ms |
| 50 | 40.467 | 40.225 | 4.814 | 28,1 ms |
| 100 | 60.558 | 63.443 | 7.493 | 37,7 ms |
| 200 | 74.842 | 82.852 | 9.372 | 67,6 ms |
| 500 | 79.043 | 91.665 | 9.037 | 219,5 ms |

### 3.3 IOR und mdtest über den FUSE-Client

| Muster | Schreiben (MiB/s) | Lesen (MiB/s) |
|---|---|---|
| Gemeinsame Datei, 1 MiB | 58,1 | 863,6 |
| Gemeinsame Datei, 64 KiB | 308,5 | 1.013,2 |
| Datei pro Prozess, 1 MiB | 880,8 | 1.765,2 |
| Gemeinsame Datei, 4 MiB, großes Volumen | 783,4 | 1.505,3 |
| Datei pro Prozess, 256 KiB | 446,3 | 1.660,8 |
| ior-hard-Form (47.008 B) | 152,9 | 265,0 |

mdtest: Anlegen 5.804/s, stat 1.030.542/s, Lesen 87.509/s, Löschen 13.620/s. mdtest-hard-Form: Anlegen 5.122/s, stat 322.050/s.

### 3.4 KV-API und Wiederverwendung des KV-Caches

| Client | Operation | Threads | Wert | ops/s | p50 |
|---|---|---|---|---|---|
| nativ | get | 1 | 64 B | 16.278 | 54,5 µs |
| Python | get | 1 | 64 B | 12.525 | 68,2 µs |
| nativ | put | 1 | 64 B | 15.683 | 52,9 µs |
| nativ | get | 16 | 64 B | 22.251 | 72,7 µs |
| nativ | multi-get, 64 Schlüssel | 16 | 64 B | 242.048 Schlüssel/s | 3,5 ms |
| nativ | get | 1 | 4 KiB | 17.290 | 57,3 µs |
| nativ | multi-get, 64 Schlüssel | 16 | 4 KiB | 130.406 Schlüssel/s | 7,1 ms |

Wiederverwendung des KV-Caches (llama3-70b, 6.144 von 8.192 Tokens im Cache, KV 1.920 MiB): geladen mit 2,1 GB/s in 3,6 Sekunden (p50). Zeit bis zum ersten Token 5,45 Sekunden, 1,36-mal schneller als die 7,41 Sekunden der Neuberechnung (Prefill geschätzt).

## 4. Clustervorbereitung und NVMe-Schicht

Baum und MPI/IO500-Builds wurden parallel auf die fünf Peers kopiert; jeder Peer führte Vorabprüfung und Build selbst aus. Die Uhren aller Hosts wichen höchstens 0,146 Sekunden vom Kopf ab.

| Host | NVMe-Schicht | O_DIRECT-Schreiben (MiB/s) |
|---|---|---|
| Kopf (172.31.7.70) | RAID 0 aus 4 Laufwerken, 21,9-TB-Schichtdatei | 2.232 |
| 172.31.1.244 | wie oben | 1.971 |
| 172.31.11.38 | wie oben | 2.144 |
| 172.31.9.9 | wie oben | 2.062 |
| 172.31.1.247 | wie oben | 2.488 |
| 172.31.10.247 | wie oben | 2.026 |

## 5. IO500

192 Ranks (32 pro Knoten), LD_PRELOAD-Interception-Client (von der Client-Probe gewählt), 6 MDTs, 6 OSTs.

| Phase | 60-s-Lauf | 300-s-Lauf | Dauer im 300-s-Lauf (s) | CPU-Auslastung des Hosts |
|---|---|---|---|---|
| ior-easy-write (GiB/s) | 16,33 | 16,36 | 367,2 | 18 % |
| mdtest-easy-write (kIOPS) | 942,3 | 933,4 | **206,8** | 56 % |
| ior-hard-write (GiB/s) | 9,56 | 9,36 | 306,4 | 37 % |
| mdtest-hard-write (kIOPS) | 419,8 | 385,6 | 311,8 | 55 % |
| find (kIOPS) | 3.187,2 | 2.321,4 | 134,4 | 68 % |
| ior-easy-read (GiB/s) | 25,03 | 22,03 | 272,5 | 43 % |
| mdtest-easy-stat (kIOPS) | 952,2 | 938,7 | 205,6 | 54 % |
| ior-hard-read (GiB/s) | 21,85 | 14,25 | 201,4 | 36 % |
| mdtest-hard-stat (kIOPS) | 942,5 | 934,1 | 129,4 | 54 % |
| mdtest-easy-delete (kIOPS) | 959,0 | 949,4 | 203,4 | 53 % |
| mdtest-hard-read (kIOPS) | 461,9 | 421,1 | 285,6 | 60 % |
| mdtest-hard-delete (kIOPS) | 947,8 | 813,9 | 148,3 | 36 % |
| **Bandbreitenergebnis (GiB/s)** | 17,09 | **14,81** | | |
| **Metadatenergebnis (kIOPS)** | 911,2 | **835,9** | | |
| **IO500-Ergebnis** | 124,79 | **111,26** | gesamt 3.112 | |

Die CPU-Auslastung des Hosts ist der genutzte Anteil der 48 vCPU von Knoten 0 während der jeweiligen Phase des 300-Sekunden-Laufs.

**Warum der 300-Sekunden-Lauf als INVALID gewertet wurde.** mdtest-easy-write hatte die vorgegebenen Dateien (1 Million pro Rank, insgesamt 192 Millionen) nach 207 Sekunden angelegt und hielt vor dem 300-Sekunden-Stonewall an. Der Standardwert von 1 Million stammt aus der Zeit, als ein Rank etwa 600 Dateien pro Sekunde anlegte; heute sind es etwa 4.900. Der Bericht zur Laufzeit nannte als Ursache die Datenmenge von ior-easy-write – auch das war falsch: ior-easy-write waren 59.006 GiB zugeteilt, bei 5.633 GiB Bedarf. **Behebung (PR #35):** Die Dateizahl pro Rank für mdtest wird aus der im 60-Sekunden-Lauf gemessenen Geschwindigkeit bestimmt (hier etwa 2,2 Millionen pro Rank). Der Bericht nennt jetzt auch die Phase, die tatsächlich zu kurz war.

**Datenmengen im 300-Sekunden-Lauf.** Die Kapazitätsgrenze der OSTs liegt bei 118.013 GiB (bestimmt durch die kleinste Schicht mit 21,9 TB). Nach den Geschwindigkeiten des 60-Sekunden-Laufs wurden ior-easy 59.006 GiB und ior-hard 3.297 GiB zugeteilt. Die Lookup-Tabelle eines MDT hat 67.108.864 Slots, die Inline-Kapazität für kleine Dateien beträgt 57.400 MiB pro MDT.

## 6. Der frühere Lauf (40984482522)

Der erste 6-Knoten-Lauf des Tages bestand alle Prüfungen auf dem Kopfknoten und scheiterte dann beim Kopieren des Baums auf einen der fünf Peers mit `tar: .: file changed as we read it`. Die Prüfung „ist das dieselbe Maschine?“ jedes Peers legte im obersten Verzeichnis des Baums eine Token-Datei an und löschte sie wieder; das fiel mit dem Lesen von tar für einen anderen Peer zusammen. Mit 1–3 Peers kam es nicht dazu, mit 5 schon. Die Instanzen wurden nach 37 Minuten vor dem Start von IO500 freigegeben (20,6 $).

- **Behebung (PR #33):** Das Token liegt in einem Verzeichnis, das vom Archiv ausgeschlossen ist, und jede Prüfung nutzt eine eigene Datei. Ein tar-Ende mit „file changed“ gilt als Erfolg. Eine fehlgeschlagene Kopie wird einmal wiederholt.
- **Test (PR #34):** Gleichzeitiges Kopieren auf fünf Peers mit einem realitätsnahen 20-MB-Baum und versetzten Prüfzeitpunkten. Mit dem alten Code schlägt er fehl (3 von 3), nach der Behebung besteht er. Ein eigenständiger Reproduktionstest zeigt nach der Behebung keine Fehler bei 1–31 Peers.

## 7. Kosten

| Posten | Betrag |
|---|---|
| 6 × i3en.12xlarge (inkl. 400-GiB-gp3-Platte) | 33,63 $/Stunde |
| Dieser Lauf, Instanzlaufzeit 1 Stunde 59 Minuten | ca. 67 $ |
| Früherer Lauf, 37 Minuten | ca. 20,6 $ |

Die Kosten sind aus der Instanzlaufzeit zu AWS-On-Demand-Preisen berechnet, nicht der AWS-Rechnungsbetrag.
