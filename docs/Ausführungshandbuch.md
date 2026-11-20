# rocepool Ausführungshandbuch

rocepool ist ein verteiltes Dateisystem auf Basis eines Speicherpools über RDMA/RoCE. Es besteht aus:

- MDT-Servern (Metadaten) und OST-Servern (Daten), nativ in C geschrieben
- drei Clients: einem FUSE-Client in C, einer LD_PRELOAD-Interception-Bibliothek und einer Python-Referenzimplementierung
- einer NVMe-Kapazitätsschicht für die OSTs (Kernel-O_DIRECT- oder SPDK-Backend, P2PDMA optional)
- durchgehender Automatisierung: Vorprüfung, Build, Verteilung, Start der Dienste, offizieller IO500-Lauf, Bericht mit Engpassanalyse

Dieses Handbuch beschreibt nur, wie man das System ausführt. Entwurf und Ergebnisse der einzelnen Runden stehen in `README.md` und `docs/` (die neueste Runde: `docs/AWS_ROUND.md`).

**Der einfachste Einstieg**:

- Entpacken und `sudo ./run.sh` ausführen. Mit RDMA-Netzwerkkarte läuft es auf echter Hardware, ohne als Simulation mit 10 Knoten.
- Auf AWS: `python3 tools/aws_cluster.py run ...` (Abschnitt 4b).

> Weitere Sprachen (gleicher Inhalt): [中文](运行说明书.md) · [日本語](実行マニュアル.md) · [Español](Manual_de_ejecución.md) · [繁體中文](執行手冊.md)

---

## 1. Verzeichnisstruktur

| Verzeichnis | Inhalt |
|---|---|
| `roce/native/` | C-Code. Wichtigste Dateien: `roce_transport.c` (RDMA-Transport über verbs/rdma_cm für RoCE-Karten und Soft-RoCE), `ofi_transport.c` (libfabric-Transport mit derselben Schnittstelle, für AWS EFA), `rocepool_servers.c` (MDT-/OST-Server), `ost_tier.c` (NVMe-Kapazitätsschicht der OSTs), `rocepool_fs.c` (Client-Kern), `rocepool_fuse.c` (FUSE-Client), `rocepool_intercept.c` (LD_PRELOAD-Interception-Client), `rocepool_kv.c` (native KV-API), `rocepool_cufile.c` (direktes Lesen in die GPU, cuFile-Schnittstelle), `build.sh` (Build für echte Netzwerkkarten), `softverbs/` (RDMA-Emulator im Userspace, für Rechner ohne Netzwerkkarte) |
| `roce/`, `fs/`, `storage_engine/` | Python-Referenzimplementierung (Protokoll, MDT, Daten-Striping, POSIX-Schicht) |
| `tools/` | Automatisierung: `roce_onsite.py` (echte Hardware mit einem Befehl), `io500_run.py` (IO500 auf N Knoten), `bench_ior.py` (lokales IOR/mdtest), `roce_preflight.py` (RoCE-Vorprüfung), `nvme_preflight.py` (NVMe-/P2P-Vorprüfung), `io500_model.py` (Engpassanalyse und Prognose), `aws_cluster.py` (AWS mit einem Befehl: starten → einrichten → ausführen → Berichte holen → freigeben), `bench_gds.py` (GPU-Prüfung und Messung des direkten Lesens), `bench_kvcache.py` (Wiederverwendung des KV-Cache und TTFT) |
| `tests/` | 465 Tests (`python3 -m unittest discover -s tests -t .`) |
| `stress_reports/` | Berichte früherer Läufe (JSON). Die Nummern sind chronologisch, der neueste ist `39_io500_sim10_intercept.json` |
| `docs/` | Entwurfsdokumente und Ergebnisse jeder Runde |

---

## 2. Voraussetzungen

**Offline nutzbar.** Das Verzeichnis `deps/` im Paket enthält den Quellcode aller Fremdkomponenten.

- Enthalten sind: MPICH 4.2.3, das offizielle IO500 (mit fest versioniertem IOR, pfind, lz4 und den Schema-Werkzeugen), IOR, libfabric 2.1.0, rdma-core 59.2.
- IOR ist bereits mit configure vorbereitet; vor Ort braucht es weder Autotools noch Internet. Der Build entnimmt es automatisch von hier (`tools/deps.py`).
- Mit `ROCEPOOL_OFFLINE=1` sind Downloads aus dem Netz ganz verboten. Fehlt etwas, bleibt der Lauf nicht bei einem Download hängen, sondern meldet sofort, was fehlt.
- Der FUSE-Client hängt nicht von der Version der Kernel-Header des Systems ab und baut daher auch unter Ubuntu 22.04 (Kernel 5.15).
- Um diese Quellen zu aktualisieren, führen Sie `tools/make_deps.sh` auf einem Rechner mit Internetzugang aus.

**Jeder Rechner** (bei einer Verteilung auf echte Hardware müssen alle Knoten das erfüllen):

- Linux x86_64, Kernel 5.x oder neuer. FUSE über io_uring braucht 6.14 oder neuer, P2PDMA 6.2 oder neuer.
- Root-Rechte (für das Einhängen von FUSE, Namensräume und das Anpassen der RoCE-Parameter).
- `python3` 3.9 oder neuer, `gcc`, `make`, `git`, `cmake` (für den automatischen Build von rdma-core).
- Bei echten Netzwerkkarten: die Entwicklungspakete von rdma-core (`libibverbs-dev librdmacm-dev` / `rdma-core-devel`). `build.sh` installiert sie selbst oder baut sie aus dem Quellcode.
- Rechner, die IO500 ausführen, brauchen zusätzlich `curl` und `autoconf automake libtool pkg-config`. MPICH und IOR/IO500 werden beim ersten Lauf automatisch heruntergeladen, gebaut, in `.bench_cache/` abgelegt und danach wiederverwendet.
- Bei mehreren Rechnern:
  - Von diesem Rechner aus muss ssh ohne Passwort auf jeden Knoten möglich sein (`root@host`).
  - Alle Hostnamen müssen verschieden sein.
  - Der Codebaum muss **auf allen Rechnern unter demselben absoluten Pfad** liegen (das Skript synchronisiert ihn automatisch).

**Entwicklungsrechner ohne Netzwerkkarte**: Alles oben außer rdma-core genügt. Tests und der simulierte Cluster laufen vollständig mit dem Emulator `softverbs`.

---

## 3. Zuerst: der Selbsttest

```bash
cd rocepool
sudo python3 -m unittest discover -s tests -t .      # etwa 5 Minuten, 465 Tests
```

Ist alles OK, ist die Umgebung in Ordnung. Die Tests erledigen automatisch Folgendes:

- Sie bauen die Emulator-Bibliothek, den FUSE-Client und die Interception-Bibliothek.
- Sie starten die C-Server lokal und hängen ein echtes FUSE ein.
- Sie führen echtes IOR/mdtest aus (falls schon in `.bench_cache` vorhanden).

---

## 4. Verteilung auf echte Hardware und IO500 (der Hauptweg)

**Ein Befehl erledigt alles.** Der Ablauf:

1. Vorprüfung, Build, Prüfung der Transportschicht
2. Synchronisation auf jeden Knoten, dort Vorprüfung und Build
3. Start von N MDTs und N OSTs
4. Probe der Clients
5. Offizieller IO500-Lauf
6. Bericht und Aufräumen

### 4.0 Die Cluster-Konfigurationsdatei cluster.conf (empfohlen, beliebig viele Knoten)

Stehen alle Knoten in einer Datei, genügt es, auf irgendeinem davon `sudo ./run.sh` auszuführen; der Rest läuft bis zum Ende durch. `--peers` ist nicht nötig, und nach keinem Passwort wird gefragt.

```bash
cp cluster.conf.example cluster.conf     # danach bearbeiten
sudo ./run.sh                            # nutzt cluster.conf, wenn vorhanden; eine andere Datei mit --conf
```

```
[login]
# Benutzer mit sudo-Rechten auf jedem Knoten und sein Passwort (auf allen Knoten gleich)
# Achtung: keinen Kommentar hinter die password-Zeile schreiben, auch # gehört dann zum Passwort
user = admin
password = xxxx
[nodes]
# SSH-Adresse      RoCE-IP            Hostname (optional)
10.0.0.11       10.10.0.11      node01
10.0.0.12      10.10.0.12     node02
...                                   # 100 Knoten: 100 Zeilen
```

run.sh erledigt automatisch (`tools/cluster_conf.py`, je 32 Knoten parallel):

1. Anhand der RoCE-IP erkennt es, welche Zeile dieser Rechner selbst ist; alle anderen sind die Gegenstellen. Die eigene RoCE-IP dient zugleich als `--ip`.
2. Es richtet die passwortlose Anmeldung von root auf diesem Rechner bei root auf jeder Gegenstelle ein.
   - Bei Knoten, die bereits ohne Passwort erreichbar sind, tut es nichts.
   - Bei den übrigen meldet es sich **ein einziges Mal** mit Benutzer und Passwort aus `[login]` an und hinterlegt den öffentlichen Schlüssel von root dieses Rechners bei root der Gegenstelle. Weder sshpass noch eine Eingabe sind nötig.
   - Spätere Läufe verwenden das Passwort nicht mehr.
3. Doppelte Hostnamen (häufig bei geklonten Rechnern) ändert es auf den Namen in der dritten Spalte. Ohne Doppelung ändert es nichts.
4. Da die Datei ein Passwort enthält, setzt es ihre Rechte automatisch auf nur für root lesbar (600). Sie gehört nicht zum Programmpaket und wird nicht auf andere Knoten synchronisiert. Sind die Schlüssel hinterlegt, kann das Passwort gelöscht werden.

Ist das Passwort leer und gibt es zwischen den Knoten noch keine passwortlose Anmeldung, wird einmal im Terminal danach gefragt (für alle Knoten gemeinsam).

### 4.1 Direkt über Befehlszeilenargumente

```bash
# 1 Rechner (Loopback der Netzwerkkarte)
sudo python3 tools/roce_onsite.py --ip 192.168.50.1

# 2 Rechner
sudo python3 tools/roce_onsite.py --ip 192.168.50.1 --peer root@node2 --peer-ip 192.168.50.2

# N Rechner (10 Knoten): in --peers jeden anderen Knoten als user@host=RoCE_IP
sudo python3 tools/roce_onsite.py --ip 192.168.50.1 \
     --peers root@n2=192.168.50.2,root@n3=192.168.50.3,...,root@n10=192.168.50.10 \
     --bench full
```

Argumente:

| Argument | Bedeutung |
|---|---|
| `--ip` | RoCE-IP dieses Rechners (fehlt sie, wird sie aus der RoCE-v2-GID ermittelt) |
| `--peers` | Die anderen Knoten, `user@host=ip` durch Kommas getrennt |
| `--bench quick\|full\|none` | `quick` (Standard): kurzer Lauf mit 30 s Stonewall. `full`: nach dem kurzen Lauf der **unveränderte offizielle 300-s-Lauf** (einreichbares Ergebnis). `none`: nur verteilen, nichts ausführen |
| `--force` | Weitermachen, auch wenn ein Schritt FAIL meldet |
| `--lib` | Build überspringen und vorhandene Bibliotheken nutzen |
| `--report` | Pfad des Berichts (Standard `onsite_report.json`) |

Jeder Schritt meldet PASS / WARN / FAIL; standardmäßig hält der Lauf beim ersten FAIL an. Die Vorprüfung behebt, was sich beheben lässt (RoCE-v2-Modus, memlock-Grenze, Namensauflösung der Hostnamen), und nennt bei allem anderen den Grund.

**Ausgabedateien**:

| Datei | Inhalt |
|---|---|
| `onsite_report.json` | Ergebnis und Details jedes Schritts |
| `onsite_io500.json` | IO500-Bericht des kurzen Laufs (mit Engpassanalyse und Prognose) |
| `onsite_io500_valid.json` | Bericht des Hauptlaufs mit `--bench full` |
| `onsite_cluster.json` | Beschreibung des Clusters. Damit lässt sich später mit beliebiger Konfiguration erneut ausführen: `ROCEPOOL_CLUSTER=onsite_cluster.json python3 tools/io500_run.py ...` |
| `roce_env.sh` | Von der Vorprüfung festgelegte Transportparameter (ToS/DSCP, busy-poll usw.) |
| `.bench_cache/io500_run/` | Rohausgaben: `io500.out`, die Server-Logs `mdt<Knoten>_<Port>.log` / `ost<Knoten>_<Port>.log`, die Client-Logs und `results/` von IO500 |

**Was der Lauf selbst entscheidet** (keine Handarbeit nötig):

1. **Client**: Er führt mit jedem der drei folgenden einen kurzen Lauf aus und nimmt den schnellsten. Alle drei Ergebnisse stehen im Bericht unter `client_probe`; der Hauptlauf übernimmt die Wahl des kurzen Laufs.
   - FUSE über den normalen Kanal
   - FUSE über io_uring (unterstützt der Kernel es, wird `fuse.enable_uring` automatisch eingeschaltet)
   - die LD_PRELOAD-Interception-Bibliothek
2. **NVMe-Schicht**: Auf jedem Knoten läuft `tools/nvme_preflight.py`.
   - Liegt ein Dateisystem auf NVMe, schreiben die OSTs ihre Daten dorthin durch. Eine Datei pro OST, die Größe ergibt sich automatisch aus dem freien Platz.
   - Sonst läuft alles nur im Arbeitsspeicher.
   - Die Entscheidung jedes Knotens steht im Bericht im Feld `ost_tier`.
   - Rohe Blockgeräte und SPDK werden **nie automatisch gewählt** (siehe Abschnitt 6).
3. **P2P-Speicher**: Sind alle folgenden Bedingungen erfüllt, werden die Anfragepuffer der OSTs aus dem CMB des Laufwerks belegt:
   - Kernel 6.2 oder neuer
   - Netzwerkkarte mlx5
   - das Laufwerk stellt einen CMB bereit
   - Netzwerkkarte und Laufwerk hängen am selben Root Port

   Lehnt die Netzwerkkarte die Registrierung ab, wird automatisch auf Host-Speicher zurückgefallen; das OST-Log vermerkt es in einer Zeile.

---

## 4b. Auf AWS ausführen

EC2 hat keine RoCE-Netzwerkkarten; es gibt zwei Wege. Beide erledigen alles mit **einem Befehl**.

Installieren Sie vorher die `aws`-CLI auf diesem Rechner und melden Sie sich an. Nötige Rechte:

- EC2: Instanzen, Sicherheitsgruppen, Schlüsselpaare und Platzierungsgruppen anlegen und löschen
- SSM: öffentliche Parameter lesen

```bash
# Weg 2: EFA (AWS-eigene RDMA-Netzwerkkarte, RDMA read in Hardware). libfabric-Transport-Backend; build.sh wählt es automatisch, wenn es EFA findet
python3 tools/aws_cluster.py run --mode efa --nodes 10 --region us-east-1

# Weg 1: Soft-RoCE (rxe). Der Code bleibt gleich, der Kernel macht eine normale Netzwerkkarte zum RoCE-v2-Gerät. Die CPU übernimmt die Arbeit der Karte, gemessen wird also die Software
python3 tools/aws_cluster.py run --mode rxe --nodes 3 --quick

python3 tools/aws_cluster.py list                       # Läufe, die noch Ressourcen belegen
python3 tools/aws_cluster.py down --run-id rp-2026...   # freigeben (nach --keep oder einem Abbruch)
```

Was automatisch geschieht:

1. Zugangsdaten und Region prüfen.
2. Ein Ubuntu-24.04-Image und eine Availability Zone wählen, in der der Instanztyp verfügbar ist.
3. Schlüsselpaar, Sicherheitsgruppe und cluster-Platzierungsgruppe anlegen.
   - Die Sicherheitsgruppe erlaubt jeden Verkehr zwischen den Mitgliedern (für EFA zwingend).
   - SSH ist nur von der öffentlichen IP dieses Rechners erlaubt.
4. N Instanzen starten (im efa-Modus jede mit einer EFA-Karte).
   - **Jede Instanz fährt nach `--max-hours` (Standard 6 Stunden) von selbst herunter und wird beendet.** Reißt die Verbindung zum eigenen Rechner ab, laufen keine Kosten weiter.
5. Jede Instanz einrichten und passwortloses root-Login zwischen den Knoten herstellen.
   - rxe: `rdma_rxe` laden, `rdma link add rxe0` ausführen, MTU auf 9001 setzen.
   - efa: den EFA-Installer von AWS (einschließlich libfabric) installieren.
6. Den Codebaum auf die erste Instanz übertragen und dort `./run.sh --peers <private IPs der anderen>` ausführen. Der Fortschritt wird live angezeigt.
   - Dazu gehören Vorprüfung, Build, Transportprüfung, Funktionstests, Benchmarks und IO500 über alle Knoten.
7. Die Berichte nach `aws_runs/<run-id>/` holen.
8. Die Instanzen beenden und alles Angelegte löschen (mit `--keep` bleibt es bestehen).

| Argument | Bedeutung |
|---|---|
| `--mode efa\|rxe` | Transportweg (Standard efa) |
| `--nodes` | Anzahl der Instanzen (Standard 10) |
| `--type` | Instanztyp. Standard bei efa `c6in.32xlarge` (128 vCPU, 200 Gb, EFA mit RDMA read), bei rxe `c6in.16xlarge`. Für lokales NVMe `i4i.32xlarge`. Im efa-Modus ist ein EFA-fähiger Typ Pflicht (sonst wird sofort abgelehnt und nichts gestartet) |
| `--quick` | Kurzer 30-s-Lauf (ohne: nach dem kurzen Lauf der offizielle 300-s-Hauptlauf) |
| `--max-hours` | Sicherheitsgrenze (Standard 6) |
| `--keep` | Nach dem Lauf nicht freigeben, ssh-Befehle anzeigen |
| `--self-test` | Die Unit-Tests auch auf dem Kopfknoten ausführen |
| `--az` / `--region` / `--ssh-cidr` / `--disk-gib` | Availability Zone, Region, für SSH erlaubter Adressbereich, Systemplatte |

Wenn Sie bereits selbst AWS-Instanzen gestartet haben:

- **EFA-fähiger Typ**: nach dem EFA-Installer direkt `sudo ./run.sh --peers ...`. EFA wird automatisch erkannt und das libfabric-Backend verwendet.
- **Normaler Typ**: `sudo ./run.sh --soft-roce --peers ...`. Auf jeder Instanz wird rxe0 automatisch angelegt.
- In beiden Fällen muss die Sicherheitsgruppe jeden Verkehr zwischen den Mitgliedern erlauben.

Der Unterschied der beiden Wege:

- **rxe**: Alle RDMA-Operationen erledigt die CPU, Bandbreite und Latenz liegen weit unter denen echter Karten. Geeignet, um Funktionen und den Ablauf über mehrere Knoten zu prüfen.
- **EFA**: echtes RDMA (SRD, zuverlässige Datagramme; Typen ab Nitro v4 mit RDMA read in Hardware). Die Ergebnisse sind aussagekräftig.
- EFA liefert ohne feste Reihenfolge aus; daher gilt:
  - Einseitiges Lesen wird so geprüft, dass die Reihenfolge keine Rolle spielt (erst die Daten, dann die Versionsnummer lesen).
  - Die einseitige Metadaten-Suche ist auf EFA standardmäßig aus (sie braucht drei Round-Trips und ist langsamer als ein RPC). Mit `ROCEPOOL_DIRECT_LOOKUP=1` lässt sie sich erzwingen.

Details in `docs/AWS_ROUND.md`.

---

## 4c. Direktes Lesen in die GPU (GPUDirect Storage)

Programme, die GPUs stark nutzen, etwa Rendering oder Training, können Daten aus rocepool direkt in den GPU-Speicher leiten.

- Die Netzwerkkarte schreibt die Daten aus dem OST-Speicher per RDMA direkt in den GPU-Speicher. Der Host-Speicher wird nicht berührt, die CPU fasst die Daten nicht an (wie die GDS-Unterstützung von Vendor W und DDN).
- Die Schnittstelle ist das NVIDIA-Standard-cuFile (`libcufile.so.0`). PyTorch/KvikIO, RAPIDS, DALI und Renderer, die mit cuFile lesen, brauchen keine Codeänderung.

```bash
# auf dem GPU-Knoten (build.sh hat roce/native/cufile/libcufile.so.0 gebaut)
export LD_LIBRARY_PATH=<rocepool-Verzeichnis>/roce/native/cufile:$LD_LIBRARY_PATH
export ROCEPOOL_MDTS=... ROCEPOOL_OSTS=...      # wie bei der Interception-Bibliothek
export ROCEPOOL_PREFIX=/mnt/rocepool            # FUSE-Einhängepunkt (oder Präfix der Interception-Bibliothek)
python3 ihr_programm                            # seine cuFile-Lese- und Schreibzugriffe gehen direkt zwischen rocepool und GPU-Speicher
```

- **Keine Handarbeit nötig.** `run.sh` hat einen Schritt `gds` (Ergebnis in `onsite_gds.json`, enthalten im Ergebnispaket).
  - Auf jedem Rechner prüft er:
    - GPU und Treiber.
    - Ob sich GPU-Speicher bei der Netzwerkkarte registrieren lässt. dma-buf braucht die Open-Source-Variante des Kernelmoduls; alternativ nvidia-peermem, das bei Bedarf automatisch per modprobe geladen wird.
    - Den PCIe-Abstand zwischen GPU und Netzwerkkarte. Hängen sie an verschiedenen CPU-Sockeln, gibt es einen Hinweis.
  - Auf Rechnern mit GPU misst er auch die Geschwindigkeit, jeweils in GB/s:
    - direkt in den GPU-Speicher.
    - über den Host-Speicher, wie bei Speicher ohne GDS-Unterstützung.
    - gleichzeitiges Lesen auf allen GPUs.
- Auf Rechnern ohne GPU meldet der Schritt "no NVIDIA GPU" und gilt nicht als Fehler.
- Lässt sich GPU-Speicher nicht registrieren, wird automatisch über den Host-Speicher gelesen. Die Ergebnisse bleiben korrekt, der Bericht erhält ein WARN.
- **KV-Cache für KI-Inferenz (Präfix-Cache, Trennung von Prefill und Decode)**: Die KV-API hat Aufrufe, die direkt mit dem GPU-Speicher austauschen. Sie eignen sich für KV-Blöcke.
  - In Python:
    - `buf = kv.register_device(tensor.data_ptr(), Byteanzahl)`
    - `kv.cached_prefix(Liste der Blockschlüssel)`: ermittelt mit einem Round-Trip pro OST, wie viele Blöcke des Präfixes im Cache liegen.
    - `buf.mget(...)` / `buf.mput(...)`
  - Die Netzwerkkarte bewegt die Daten per RDMA direkt in den GPU-Speicher und zurück, ohne Host-Speicher und ohne Metadatenserver.
  - Der Schritt `kvcache` von `run.sh` misst automatisch:
    - mit den KV-Größen echter Modelle: quick mit Llama-3-8B, 3072 von 4096 Tokens im Cache; full mit Llama-3-70B, 6144 von 8192 Tokens im Cache.
    - Speichern, Präfix-Abfrage und Lesen in den GPU-Speicher, wobei jeder Block geprüft wird.
    - Ausgabe: Leselatenz (p50/p99), GB/s und TTFT. Die TTFT wird mit Cache und bei vollständiger Neuberechnung verglichen. Die Prefill-Zeit wird aus der Rechenleistung des GPU-Modells geschätzt; der Bericht weist darauf hin.
    - Das Ergebnis steht in `onsite_kvcache.json`. Auf Rechnern ohne GPU wird nur die Stufe „Lesen in den Host-Speicher“ gemessen.
- Details in `docs/GPUDIRECT_STORAGE.md`.

---

## 5. Die Ergebnisse lesen

Am Ende eines Laufs erscheint eine Tabelle. Alle Daten stehen in `io500_report.json`.

```
phase            measured   emu%  rpc/op  hot MDT   projected   bottleneck
ior-easy-write   ...        ...                                 RAM to hold a 300 s write phase / NVMe tier write bandwidth
mdtest-hard-write ...                                           per-rank latency / CPU of the busiest node / ...
projected on ...: bw X GiB/s, md Y kIOPS, score Z
score range over the estimated inputs: a .. b
vs Vendor W (score 105.94): ...
what the fixes are worth (cumulative): ...
```

- **measured**: die in diesem Lauf tatsächlich erreichten Werte. Auf echter Hardware ist das Ihr Ergebnis (beim Hauptlauf die Zeile `[SCORE]`).
- **bottleneck**: die Grenze, an die die Phase zuerst stößt, geschätzt aus den gemessenen CPU-, RPC- und Byte-Kosten pro Operation.
- **projected**: Prognose, die die gemessenen Kosten pro Operation auf die Zielhardware überträgt (`--project`; auf echter Hardware standardmäßig `auto`, also der Rechner selbst).
  - Auf echter Hardware sollten Messung und Prognose fast übereinstimmen. Weichen sie stark ab, übersieht das Modell etwas.
  - Das Verhältnis steht in der Zeile "model check" der Tabelle.
- In Berichten von echter Hardware lohnen auch `node_facts`, `ost_tier`, `client_probe`, `fuse_channel_probe` und `dead_servers` (fällt ein Server aus, mit dem Ende seines Logs) einen Blick.

---

## 6. Manuelle Steuerung (optional)

Steht der Cluster nach `roce_onsite.py` (`onsite_cluster.json`), lässt sich `io500_run.py` direkt aufrufen.

```bash
export ROCEPOOL_CLUSTER=onsite_cluster.json
sudo -E python3 tools/io500_run.py --valid                      # offizieller 300-s-Hauptlauf
sudo -E python3 tools/io500_run.py --client intercept --ost-tier none   # Interception-Bibliothek + nur Arbeitsspeicher erzwingen
sudo -E python3 tools/io500_run.py --ost-tier file:/dev/nvme1n1 # dieselbe rohe Platte auf jedem Knoten (wird überschrieben!)
sudo -E python3 tools/io500_run.py --ost-tier spdk:0000:41:00.0 # SPDK-Backend (Build mit SPDK_DIR nötig, siehe unten)
```

Häufige Argumente:

| Argument | Bedeutung |
|---|---|
| `--client auto\|fuse\|intercept` | Welcher Client verwendet wird |
| `--direct-read auto\|on\|off` | Einseitiges Lesen und einseitige Suche. auto entscheidet per Probe, ob mit oder ohne schneller ist |
| `--bind-ranks auto\|numa\|core\|none` | Wie die Ranks gebunden werden |
| `--fuse-uring auto\|on\|off` | FUSE über io_uring |
| `--ost-tier auto\|none\|file:PATH[:GIB]\|spdk:TRADDR[:NSID]` | Kapazitätsschicht der OSTs. `{n}` steht für die Nummer des OST |
| `--ost-ram-cache-mib` | RAM-Cache der OSTs |
| `--ranks-per-node`, `--mdts-per-node`, `--osts-per-node` | Ranks, MDTs und OSTs pro Knoten |
| `--stonewall`, `--valid`, `--project`, `--report` | Stonewall-Zeit, Hauptlauf, Prognoseziel, Pfad des Berichts |

**SPDK-Backend**, Schritte (auf jedem Rechner):

1. SPDK bauen: `git clone https://github.com/spdk/spdk && cd spdk && git submodule update --init && ./configure && make`.
2. Das Ziel-NVMe an vfio-pci binden (`scripts/setup.sh`). Die Platte wird dabei vom Kernel getrennt.
3. `SPDK_DIR=/path/to/spdk bash roce/native/build.sh` ausführen.
4. Mit `--ost-tier spdk:<PCI-Adresse>` starten.

Der SPDK-Pfad baut nachweislich mit den Headern von v25.05, lief aber noch nicht auf Hardware. Prüfen Sie beim ersten Mal das OST-Log.

**Nur die lokalen Benchmarks ausführen** (1 Rechner, mit zwei Netzwerk-Namensräumen oder einer echten Karte):

```bash
sudo python3 tools/bench_ior.py --backend dist --hard                      # FUSE-Client
sudo python3 tools/bench_ior.py --backend dist --hard --client intercept   # Interception-Bibliothek
sudo python3 tools/io500_run.py --nodes 10                                 # 10 Knoten komplett auf diesem Rechner simulieren
```

**Ein beliebiges Programm nur mit der Interception-Bibliothek ausführen**:

```bash
LD_PRELOAD=roce/native/librocepool_intercept.so ROCEPOOL_PREFIX=/mnt/rocepool \
ROCEPOOL_MDTS=h1:19901,h2:19901 ROCEPOOL_OSTS=h1:19951,h2:19951 \
mpirun -n 32 ior -a POSIX -o /mnt/rocepool/f ...
```

`/mnt/rocepool` muss nicht existieren. Es ist nur ein Präfix; alle anderen Pfade gehen unverändert an die libc.

---

## 7. Umgebungsvariablen zur Laufzeit (alle mit Standardwerten, normalerweise nicht zu ändern)

| Variable | Zweck |
|---|---|
| `ROCEPOOL_OST_TIER`, `ROCEPOOL_OST_RAM_CACHE_MIB`, `ROCEPOOL_P2PMEM` | Kapazitätsschicht, RAM-Cache und P2P-Speicher der OSTs (`io500_run.py` setzt sie nach der Vorprüfung automatisch) |
| `ROCEPOOL_PREFIX`, `ROCEPOOL_MDTS`, `ROCEPOOL_OSTS`, `ROCEPOOL_IO_THREADS`, `ROCEPOOL_INTERCEPT_LOG` | Interception-Bibliothek |
| `ROCEPOOL_ATTR_TTL`, `ROCEPOOL_DIRTY_CAP_MIB`, `ROCEPOOL_SCACHE_SLOTS`, `ROCEPOOL_RPC_TIMEOUT_MS`, `ROCEPOOL_DIR_STRIPE`, `ROCEPOOL_DIRECT_READ` | Client-Kern: TTL des Attribut-Caches, Schreib-Cache pro Datei, Anzahl der Stripes des Lese-Caches, RPC-Timeout, gestreifte Verzeichnisse, einseitiges Lesen und einseitige Suche |
| `ROCEPOOL_NUMA_NODE`, `ROCE_PIN_THREADS`, `ROCEPOOL_MDT_SLOTS` | Server: Bindung an einen NUMA-Knoten (Kerne und Speicher), Bindung der Verbindungs-Threads an je einen Kern, Anzahl der Slots in der Tabelle für einseitige MDT-Suche (`io500_run.py` setzt sie passend zur Laufgröße) |
| `ROCE_TOS`, `ROCE_ACK_TIMEOUT`, `ROCE_POLL_SPIN_US`, `ROCE_BUFPOOL_MAX`, `ROCE_SERVER_MAX_INFLIGHT` | Transportschicht (die Vorprüfung schreibt sie in `roce_env.sh`) |
| `ROCE_TRANSPORT_LIB` | Verwendete Transportbibliothek (echte Karte `libroce_transport.so`, Emulator `softverbs/libroce_transport_softverbs.so`) |
| `ROCE_BACKEND` | Welchen Transport `build.sh` baut: `auto` (ofi, wenn ein EFA-Gerät da ist, sonst verbs) / `verbs` / `ofi`. `ROCE_LIBFABRIC` gibt das libfabric-Verzeichnis an (Standard `/opt/amazon/efa`; fehlt es, das Paket der Distribution installieren oder aus dem Quellcode bauen) |
| `ROCE_OFI_PROVIDER`, `ROCE_OFI_SERVER_EPS`, `ROCE_OFI_CLIENT_EPS`, `ROCE_OFI_POST_TIMEOUT_MS` | ofi-Backend: libfabric-Provider (mit EFA standardmäßig `efa`, sonst `tcp`), Endpunkte pro Server (Standard: Anzahl der Kerne), Endpunkte pro Client, Obergrenze für Wiederholungen beim Einstellen |
| `ROCEPOOL_DIRECT_LOOKUP` | Einseitige Metadaten-Suche. Standardmäßig an bei Transporten mit garantierter Reihenfolge (RC), aus bei solchen ohne (EFA). `1`/`0` erzwingt es |

---

## 8. Häufige Probleme

| Symptom | Abhilfe |
|---|---|
| FAIL in der Vorprüfung: RoCE-Modus / memlock | Die Vorprüfung als root behebt es automatisch. Geht das nicht, nennt sie den konkreten sysfs-/configfs-Pfad |
| Gleiche Hostnamen, oder sie lösen auf 127.x auf | Das Skript schreibt `/etc/hosts` automatisch passend. Schlägt das fehl, die Zeile wie angezeigt von Hand ergänzen |
| "IO500 did not start in 300 s" | Die MPI-Ranks erreichen einander nicht. ssh ohne Passwort, Firewall und Namensauflösung prüfen |
| Ein Server ist ausgefallen (`dead_servers`) | Das zugehörige `ost<Knoten>_<Port>.log` / `mdt<Knoten>_<Port>.log` und `kernel_log` im Bericht (OOM usw.) prüfen. Bei Speichermangel `--mem-budget-gib` überdenken |
| RPC-Timeout im Client (`timed out waiting for completion`) | Tritt bei stark überlasteten Rechnern auf. `ROCEPOOL_RPC_TIMEOUT_MS` erhöhen |
| Im OST-Log `ibv_reg_mr refused the supplied (P2P) chunk memory` | Der Treiber der Netzwerkkarte kann P2P-Seiten nicht registrieren. Es wird automatisch Host-Speicher verwendet; die Funktion ist nicht betroffen |
| `Write phase needed ...s instead of stonewall 300s` (ior-easy INVALID) | Die Daten passen nicht in 300 Sekunden. Am Laufbeginn nennt die Zeile `IOR data budget ... from the OSTs' capacity`, an der Kapazität welches OST es hängt. Dateisysteme auf NVMe (auch auf LVM/RAID) werden automatisch zur Schicht; gibt es keine, wird das größte lokale Dateisystem des Rechners verwendet. Auf rohe Platten wird nie automatisch geschrieben; bei Bedarf `--ost-tier file:/dev/nvmeXnY` angeben |
| Ein Server fiel während des Laufs aus | IO500 stoppt sofort und führt die übrigen Phasen nicht aus. `dead_servers[].kernel_log` im Bericht enthält das Kernel-Log dieses Rechners (OOM usw.) |
| Schreibfehler in der NVMe-Schicht `tier full` | Die Datei der Schicht ist zu klein. Mit `--ost-tier file:PATH:GIB` größer angeben |
| AWS: `has no EFA` | Der Typ unterstützt EFA nicht. Auf `c6in.32xlarge` / `i4i.32xlarge` / `hpc7a.96xlarge` o. Ä. wechseln oder `--mode rxe` verwenden |
| AWS: FAIL bei `libfabric` in der Vorprüfung | Der EFA-Installer ist nicht richtig installiert, oder die Sicherheitsgruppe erlaubt nicht jeden Verkehr zwischen den Mitgliedern |
| AWS: WARN bei `rdma_read` in der Vorprüfung | Der Typ ist nicht Nitro v4 oder neuer. libfabric emuliert RDMA read auf der CPU der Gegenseite. Alles funktioniert, einseitiges Lesen ist aber langsamer. Die Probe wählt automatisch zwischen einseitigem Lesen und Lesen per RPC |
| AWS: rxe lässt sich nicht anlegen | Dem Kernel fehlt `rdma_rxe`. `apt install linux-modules-extra-$(uname -r)` ausführen |
| AWS: Verbindung brach unterwegs ab | Die Instanzen beenden sich nach `--max-hours` selbst. Mit `aws_cluster.py down --run-id ...` lassen sie sich sofort freigeben |

---

## 9. Ergebnisse dieser Runde im Überblick (Details in `docs/KERNEL_BYPASS_ROUND.md`)

- **Interception-Bibliothek gegen FUSE** (Emulator mit 2 Kernen, ior-hard): Die Interception-Bibliothek ist beim Schreiben 1,9-mal, beim Lesen 2,6-mal und bei create 1,5-mal so schnell.
- **Simulation mit 10 Knoten** (`stress_reports/39`):
  - Gemessen 1,68 → 3,02.
  - Prognose für 10 × 32 Kerne / 100 GbE: 375 (nur Arbeitsspeicher) / 687 (NVMe-Schicht). Vendor W: 105,94.
  - Die Metadaten-Prognose ist eine Obergrenze; die Bandbreite bestimmt die tatsächliche Schreibbandbreite der Platten.
- **Prognose für gleiche Hardware (96 Kerne / 750 GB / 200G)**: 806 / 1 342. Der Vergleich nach Einzelwerten steht im Austausch während der Entwicklung.
- **Einseitiges Lesen und Suchen + NUMA-Bindung** (`docs/ONE_SIDED_ROUND.md`, `stress_reports/41`):
  - Gemessen in der Simulation mit 10 Knoten: 3,02 → 3,77.
  - Die stat-Phasen 2,5- bis 3-mal schneller, Prognose 375 → 449.
- **AWS** (`docs/AWS_ROUND.md`):
  - Ein EFA-Transport-Backend (libfabric) kam hinzu; alle Tests bestehen mit beiden Backends.
  - Mit dem einen Befehl `tools/aws_cluster.py` lässt sich ein Cluster starten und IO500 ausführen.
