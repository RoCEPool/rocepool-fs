# IO500 SC26: offizielles Ergebnis

Die Einreichung von RoCEPool für die IO500-Liste SC26 wurde von IO500 angenommen und veröffentlicht.

| | |
|---|---|
| **IO500-Ergebnis** | **102,30** |
| Bandbreitenergebnis | 12,29 GiB/s |
| Metadatenergebnis | 851,18 kIOP/s |
| Platz | 101 von 162 auf der SC26-Research-Liste |
| Client-Knoten / Prozesse | 6 Client-Knoten mit je 32 Prozessen (insgesamt 192) |
| Eingereicht | 3. November 2026 (Lauf am 2. November 2026) |
| Offizieller Eintrag | [https://io500.org/submissions/view/819](https://io500.org/submissions/view/819) |

## Ergebnisse nach Phase

| Phase | Ergebnis | Einheit |
|---|---:|---|
| ior-easy-write | 16,44 | GiB/s |
| ior-easy-read | 26,87 | GiB/s |
| ior-hard-write | 3,56 | GiB/s |
| ior-hard-read | 14,54 | GiB/s |
| mdtest-easy-write | 730,66 | kIOP/s |
| mdtest-easy-stat | 969,38 | kIOP/s |
| mdtest-easy-delete | 747,96 | kIOP/s |
| mdtest-hard-write | 267,96 | kIOP/s |
| mdtest-hard-read | 889,31 | kIOP/s |
| mdtest-hard-stat | 982,08 | kIOP/s |
| mdtest-hard-delete | 682,54 | kIOP/s |
| find | 3.256,07 | kIOP/s |
| ior-rnd4K-easy-read | 2,06 | GiB/s |

Berechnung: Die Bandbreite ist das geometrische Mittel der vier IOR-Phasen, die Metadaten das der acht mdtest- und find-Phasen, das IO500-Ergebnis ist √(Bandbreite × Metadaten). Die Phase mit zufälligen 4-KiB-Lesezugriffen gilt bei IO500 als experimentell und zählt nicht zum Ergebnis.

## System

| | |
|---|---|
| Systemname im Eintrag | RoCEPool2026 |
| Dateisystem | RoCEPool (Open Source, Apache 2.0), Commit `4495b00` |
| Hardware | 6 × Amazon EC2 i3en.12xlarge, AWS us-east-2; jeder Knoten ist zugleich Client und Speicherserver (6 Datenserver, lokale NVMe-SSDs, Ethernet mit EFA) |
| Speicherkapazität (laut Eintrag) | 128,3 TiB |
| Lauf | GitHub-Actions-Lauf `gh-41438027754` |
| Berichte | Einreichungsbericht: [RoCEPool_IO500_Einreichungsbericht_gh-41438027754.pdf](RoCEPool_IO500_Einreichungsbericht_gh-41438027754.pdf) · Messergebnisse: [RoCEPool_IO500_Messergebnisse_SC26_6_Knoten.pdf](RoCEPool_IO500_Messergebnisse_SC26_6_Knoten.pdf) |

Weitere Sprachen: [English](IO500_SC26_Official_Result.md) · [日本語](IO500_SC26_公式結果.md) · [Español](IO500_SC26_Resultado_oficial.md) · [Français](IO500_SC26_Résultat_officiel.md)
