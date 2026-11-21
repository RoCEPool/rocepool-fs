# IO500 SC26 : résultat officiel

La soumission de RoCEPool à la liste IO500 SC26 a été acceptée et publiée par IO500.

| | |
|---|---|
| **Score IO500** | **102,30** |
| Score de bande passante | 12,29 Gio/s |
| Score de métadonnées | 851,18 kIOP/s |
| Classement | 101e sur 162 dans la liste SC26 Research |
| Nœuds clients / processus | 6 nœuds clients de 32 processus chacun (192 au total) |
| Soumis le | 3 novembre 2026 (exécution le 2 novembre 2026) |
| Entrée officielle | [https://io500.org/submissions/view/819](https://io500.org/submissions/view/819) |

## Résultats par phase

| Phase | Résultat | Unité |
|---|---:|---|
| ior-easy-write | 16,44 | Gio/s |
| ior-easy-read | 26,87 | Gio/s |
| ior-hard-write | 3,56 | Gio/s |
| ior-hard-read | 14,54 | Gio/s |
| mdtest-easy-write | 730,66 | kIOP/s |
| mdtest-easy-stat | 969,38 | kIOP/s |
| mdtest-easy-delete | 747,96 | kIOP/s |
| mdtest-hard-write | 267,96 | kIOP/s |
| mdtest-hard-read | 889,31 | kIOP/s |
| mdtest-hard-stat | 982,08 | kIOP/s |
| mdtest-hard-delete | 682,54 | kIOP/s |
| find | 3 256,07 | kIOP/s |
| ior-rnd4K-easy-read | 2,06 | Gio/s |

Calcul : la bande passante est la moyenne géométrique des quatre phases IOR, les métadonnées celle des huit phases mdtest et find, et le score IO500 vaut √(bande passante × métadonnées). La phase de lectures aléatoires de 4 Kio est expérimentale selon IO500 et n’entre pas dans le score.

## Système

| | |
|---|---|
| Nom du système dans l’entrée | RoCEPool2026 |
| Système de fichiers | RoCEPool (open source, Apache 2.0), commit `4495b00` |
| Matériel | 6 × Amazon EC2 i3en.12xlarge, AWS us-east-2 ; chaque nœud est à la fois client et serveur de stockage (6 serveurs de données, SSD NVMe locaux, Ethernet avec EFA) |
| Capacité de stockage (selon l’entrée) | 128,3 Tio |
| Exécution | exécution GitHub Actions `gh-41438027754` |
| Rapports | Rapport de soumission (en anglais) : [RoCEPool_IO500_Submission_Report_gh-41438027754.pdf](RoCEPool_IO500_Submission_Report_gh-41438027754.pdf) · Résultats (en anglais) : [RoCEPool_IO500_Results_SC26_6node.pdf](RoCEPool_IO500_Results_SC26_6node.pdf) |

Autres langues : [English](IO500_SC26_Official_Result.md) · [日本語](IO500_SC26_公式結果.md) · [Deutsch](IO500_SC26_Offizielles_Ergebnis.md) · [Español](IO500_SC26_Resultado_oficial.md)
