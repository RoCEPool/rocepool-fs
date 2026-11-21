# IO500 SC26: official result

RoCEPool's submission to the IO500 SC26 list has been accepted and published by IO500.

| | |
|---|---|
| **IO500 score** | **102.30** |
| Bandwidth score | 12.29 GiB/s |
| Metadata score | 851.18 kIOP/s |
| Rank | 101st of 162 on the SC26 Research list |
| Client nodes / processes | 6 client nodes, 32 processes each (192 in total) |
| Submitted | 3 November 2026 (run on 2 November 2026) |
| Official entry | [https://io500.org/submissions/view/819](https://io500.org/submissions/view/819) |

## Results by phase

| Phase | Result | Unit |
|---|---:|---|
| ior-easy-write | 16.44 | GiB/s |
| ior-easy-read | 26.87 | GiB/s |
| ior-hard-write | 3.56 | GiB/s |
| ior-hard-read | 14.54 | GiB/s |
| mdtest-easy-write | 730.66 | kIOP/s |
| mdtest-easy-stat | 969.38 | kIOP/s |
| mdtest-easy-delete | 747.96 | kIOP/s |
| mdtest-hard-write | 267.96 | kIOP/s |
| mdtest-hard-read | 889.31 | kIOP/s |
| mdtest-hard-stat | 982.08 | kIOP/s |
| mdtest-hard-delete | 682.54 | kIOP/s |
| find | 3,256.07 | kIOP/s |
| ior-rnd4K-easy-read | 2.06 | GiB/s |

Scores: bandwidth is the geometric mean of the four IOR phases, metadata that of the eight mdtest and find phases, and the IO500 score is √(bandwidth × metadata). The random 4 KiB read phase is marked experimental by IO500 and is not part of the score.

## System

| | |
|---|---|
| System name in the entry | RoCEPool2026 |
| File system | RoCEPool (open source, Apache 2.0), commit `4495b00` |
| Hardware | 6 × Amazon EC2 i3en.12xlarge, AWS us-east-2; each node is both client and storage server (6 data servers, local NVMe SSD, Ethernet with EFA) |
| Storage capacity (as listed) | 128.3 TiB |
| Run | GitHub Actions run `gh-41438027754` |
| Reports | Submission report: [RoCEPool_IO500_Submission_Report_gh-41438027754.pdf](RoCEPool_IO500_Submission_Report_gh-41438027754.pdf) · Results summary: [RoCEPool_IO500_Results_SC26_6node.pdf](RoCEPool_IO500_Results_SC26_6node.pdf) |

Other languages: [日本語](IO500_SC26_公式結果.md) · [Deutsch](IO500_SC26_Offizielles_Ergebnis.md) · [Español](IO500_SC26_Resultado_oficial.md) · [Français](IO500_SC26_Résultat_officiel.md)
