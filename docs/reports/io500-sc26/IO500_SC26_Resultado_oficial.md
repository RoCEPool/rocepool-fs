# IO500 SC26: resultado oficial

El envío de RoCEPool a la lista IO500 SC26 ha sido aceptado y publicado por IO500.

| | |
|---|---|
| **Puntuación IO500** | **102,30** |
| Puntuación de ancho de banda | 12,29 GiB/s |
| Puntuación de metadatos | 851,18 kIOP/s |
| Posición | 101 de 162 en la lista SC26 Research |
| Nodos cliente / procesos | 6 nodos cliente con 32 procesos cada uno (192 en total) |
| Enviado | 3 de noviembre de 2026 (ejecutado el 2 de noviembre de 2026) |
| Entrada oficial | [https://io500.org/submissions/view/819](https://io500.org/submissions/view/819) |

## Resultados por fase

| Fase | Resultado | Unidad |
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

Cálculo: el ancho de banda es la media geométrica de las cuatro fases IOR; los metadatos, la de las ocho fases de mdtest y find; la puntuación IO500 es √(ancho de banda × metadatos). IO500 considera experimental la fase de lecturas aleatorias de 4 KiB, que no cuenta para la puntuación.

## Sistema

| | |
|---|---|
| Nombre del sistema en la entrada | RoCEPool2026 |
| Sistema de archivos | RoCEPool (código abierto, Apache 2.0), commit `4495b00` |
| Hardware | 6 × Amazon EC2 i3en.12xlarge, AWS us-east-2; cada nodo es cliente y servidor de almacenamiento a la vez (6 servidores de datos, SSD NVMe locales, Ethernet con EFA) |
| Capacidad de almacenamiento (según la entrada) | 128,3 TiB |
| Ejecución | ejecución de GitHub Actions `gh-41438027754` |
| Informes | Informe de envío: [RoCEPool_IO500_Informe_de_envío_gh-41438027754.pdf](RoCEPool_IO500_Informe_de_envío_gh-41438027754.pdf) · Resultados: [RoCEPool_IO500_Resultados_SC26_6_nodos.pdf](RoCEPool_IO500_Resultados_SC26_6_nodos.pdf) |

Otros idiomas: [English](IO500_SC26_Official_Result.md) · [日本語](IO500_SC26_公式結果.md) · [Deutsch](IO500_SC26_Offizielles_Ergebnis.md) · [Français](IO500_SC26_Résultat_officiel.md)
