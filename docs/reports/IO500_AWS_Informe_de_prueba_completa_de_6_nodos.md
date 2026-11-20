# Informe de prueba completa: 6 × i3en.12xlarge en AWS (ejecución 40999521729)

| | |
|---|---|
| Fecha del informe | 2026-10-31 |
| Objeto | RoCEPool (sistema de archivos distribuido basado en un pool de memoria RDMA), commit 0f85b7b de main |
| Plataforma | Amazon EC2 us-east-2, 6 × i3en.12xlarge (por nodo: 48 vCPU, 384 GiB de RAM, 4 × 7,5 TB NVMe, EFA de 50 Gb/s), Ubuntu, kernel 7.0.0-1014-aws, proveedor efa de libfabric |
| Ejecución | Ejecución de GitHub Actions 40999521729, 2026-10-30 03:49–05:48 UTC. Datos en bruto en la rama `aws-results/gh-40999521729` |
| Veredicto global | **WARN.** Todas las comprobaciones superadas. El IO500 de 300 segundos obtuvo 111,26, pero IO500 lo marcó como INVALID por una fase (sección 5) |
| Relacionado | Comparación IO500 con Vendor W: [IO500_AWS_Informe_de_resultados_de_6_nodos.md](IO500_AWS_Informe_de_resultados_de_6_nodos.md) · versión en inglés de este informe: [IO500_AWS_6NODE_FULL_RUN_REPORT.md](IO500_AWS_6NODE_FULL_RUN_REPORT.md) · otros idiomas: [日本語](IO500_AWS_6ノード全項目テストレポート.md) · [Deutsch](IO500_AWS_6-Knoten-Gesamttestbericht.md) · [繁體中文](IO500_AWS_6節點全項目測試報告.md) |

## 1. Resumen

Todo el proceso, desde el arranque de las seis instancias hasta su eliminación, se ejecutó de forma automática. Duración: 1 hora y 59 minutos; coste: unos 67 $.

- **Todas las pruebas funcionales y de carga superadas** (nodo principal). Los seis nodos superaron la comprobación previa y la compilación.
- **IO500, ejecución de 60 segundos:** 124,79 (ancho de banda 17,09 GiB/s, metadatos 911,2 kIOPS).
- **IO500, ejecución de 300 segundos:** **111,26** (ancho de banda 14,81 GiB/s, metadatos 835,9 kIOPS). Supera los 105,94 de Vendor W con 28 máquinas. Como mdtest-easy-write terminó de crear sus archivos a los 207 segundos, IO500 marcó la ejecución como INVALID. Las otras 11 fases cumplen las reglas. La causa está corregida (PR #35).
- **Una ejecución anterior el mismo día** (40984482522) se detuvo antes de IO500 por una condición de carrera al copiar el árbol a cinco pares a la vez. Se corrigió antes de esta ejecución (PR #33) y se añadió una prueba (PR #34).

| Paso | Veredicto | Resultado |
|---|---|---|
| Comprobación previa (principal + 5 pares) | WARN | Todo superado. WARN porque EFA en i3en no tiene RDMA read por hardware (emulado, como se esperaba) |
| Pares | PASS | SSH, root, direcciones y nombres de host únicos comprobados en los cinco |
| Compilación (principal + 5 pares) | PASS | Transporte (libfabric/EFA), cliente FUSE, biblioteca KV, cliente LD_PRELOAD, biblioteca cuFile |
| Transporte | PASS | 92,8 segundos de pruebas de transporte sobre EFA |
| Ajuste de busy polling | PASS | 50 µs elegidos |
| Pruebas funcionales | PASS | smoke, pool, MPI, HPC, stream |
| Prueba de carga concurrente | PASS | hasta 500 clientes, 0 % de errores, sin servidores colgados ni caídos |
| IOR + mdtest (FUSE) | PASS | 5 patrones de IOR y mdtest |
| ior-hard + mdtest-hard | PASS | las fases hard de IO500 a escala de comprobación funcional |
| API KV | PASS | cliente nativo y de Python |
| GPUDirect Storage | PASS | este tipo de instancia no tiene GPU; verificado con pruebas unitarias |
| Reutilización de la caché KV | PASS | TTFT 1,36 veces más rápido que recalcular |
| Preparación del clúster | WARN | Árbol y compilaciones de MPI/IO500 copiados a los cinco. WARN de la comprobación previa de pares igual que arriba |
| Comprobación previa de NVMe | PASS | Capa NVMe en todos los hosts, escritura O_DIRECT 1.971–2.488 MiB/s |
| IO500, 60 s | PASS | 124,79 |
| IO500, 300 s | WARN | 111,26, marcado INVALID (sección 5) |

## 2. Cronología

| Hora (UTC) | Evento |
|---|---|
| 03:49:42 | Inicio del trabajo. Límite de coste comprobado: como máximo 100,89 $ para 3 horas |
| 03:50:17 | Cuota de vCPU 300 ≥ 288 necesarias |
| 03:50:29 | Arranque de seis instancias |
| 03:54:10 | Las seis listas con EFA |
| 03:54:16 | Inicio de la ejecución local en el nodo principal: dependencias, compilación, comprobaciones |
| aprox. 04:43 | Ejecución IO500 de 60 segundos (696 segundos) |
| aprox. 04:55 | Ejecución IO500 de 300 segundos (3.112 segundos) |
| 05:47:58 | Informes recogidos |
| 05:48:33 | Todos los recursos liberados |

## 3. Comprobaciones en el nodo principal

Se hacen en un solo host con volúmenes pequeños. Comprueban funcionamiento y estabilidad, no el rendimiento del clúster.

### 3.1 Transporte y ajuste de busy polling

Las pruebas de transporte (pool de conexiones, recuperación de conexiones obsoletas, drenaje, contrapresión en recepción, límite de memoria registrada, correspondencia MDT/OST, atomicidad de compare-and-swap) pasaron en 92,8 segundos.

| Busy polling | 1 hilo (req/s) | Latencia p50 | 8 hilos (req/s) |
|---|---|---|---|
| 0 µs | 16.010 | 62 µs | 28.970 |
| 20 µs | 12.221 | 82 µs | 24.057 |
| **50 µs (elegido)** | **26.645** | **37 µs** | **38.518** |
| 100 µs | 26.864 | 37 µs | 39.502 |
| 200 µs | 26.648 | 37 µs | 39.106 |
| 400 µs | 26.713 | 37 µs | 39.206 |

### 3.2 Prueba de carga concurrente

El número de clientes subió de 1 a 500. En cada nivel: 0 % de errores, 14 hilos de servidor, sin sockets TIME_WAIT, sin desbordamiento de listen, sin servidores colgados ni caídos.

| Clientes | Metadatos (ops/s) | Metadatos en directorio compartido (ops/s) | Datos (ops/s) | Peor p99 (datos) |
|---|---|---|---|---|
| 1 | 6.702 | 6.737 | 505 | 3,9 ms |
| 10 | 10.823 | 10.597 | 1.347 | 19,6 ms |
| 50 | 40.467 | 40.225 | 4.814 | 28,1 ms |
| 100 | 60.558 | 63.443 | 7.493 | 37,7 ms |
| 200 | 74.842 | 82.852 | 9.372 | 67,6 ms |
| 500 | 79.043 | 91.665 | 9.037 | 219,5 ms |

### 3.3 IOR y mdtest a través del cliente FUSE

| Patrón | Escritura (MiB/s) | Lectura (MiB/s) |
|---|---|---|
| Archivo compartido, 1 MiB | 58,1 | 863,6 |
| Archivo compartido, 64 KiB | 308,5 | 1.013,2 |
| Archivo por proceso, 1 MiB | 880,8 | 1.765,2 |
| Archivo compartido, 4 MiB, gran volumen | 783,4 | 1.505,3 |
| Archivo por proceso, 256 KiB | 446,3 | 1.660,8 |
| Forma de ior-hard (47.008 B) | 152,9 | 265,0 |

mdtest: creación 5.804/s, stat 1.030.542/s, lectura 87.509/s, borrado 13.620/s. Forma de mdtest-hard: creación 5.122/s, stat 322.050/s.

### 3.4 API KV y reutilización de la caché KV

| Cliente | Operación | Hilos | Valor | ops/s | p50 |
|---|---|---|---|---|---|
| nativo | get | 1 | 64 B | 16.278 | 54,5 µs |
| Python | get | 1 | 64 B | 12.525 | 68,2 µs |
| nativo | put | 1 | 64 B | 15.683 | 52,9 µs |
| nativo | get | 16 | 64 B | 22.251 | 72,7 µs |
| nativo | multi-get, 64 claves | 16 | 64 B | 242.048 claves/s | 3,5 ms |
| nativo | get | 1 | 4 KiB | 17.290 | 57,3 µs |
| nativo | multi-get, 64 claves | 16 | 4 KiB | 130.406 claves/s | 7,1 ms |

Reutilización de la caché KV (llama3-70b, 6.144 de 8.192 tokens en caché, KV de 1.920 MiB): carga a 2,1 GB/s en 3,6 segundos (p50). Tiempo hasta el primer token: 5,45 segundos, 1,36 veces más rápido que los 7,41 segundos de recalcular (prefill estimado).

## 4. Preparación del clúster y capa NVMe

El árbol y las compilaciones de MPI/IO500 se copiaron en paralelo a los cinco pares, y cada par hizo su propia comprobación previa y compilación. Los relojes de todos los hosts se desviaban como mucho 0,146 segundos del principal.

| Host | Capa NVMe | Escritura O_DIRECT (MiB/s) |
|---|---|---|
| Principal (172.31.7.70) | RAID 0 de 4 unidades, archivo de capa de 21,9 TB | 2.232 |
| 172.31.1.244 | igual | 1.971 |
| 172.31.11.38 | igual | 2.144 |
| 172.31.9.9 | igual | 2.062 |
| 172.31.1.247 | igual | 2.488 |
| 172.31.10.247 | igual | 2.026 |

## 5. IO500

192 ranks (32 por nodo), cliente de interceptación LD_PRELOAD (elegido por la sonda de clientes), 6 MDT, 6 OST.

| Fase | Ejecución de 60 s | Ejecución de 300 s | Duración en la de 300 s (s) | Uso de CPU del host |
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
| **Puntuación de ancho de banda (GiB/s)** | 17,09 | **14,81** | | |
| **Puntuación de metadatos (kIOPS)** | 911,2 | **835,9** | | |
| **Puntuación IO500** | 124,79 | **111,26** | total 3.112 | |

El uso de CPU del host es la parte usada de las 48 vCPU del nodo 0 durante cada fase de la ejecución de 300 segundos.

**Por qué la ejecución de 300 segundos se marcó como INVALID.** mdtest-easy-write terminó de crear los archivos indicados (1 millón por rank, 192 millones en total) a los 207 segundos y se detuvo antes del stonewall de 300 segundos. El valor por defecto de 1 millón viene de cuando un rank creaba unos 600 archivos por segundo; ahora crea unos 4.900. El informe en tiempo de ejecución atribuyó la causa al volumen de datos de ior-easy-write, lo que también era erróneo: a ior-easy-write se le asignaron 59.006 GiB para 5.633 GiB necesarios. **Corrección (PR #35):** el número de archivos por rank de mdtest se calcula a partir de la velocidad medida en la ejecución de 60 segundos (en este caso, unos 2,2 millones por rank). El informe también indica ahora la fase que de verdad se quedó corta.

**Volúmenes de datos en la ejecución de 300 segundos.** El límite de capacidad de los OST es de 118.013 GiB (lo fija la capa más pequeña, de 21,9 TB). A partir de las velocidades de la ejecución de 60 segundos se asignaron 59.006 GiB a ior-easy y 3.297 GiB a ior-hard. La tabla de búsqueda de cada MDT tiene 67.108.864 ranuras, y la capacidad en línea para archivos pequeños es de 57.400 MiB por MDT.

## 6. La ejecución anterior (40984482522)

La primera ejecución de 6 nodos del día superó todas las comprobaciones del nodo principal y luego falló al copiar el árbol a uno de los cinco pares con `tar: .: file changed as we read it`. La comprobación «¿es la misma máquina?» de cada par creaba y borraba un archivo de testigo en el directorio raíz del árbol, y eso coincidió con la lectura de tar para otro par. Con 1–3 pares no coincidía; con 5, sí. Las instancias se liberaron a los 37 minutos, antes de empezar IO500 (20,6 $).

- **Corrección (PR #33):** el testigo va en un directorio excluido del archivo, con un archivo distinto por comprobación. Una salida de tar por «file changed» se trata como éxito. Una copia fallida se reintenta una vez.
- **Prueba (PR #34):** copia simultánea a cinco pares con un árbol realista de 20 MB y comprobaciones desfasadas. Con el código antiguo falla (3 de 3); tras la corrección, pasa. Una prueba de reproducción aislada no muestra fallos con 1–31 pares tras la corrección.

## 7. Coste

| Concepto | Importe |
|---|---|
| 6 × i3en.12xlarge (disco gp3 de 400 GiB incluido) | 33,63 $/hora |
| Esta ejecución, 1 hora y 59 minutos de instancias | aprox. 67 $ |
| Ejecución anterior, 37 minutos | aprox. 20,6 $ |

El coste se calcula a partir del tiempo de las instancias con precios bajo demanda de AWS; no es el importe facturado por AWS.
