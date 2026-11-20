# Manual de ejecución de rocepool

rocepool es un sistema de archivos distribuido basado en un pool de memoria sobre RDMA/RoCE. Se compone de:

- servidores MDT (metadatos) y OST (datos) escritos en C nativo
- tres clientes: un cliente FUSE en C, una biblioteca de interceptación LD_PRELOAD y una implementación de referencia en Python
- una capa de capacidad NVMe para los OST (backend O_DIRECT del kernel o SPDK, con P2PDMA opcional)
- automatización completa: comprobación previa, compilación, despliegue, arranque de los servicios, ejecución oficial de IO500 e informe con análisis de cuellos de botella

Este manual solo explica cómo ejecutarlo. El diseño y los resultados de cada ronda están en `README.md` y `docs/` (la ronda más reciente: `docs/AWS_ROUND.md`).

**La forma más sencilla de empezar**:

- Descomprimir y ejecutar `sudo ./run.sh`. Con una tarjeta de red RDMA se ejecuta sobre hardware real; sin ella, como simulación de 10 nodos.
- En AWS: `python3 tools/aws_cluster.py run ...` (sección 4b).

> Otros idiomas (mismo contenido): [中文](运行说明书.md) · [日本語](実行マニュアル.md) · [Deutsch](Ausführungshandbuch.md) · [繁體中文](執行手冊.md)

---

## 1. Estructura de directorios

| Directorio | Contenido |
|---|---|
| `roce/native/` | Código C. Archivos principales: `roce_transport.c` (transporte RDMA con verbs/rdma_cm para tarjetas RoCE y Soft-RoCE), `ofi_transport.c` (transporte libfabric con la misma interfaz, para AWS EFA), `rocepool_servers.c` (servidores MDT/OST), `ost_tier.c` (capa de capacidad NVMe de los OST), `rocepool_fs.c` (núcleo del cliente), `rocepool_fuse.c` (cliente FUSE), `rocepool_intercept.c` (cliente de interceptación LD_PRELOAD), `rocepool_kv.c` (API KV nativa), `rocepool_cufile.c` (lectura directa a la GPU, interfaz cuFile), `build.sh` (compilación para tarjetas reales), `softverbs/` (emulador RDMA en espacio de usuario, para equipos sin tarjeta de red) |
| `roce/`, `fs/`, `storage_engine/` | Implementación de referencia en Python (protocolo, MDT, distribución de datos en franjas, capa POSIX) |
| `tools/` | Automatización: `roce_onsite.py` (hardware real con un solo comando), `io500_run.py` (IO500 en N nodos), `bench_ior.py` (IOR/mdtest local), `roce_preflight.py` (comprobación previa de RoCE), `nvme_preflight.py` (comprobación previa de NVMe/P2P), `io500_model.py` (análisis de cuellos de botella y proyección), `aws_cluster.py` (AWS con un solo comando: arrancar → preparar el entorno → ejecutar → recoger los informes → liberar), `bench_gds.py` (comprobación de GPU y medición de la lectura directa), `bench_kvcache.py` (reutilización de la caché KV y TTFT) |
| `tests/` | 465 pruebas (`python3 -m unittest discover -s tests -t .`) |
| `stress_reports/` | Informes de ejecuciones anteriores (JSON). Los números siguen el orden cronológico; el más reciente es `39_io500_sim10_intercept.json` |
| `docs/` | Documentos de diseño y resultados de cada ronda |

---

## 2. Requisitos

**Se puede usar sin conexión.** El directorio `deps/` del paquete contiene el código fuente de todos los componentes de terceros.

- Incluye: MPICH 4.2.3, IO500 oficial (con IOR, pfind, lz4 y las herramientas de esquema en versiones fijas), IOR, libfabric 2.1.0 y rdma-core 59.2.
- IOR ya viene con configure generado, así que en sitio no hacen falta autotools ni acceso a internet. La compilación lo toma de aquí automáticamente (`tools/deps.py`).
- Con `ROCEPOOL_OFFLINE=1` se prohíbe por completo descargar de la red. Si falta algo, en lugar de quedarse esperando una descarga, indica al momento qué falta.
- El cliente FUSE no depende de la versión de las cabeceras del kernel del sistema, así que también compila en Ubuntu 22.04 (kernel 5.15).
- Para actualizar estas fuentes, ejecute `tools/make_deps.sh` en un equipo con acceso a internet.

**Cada máquina** (al desplegar sobre hardware real, todos los nodos deben cumplirlo):

- Linux x86_64, kernel 5.x o posterior. FUSE sobre io_uring requiere 6.14 o posterior; P2PDMA, 6.2 o posterior.
- Permisos de root (para montar FUSE, los espacios de nombres y ajustar los parámetros de RoCE).
- `python3` 3.9 o posterior, `gcc`, `make`, `git`, `cmake` (para compilar rdma-core automáticamente).
- Con tarjetas reales: los paquetes de desarrollo de rdma-core (`libibverbs-dev librdmacm-dev` / `rdma-core-devel`). `build.sh` los instala por sí mismo o los compila desde el código fuente.
- Las máquinas que ejecutan IO500 necesitan además `curl` y `autoconf automake libtool pkg-config`. MPICH e IOR/IO500 se descargan y compilan automáticamente en la primera ejecución, se guardan en `.bench_cache/` y después se reutilizan.
- Con varias máquinas:
  - Desde esta máquina debe poder hacerse ssh sin contraseña a cada nodo (`root@host`).
  - Todos los nombres de host deben ser distintos.
  - El árbol de código debe estar **en la misma ruta absoluta en todas las máquinas** (el script lo sincroniza automáticamente).

**Equipo de desarrollo sin tarjeta de red**: basta con todo lo anterior salvo rdma-core. Las pruebas y el clúster simulado funcionan por completo con el emulador `softverbs`.

---

## 3. Primero: la autoprueba

```bash
cd rocepool
sudo python3 -m unittest discover -s tests -t .      # unos 5 minutos, 465 pruebas
```

Si todo da OK, el entorno está bien. Las pruebas hacen automáticamente lo siguiente:

- Compilan la biblioteca del emulador, el cliente FUSE y la biblioteca de interceptación.
- Arrancan los servidores en C en local y montan un FUSE real.
- Ejecutan IOR/mdtest reales (si ya están en `.bench_cache`).

---

## 4. Despliegue en hardware real e IO500 (el procedimiento principal)

**Un solo comando lo hace todo.** El flujo es:

1. Comprobación previa, compilación y verificación de la capa de transporte
2. Sincronización a cada nodo y, en cada uno, comprobación previa y compilación
3. Arranque de N MDT y N OST
4. Sondeo de los clientes
5. Ejecución oficial de IO500
6. Informe y limpieza

### 4.0 El archivo de configuración del clúster cluster.conf (recomendado, sin límite de nodos)

Si todos los nodos están en un archivo, basta con ejecutar `sudo ./run.sh` en cualquiera de ellos y todo sigue hasta el final. No hace falta `--peers` ni se pide ninguna contraseña.

```bash
cp cluster.conf.example cluster.conf     # y después editarlo
sudo ./run.sh                            # usa cluster.conf si existe; otro archivo con --conf
```

```
[login]
# usuario con permisos de sudo en cada nodo y su contraseña (igual en todos los nodos)
# atención: no escriba un comentario detrás de la línea password; el # también formaría parte de la contraseña
user = admin
password = xxxx
[nodes]
# dirección SSH      IP RoCE            nombre de host (opcional)
10.0.0.11       10.10.0.11      node01
10.0.0.12      10.10.0.12     node02
...                                   # 100 nodos: 100 líneas
```

run.sh hace automáticamente lo siguiente (`tools/cluster_conf.py`, de 32 en 32 nodos en paralelo):

1. Por la IP RoCE determina qué línea es esta misma máquina; todas las demás son los nodos remotos. Su propia IP RoCE se usa también como `--ip`.
2. Prepara el acceso sin contraseña de root en esta máquina a root en cada nodo remoto.
   - En los nodos a los que ya se accede sin contraseña no hace nada.
   - En los demás inicia sesión **una sola vez** con el usuario y la contraseña de `[login]` y registra la clave pública de root de esta máquina en root del nodo remoto. No hacen falta sshpass ni que nadie escriba nada.
   - Las ejecuciones posteriores ya no usan la contraseña.
3. Si hay nombres de host repetidos (habitual en máquinas clonadas), los cambia por el nombre de la tercera columna. Si no se repiten, no cambia nada.
4. Como el archivo contiene una contraseña, cambia sus permisos automáticamente para que solo root pueda leerlo (600). No forma parte del paquete del programa ni se sincroniza a otros nodos. Una vez registradas las claves públicas, se puede borrar la contraseña.

Si la contraseña está vacía y todavía no hay acceso sin contraseña entre los nodos, se pide una vez en la terminal (común a todos los nodos).

### 4.1 Indicarlo directamente con argumentos de línea de comandos

```bash
# 1 máquina (bucle local de la tarjeta de red)
sudo python3 tools/roce_onsite.py --ip 192.168.50.1

# 2 máquinas
sudo python3 tools/roce_onsite.py --ip 192.168.50.1 --peer root@node2 --peer-ip 192.168.50.2

# N máquinas (10 nodos): en --peers, cada nodo distinto de este como user@host=RoCE_IP
sudo python3 tools/roce_onsite.py --ip 192.168.50.1 \
     --peers root@n2=192.168.50.2,root@n3=192.168.50.3,...,root@n10=192.168.50.10 \
     --bench full
```

Argumentos:

| Argumento | Significado |
|---|---|
| `--ip` | IP RoCE de esta máquina (si se omite, se detecta a partir del GID de RoCE v2) |
| `--peers` | Los demás nodos, `user@host=ip` separados por comas |
| `--bench quick\|full\|none` | `quick` (predeterminado): ejecución corta con stonewall de 30 s. `full`: tras la ejecución corta, la **ejecución oficial de 300 s sin modificar** (resultado apto para envío). `none`: solo desplegar, sin ejecutar |
| `--force` | Continuar aunque algún paso dé FAIL |
| `--lib` | Omitir la compilación y usar las bibliotecas existentes |
| `--report` | Ruta del informe (predeterminada `onsite_report.json`) |

Cada paso informa PASS / WARN / FAIL y, por defecto, se detiene en el primer FAIL. La comprobación previa corrige lo que se puede corregir (modo RoCE v2, límite de memlock, resolución de nombres de host) y explica el motivo de lo que no.

**Archivos de salida**:

| Archivo | Contenido |
|---|---|
| `onsite_report.json` | Resultado y detalles de cada paso |
| `onsite_io500.json` | Informe IO500 de la ejecución corta (con análisis de cuellos de botella y proyección) |
| `onsite_io500_valid.json` | Informe de la ejecución principal con `--bench full` |
| `onsite_cluster.json` | Descripción del clúster. Permite volver a ejecutar después con cualquier configuración: `ROCEPOOL_CLUSTER=onsite_cluster.json python3 tools/io500_run.py ...` |
| `roce_env.sh` | Parámetros de transporte fijados por la comprobación previa (ToS/DSCP, busy-poll, etc.) |
| `.bench_cache/io500_run/` | Salida en bruto: `io500.out`, los registros de cada servidor `mdt<nodo>_<puerto>.log` / `ost<nodo>_<puerto>.log`, los registros de los clientes y `results/` de IO500 |

**Lo que se decide automáticamente durante la ejecución** (sin trabajo manual):

1. **Cliente**: hace una ejecución corta con cada uno de los tres siguientes y usa el más rápido. Los tres resultados están en `client_probe` del informe, y la ejecución principal mantiene lo elegido en la corta.
   - FUSE por el canal normal
   - FUSE sobre io_uring (si el kernel lo admite, activa `fuse.enable_uring` automáticamente)
   - la biblioteca de interceptación LD_PRELOAD
2. **Capa NVMe**: en cada nodo se ejecuta `tools/nvme_preflight.py`.
   - Si hay un sistema de archivos sobre NVMe, los OST escriben sus datos en él (escritura directa). Un archivo por OST, con el tamaño calculado automáticamente según el espacio libre.
   - Si no, todo funciona solo en memoria.
   - La decisión de cada nodo queda en el campo `ost_tier` del informe.
   - Los dispositivos de bloque en bruto y SPDK **nunca se eligen automáticamente** (véase la sección 6).
3. **Memoria P2P**: si se cumplen todas las condiciones siguientes, los búferes de petición de los OST se asignan desde el CMB del disco:
   - kernel 6.2 o posterior
   - tarjeta de red mlx5
   - el disco expone un CMB
   - la tarjeta y el disco cuelgan del mismo root port

   Si la tarjeta rechaza el registro, se vuelve automáticamente a la memoria del host y el registro del OST lo anota en una línea.

---

## 4b. Ejecutar en AWS

EC2 no tiene tarjetas RoCE, así que hay dos caminos. Ambos lo hacen todo con **un solo comando**.

Antes, instale la CLI `aws` en esta máquina e inicie sesión. Permisos necesarios:

- EC2: crear y eliminar instancias, grupos de seguridad, pares de claves y grupos de ubicación
- SSM: leer parámetros públicos

```bash
# Camino 2: EFA (tarjeta RDMA propia de AWS, RDMA read por hardware). Backend de transporte libfabric; build.sh lo elige automáticamente si encuentra EFA
python3 tools/aws_cluster.py run --mode efa --nodes 10 --region us-east-1

# Camino 1: Soft-RoCE (rxe). El código no cambia; el kernel convierte una tarjeta normal en un dispositivo RoCE v2. La CPU hace el trabajo de la tarjeta, así que lo que se mide es el software
python3 tools/aws_cluster.py run --mode rxe --nodes 3 --quick

python3 tools/aws_cluster.py list                       # ejecuciones que aún usan recursos
python3 tools/aws_cluster.py down --run-id rp-2026...   # liberar (tras --keep o una interrupción)
```

Lo que se hace automáticamente:

1. Comprobar las credenciales y la región.
2. Elegir una imagen de Ubuntu 24.04 y una zona de disponibilidad donde exista el tipo de instancia.
3. Crear el par de claves, el grupo de seguridad y el grupo de ubicación cluster.
   - El grupo de seguridad permite todo el tráfico entre sus miembros (obligatorio para EFA).
   - SSH solo se permite desde la IP pública de esta máquina.
4. Arrancar N instancias (en modo efa, cada una con una tarjeta EFA).
   - **Cada instancia se apaga y termina sola al alcanzar `--max-hours` (6 horas por defecto).** Si se corta la conexión con su equipo, el cobro no continúa.
5. Preparar cada instancia y configurar el acceso de root sin contraseña entre los nodos.
   - rxe: cargar `rdma_rxe`, ejecutar `rdma link add rxe0` y fijar la MTU en 9001.
   - efa: instalar el instalador de EFA de AWS (incluye libfabric).
6. Transferir el árbol de código a la primera instancia y ejecutar allí `./run.sh --peers <IP privadas de las demás>`. El progreso se muestra en tiempo real.
   - Incluye comprobación previa, compilación, verificación del transporte, pruebas funcionales, benchmarks e IO500 en todos los nodos.
7. Recoger los informes en `aws_runs/<run-id>/`.
8. Terminar las instancias y eliminar todo lo creado (con `--keep` se conserva).

| Argumento | Significado |
|---|---|
| `--mode efa\|rxe` | Método de transporte (efa por defecto) |
| `--nodes` | Número de instancias (10 por defecto) |
| `--type` | Tipo de instancia. Por defecto `c6in.32xlarge` en efa (128 vCPU, 200 Gb, EFA con RDMA read) y `c6in.16xlarge` en rxe. Para NVMe local, `i4i.32xlarge`. En modo efa es obligatorio un tipo compatible con EFA (si no, se rechaza al momento y no se arranca nada) |
| `--quick` | Ejecución corta de 30 s (sin él: tras la corta, la ejecución principal oficial de 300 s) |
| `--max-hours` | Límite de seguridad (6 por defecto) |
| `--keep` | No liberar tras la ejecución y mostrar los comandos ssh |
| `--self-test` | Ejecutar también las pruebas unitarias en el nodo principal |
| `--az` / `--region` / `--ssh-cidr` / `--disk-gib` | Zona de disponibilidad, región, rango de red permitido para SSH, disco del sistema |

Si ya tiene instancias de AWS arrancadas por su cuenta:

- **Tipo compatible con EFA**: tras el instalador de EFA, directamente `sudo ./run.sh --peers ...`. Reconoce EFA automáticamente y usa el backend libfabric.
- **Tipo normal**: `sudo ./run.sh --soft-roce --peers ...`. Crea rxe0 en cada instancia automáticamente.
- En ambos casos, el grupo de seguridad debe permitir todo el tráfico entre sus miembros.

Diferencias entre los dos caminos:

- **rxe**: la CPU hace todas las operaciones RDMA, así que el ancho de banda y la latencia quedan muy lejos de los de una tarjeta real. Sirve para verificar las funciones y el flujo con varios nodos.
- **EFA**: RDMA real (SRD, datagramas fiables; los tipos Nitro v4 o posteriores tienen RDMA read por hardware). Los resultados son significativos.
- EFA entrega sin orden garantizado, por lo que:
  - La verificación de la lectura unilateral está hecha para tolerar el desorden (lee primero los datos y después el número de versión).
  - La búsqueda unilateral de metadatos está desactivada por defecto en EFA (necesita tres idas y vueltas y es más lenta que un RPC). Se puede forzar con `ROCEPOOL_DIRECT_LOOKUP=1`.

Detalles en `docs/AWS_ROUND.md`.

---

## 4c. Lectura directa a la GPU (GPUDirect Storage)

Los programas que usan mucho la GPU, como el renderizado o el entrenamiento, pueden llevar los datos de rocepool directamente a la memoria de la GPU.

- La tarjeta de red escribe por RDMA los datos de la memoria del OST directamente en la memoria de la GPU. No pasa por la memoria del host y la CPU no toca los datos (igual que el soporte GDS de Vendor W y DDN).
- La interfaz es cuFile, el estándar de NVIDIA (`libcufile.so.0`). PyTorch/KvikIO, RAPIDS, DALI y los renderizadores que leen con cuFile no necesitan cambiar el código.

```bash
# en el nodo con GPU (build.sh ya compiló roce/native/cufile/libcufile.so.0)
export LD_LIBRARY_PATH=<directorio de rocepool>/roce/native/cufile:$LD_LIBRARY_PATH
export ROCEPOOL_MDTS=... ROCEPOOL_OSTS=...      # igual que con la biblioteca de interceptación
export ROCEPOOL_PREFIX=/mnt/rocepool            # punto de montaje FUSE (o prefijo de la biblioteca de interceptación)
python3 su_programa                             # sus lecturas y escrituras cuFile van directamente entre rocepool y la memoria de la GPU
```

- **No hace falta nada manual.** `run.sh` tiene un paso `gds` (resultado en `onsite_gds.json`, incluido en el paquete de resultados).
  - En cada máquina comprueba:
    - La GPU y el controlador.
    - Si la memoria de la GPU se puede registrar en la tarjeta de red. dma-buf necesita la versión de código abierto del módulo del kernel; como alternativa, nvidia-peermem, que se carga con modprobe automáticamente si falta.
    - La distancia en PCIe entre la GPU y la tarjeta de red. Si están en sockets de CPU distintos, avisa.
  - En las máquinas con GPU mide también la velocidad, cada una en GB/s:
    - directamente a la memoria de la GPU.
    - a través de la memoria del host, como ocurre con un almacenamiento sin soporte GDS.
    - lectura simultánea en todas las GPU.
- En las máquinas sin GPU, el paso indica "no NVIDIA GPU" y no se considera un fallo.
- Si la memoria de la GPU no se puede registrar, se vuelve automáticamente a pasar por la memoria del host. Los resultados siguen siendo correctos y el informe lleva un WARN.
- **Caché KV para inferencia de IA (caché de prefijos, separación de prefill y decode)**: la API KV tiene llamadas que intercambian datos directamente con la memoria de la GPU. Son adecuadas para bloques KV.
  - En Python:
    - `buf = kv.register_device(tensor.data_ptr(), número_de_bytes)`
    - `kv.cached_prefix(lista_de_claves_de_bloque)`: averigua, con una ida y vuelta por OST, cuántos bloques del prefijo están en caché.
    - `buf.mget(...)` / `buf.mput(...)`
  - La tarjeta de red mueve los datos por RDMA directamente a y desde la memoria de la GPU, sin pasar por la memoria del host ni por el servidor de metadatos.
  - El paso `kvcache` de `run.sh` mide automáticamente:
    - con los tamaños KV de modelos reales: quick con Llama-3-8B, 3072 de 4096 tokens en caché; full con Llama-3-70B, 6144 de 8192 tokens en caché.
    - guardado, consulta de prefijos y lectura a la memoria de la GPU, verificando cada bloque.
    - salida: latencia de lectura (p50/p99), GB/s y TTFT. La TTFT se compara con caché y recalculando todo. El tiempo de prefill se estima a partir de la potencia de cálculo del modelo de GPU, y el informe lo indica.
    - El resultado está en `onsite_kvcache.json`. En las máquinas sin GPU solo se mide la etapa de «lectura a la memoria del host».
- Detalles en `docs/GPUDIRECT_STORAGE.md`.

---

## 5. Cómo leer los resultados

Al terminar la ejecución se muestra una tabla. Todos los datos están en `io500_report.json`.

```
phase            measured   emu%  rpc/op  hot MDT   projected   bottleneck
ior-easy-write   ...        ...                                 RAM to hold a 300 s write phase / NVMe tier write bandwidth
mdtest-hard-write ...                                           per-rank latency / CPU of the busiest node / ...
projected on ...: bw X GiB/s, md Y kIOPS, score Z
score range over the estimated inputs: a .. b
vs Vendor W (score 105.94): ...
what the fixes are worth (cumulative): ...
```

- **measured**: los valores obtenidos realmente en esta ejecución. En hardware real, esa es su puntuación (en la ejecución principal, mire la línea `[SCORE]`).
- **bottleneck**: el límite con el que la fase choca primero, estimado a partir del coste medido de CPU, RPC y bytes por operación.
- **projected**: proyección que aplica el coste medido por operación al hardware objetivo (`--project`; en hardware real, por defecto `auto`, es decir, la propia máquina).
  - En hardware real, la medición y la proyección deberían coincidir casi. Si difieren mucho, al modelo se le escapa algo.
  - La relación aparece en la fila "model check" de la tabla.
- En los informes de hardware real también vale la pena mirar `node_facts`, `ost_tier`, `client_probe`, `fuse_channel_probe` y `dead_servers` (si un servidor cayó, con el final de su registro).

---

## 6. Control manual (opcional)

Una vez que `roce_onsite.py` ha montado el clúster (`onsite_cluster.json`), se puede llamar directamente a `io500_run.py`.

```bash
export ROCEPOOL_CLUSTER=onsite_cluster.json
sudo -E python3 tools/io500_run.py --valid                      # ejecución principal oficial de 300 s
sudo -E python3 tools/io500_run.py --client intercept --ost-tier none   # forzar biblioteca de interceptación + solo memoria
sudo -E python3 tools/io500_run.py --ost-tier file:/dev/nvme1n1 # el mismo disco en bruto en cada nodo (¡se sobrescribe!)
sudo -E python3 tools/io500_run.py --ost-tier spdk:0000:41:00.0 # backend SPDK (requiere compilar con SPDK_DIR, véase abajo)
```

Argumentos frecuentes:

| Argumento | Significado |
|---|---|
| `--client auto\|fuse\|intercept` | Qué cliente usar |
| `--direct-read auto\|on\|off` | Lectura y búsqueda unilaterales. auto decide por sondeo si es más rápido con o sin ellas |
| `--bind-ranks auto\|numa\|core\|none` | Cómo se asignan los ranks |
| `--fuse-uring auto\|on\|off` | FUSE sobre io_uring |
| `--ost-tier auto\|none\|file:PATH[:GIB]\|spdk:TRADDR[:NSID]` | Capa de capacidad de los OST. `{n}` representa el número del OST |
| `--ost-ram-cache-mib` | Caché RAM de los OST |
| `--ranks-per-node`, `--mdts-per-node`, `--osts-per-node` | Ranks, MDT y OST por nodo |
| `--stonewall`, `--valid`, `--project`, `--report` | Tiempo de stonewall, ejecución principal, destino de la proyección, ruta del informe |

**Backend SPDK**, pasos (en cada máquina):

1. Compilar SPDK: `git clone https://github.com/spdk/spdk && cd spdk && git submodule update --init && ./configure && make`.
2. Vincular el NVMe de destino a vfio-pci (`scripts/setup.sh`). El disco queda separado del kernel.
3. Ejecutar `SPDK_DIR=/path/to/spdk bash roce/native/build.sh`.
4. Ejecutar con `--ost-tier spdk:<dirección PCI>`.

Está comprobado que la ruta SPDK compila con las cabeceras de v25.05, pero todavía no se ha ejecutado sobre hardware. La primera vez, revise el registro del OST.

**Ejecutar solo los benchmarks locales** (1 máquina, con dos espacios de nombres de red o una tarjeta real):

```bash
sudo python3 tools/bench_ior.py --backend dist --hard                      # cliente FUSE
sudo python3 tools/bench_ior.py --backend dist --hard --client intercept   # biblioteca de interceptación
sudo python3 tools/io500_run.py --nodes 10                                 # simular 10 nodos completos en esta máquina
```

**Ejecutar cualquier programa solo con la biblioteca de interceptación**:

```bash
LD_PRELOAD=roce/native/librocepool_intercept.so ROCEPOOL_PREFIX=/mnt/rocepool \
ROCEPOOL_MDTS=h1:19901,h2:19901 ROCEPOOL_OSTS=h1:19951,h2:19951 \
mpirun -n 32 ior -a POSIX -o /mnt/rocepool/f ...
```

`/mnt/rocepool` no necesita existir. Es solo un prefijo; las demás rutas pasan tal cual a la libc.

---

## 7. Variables de entorno en ejecución (todas con valores predeterminados; normalmente no hay que cambiarlas)

| Variable | Función |
|---|---|
| `ROCEPOOL_OST_TIER`, `ROCEPOOL_OST_RAM_CACHE_MIB`, `ROCEPOOL_P2PMEM` | Capa de capacidad, caché RAM y memoria P2P de los OST (`io500_run.py` las fija automáticamente según la comprobación previa) |
| `ROCEPOOL_PREFIX`, `ROCEPOOL_MDTS`, `ROCEPOOL_OSTS`, `ROCEPOOL_IO_THREADS`, `ROCEPOOL_INTERCEPT_LOG` | Biblioteca de interceptación |
| `ROCEPOOL_ATTR_TTL`, `ROCEPOOL_DIRTY_CAP_MIB`, `ROCEPOOL_SCACHE_SLOTS`, `ROCEPOOL_RPC_TIMEOUT_MS`, `ROCEPOOL_DIR_STRIPE`, `ROCEPOOL_DIRECT_READ` | Núcleo del cliente: TTL de la caché de atributos, caché de escritura por archivo, número de franjas de la caché de lectura, tiempo límite de RPC, directorios en franjas, lectura y búsqueda unilaterales |
| `ROCEPOOL_NUMA_NODE`, `ROCE_PIN_THREADS`, `ROCEPOOL_MDT_SLOTS` | Servidor: fijación a un nodo NUMA (núcleos y memoria), fijación de cada hilo de conexión a un núcleo, número de ranuras de la tabla de búsqueda unilateral del MDT (`io500_run.py` la fija según el tamaño de la ejecución) |
| `ROCE_TOS`, `ROCE_ACK_TIMEOUT`, `ROCE_POLL_SPIN_US`, `ROCE_BUFPOOL_MAX`, `ROCE_SERVER_MAX_INFLIGHT` | Capa de transporte (la comprobación previa las escribe en `roce_env.sh`) |
| `ROCE_TRANSPORT_LIB` | Biblioteca de transporte usada (tarjeta real `libroce_transport.so`, emulador `softverbs/libroce_transport_softverbs.so`) |
| `ROCE_BACKEND` | Qué transporte compila `build.sh`: `auto` (ofi si hay un dispositivo EFA, si no verbs) / `verbs` / `ofi`. `ROCE_LIBFABRIC` indica el directorio de libfabric (por defecto `/opt/amazon/efa`; si no existe, instale el paquete de la distribución o compílelo desde el código fuente) |
| `ROCE_OFI_PROVIDER`, `ROCE_OFI_SERVER_EPS`, `ROCE_OFI_CLIENT_EPS`, `ROCE_OFI_POST_TIMEOUT_MS` | Backend ofi: proveedor de libfabric (por defecto `efa` si hay EFA, si no `tcp`), puntos de conexión por servidor (por defecto, el número de núcleos), puntos de conexión por cliente, límite de reintentos al encolar |
| `ROCEPOOL_DIRECT_LOOKUP` | Búsqueda unilateral de metadatos. Activada por defecto en transportes con orden garantizado (RC) y desactivada en los que no lo garantizan (EFA). `1`/`0` la fuerza |

---

## 8. Problemas frecuentes

| Síntoma | Solución |
|---|---|
| FAIL en la comprobación previa: modo RoCE / memlock | La comprobación previa como root lo corrige automáticamente. Si no puede, muestra la ruta sysfs/configfs concreta |
| Nombres de host iguales o que se resuelven a 127.x | El script corrige `/etc/hosts` automáticamente. Si aun así falla, añada la línea a mano como se indica |
| "IO500 did not start in 300 s" | Los ranks de MPI no se conectan entre sí. Revise ssh sin contraseña, el cortafuegos y la resolución de nombres |
| Un servidor se cayó (`dead_servers`) | Revise el `ost<nodo>_<puerto>.log` / `mdt<nodo>_<puerto>.log` correspondiente y `kernel_log` del informe (OOM, etc.). Si falta memoria, revise `--mem-budget-gib` |
| Tiempo límite de RPC en el cliente (`timed out waiting for completion`) | Ocurre cuando la máquina está muy sobrecargada. Aumente `ROCEPOOL_RPC_TIMEOUT_MS` |
| En el registro del OST, `ibv_reg_mr refused the supplied (P2P) chunk memory` | El controlador de la tarjeta de red no admite registrar páginas P2P. Se usa la memoria del host automáticamente; la funcionalidad no se ve afectada |
| `Write phase needed ...s instead of stonewall 300s` (ior-easy INVALID) | Los datos no caben en 300 segundos. Al comienzo de la ejecución, la línea `IOR data budget ... from the OSTs' capacity` indica con la capacidad de qué OST se atascó. Los sistemas de archivos sobre NVMe (también sobre LVM/RAID) pasan a ser la capa automáticamente; si no hay ninguno, se usa el mayor sistema de archivos local de la máquina. Nunca se escribe automáticamente en discos en bruto; si hace falta, indique `--ost-tier file:/dev/nvmeXnY` |
| Un servidor se cayó durante la ejecución | IO500 se detiene al momento y no ejecuta las fases restantes. `dead_servers[].kernel_log` del informe incluye el registro del kernel de esa máquina (OOM, etc.) |
| Fallo de escritura en la capa NVMe `tier full` | El archivo de la capa es demasiado pequeño. Indíquelo más grande con `--ost-tier file:PATH:GIB` |
| AWS: `has no EFA` | Ese tipo no admite EFA. Cambie a `c6in.32xlarge` / `i4i.32xlarge` / `hpc7a.96xlarge` u otro, o use `--mode rxe` |
| AWS: FAIL de `libfabric` en la comprobación previa | El instalador de EFA no está bien instalado, o el grupo de seguridad no permite todo el tráfico entre sus miembros |
| AWS: WARN de `rdma_read` en la comprobación previa | El tipo no es Nitro v4 o posterior. libfabric emula RDMA read en la CPU del otro extremo. Todo funciona, pero la lectura unilateral es más lenta. El sondeo elige automáticamente entre lectura unilateral y lectura por RPC |
| AWS: no se puede crear rxe | Al kernel le falta `rdma_rxe`. Ejecute `apt install linux-modules-extra-$(uname -r)` |
| AWS: la conexión se cortó a mitad | Las máquinas terminan solas al llegar a `--max-hours`. También se pueden liberar al momento con `aws_cluster.py down --run-id ...` |

---

## 9. Resumen de los resultados de esta ronda (detalles en `docs/KERNEL_BYPASS_ROUND.md`)

- **Biblioteca de interceptación frente a FUSE** (emulador de 2 núcleos, ior-hard): la biblioteca de interceptación es 1,9 veces más rápida en escritura, 2,6 veces en lectura y 1,5 veces en create.
- **Simulación de 10 nodos** (`stress_reports/39`):
  - Medido: 1,68 → 3,02.
  - Proyección para 10 × 32 núcleos / 100 GbE: 375 (solo memoria) / 687 (capa NVMe). Vendor W: 105,94.
  - La proyección de metadatos es un límite superior; el ancho de banda lo determina el ancho de banda real de escritura de los discos.
- **Proyección con el mismo hardware (96 núcleos / 750 GB / 200G)**: 806 / 1 342. La comparación por apartados está en el intercambio durante el desarrollo.
- **Lectura y búsqueda unilaterales + fijación NUMA** (`docs/ONE_SIDED_ROUND.md`, `stress_reports/41`):
  - Medido en la simulación de 10 nodos: 3,02 → 3,77.
  - Las fases stat, de 2,5 a 3 veces más rápidas; proyección 375 → 449.
- **AWS** (`docs/AWS_ROUND.md`):
  - Se añadió un backend de transporte EFA (libfabric); todas las pruebas pasan con ambos backends.
  - Con un solo comando, `tools/aws_cluster.py`, se arranca un clúster y se ejecuta IO500.
