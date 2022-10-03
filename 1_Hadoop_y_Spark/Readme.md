# Hadoop y Spark 

## ¿Cómo instalar la plantilla de LaTeX?  

- Instalar [pandoc](https://pandoc.org/installing.html).  
- Instalar laTEX (si es que no lo tenías instalado).
- 
## Instrucciones rápidas   

El comando `make` creará un contenedor llamado `hadoop` el cual ejecutará automáticamente todos los pasos que aquí se describen. 

## Hadoop  

Hadoop [2] es un *framework* (conjunto estadarizado de conceptos, prácticas y criterios para 
enfocar un tipo concreto de problema particular [1]) para programar aplicaciones 
distribuidas que requieren de un gran volumen de datos. 

### HDFS  

Recurso [11]
 
HDFS son las siglas de *Hadoop Distributed File System*. Es un sistema distribuido basado en Java que permite obtener una visión de los recursos como una sola unidad.

#### Arquitectura y componentes de HDFS 

#### NameNode

###  Pseudo-Distributed Operation 

https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html

### Instalación  

Instalaremos Hadoop en una instancia de un contenedor Docker, ya que es lo que más se ajusta a la realidad. 
Requisito previo: tener instalado Docker. 

1. Descargamos una imagen de CentOS para Docker [4] `docker pull centos:centos7`
2. Corremos el contenedor y accedemos a él `docker run --rm -it centos:centos7 bash`
3. Intalación de paquetes básicos 
-  Actualizamos el SO `yum -y update && yum clean all`. 
- Instalamos `wget` `yum install wget`. 
- Instalamos `which` `yum install which`. 
4. Descargamos **Hadoop** en la carpeta donde nos encontramos:  
   `wget https://archive.apache.org/dist/hadoop/common/hadoop-2.8.1/hadoop-2.8.1.tar.gz` [3]
   (al hacer `ls` podríamos ver el fichero propiamente dicho). 
5. Instalación de Java
Para esta versión de Hadoop es necesario utilizar la vesión compatible de Java `java-1.7.0-openjdk`, 
esto es `yum install java-1.7.0-openjdk`.   
Si todo ha ido bien al hacer `java -version` deberías obtener: 
```
java version "1.7.0_261"
OpenJDK Runtime Environment (rhel-2.6.22.2.el7_8-aarch64 u261-b02)
OpenJDK 64-Bit Server VM (build 24.261-b02, mixed mode)
```
6. `yum -y install rsync` [5][6]
7. Descomprimimos Hadoop `tar xvzf hadoop-2.8.1.tar.gz`
8. Vamos a transladar el fichero descomprimido al directorio `opt` (directorio usual donde se instalan aplicaciones de terceros [7]).
`mv hadoop-2.8.1 opt/`
9.  Control de versiones en Hadoop. Para gestionar las versiones creamos el link simbólico `hadoop`.   
`ln -s hadoop-2.8.1 hadoop`

10. Indicamos la versión de Java. 
`vi etc/hadoop/hadoop-env.sh`
Pulse `i` para insertar 
`export JAVA_HOME=/usr/lib/jvm/jre-1.7.0-openjdk` (cambiar la variable a esto). 

Veamos que todo e proceso es correcto: 
- Nos movemos a la carpeta `cd /opt/hadoop/` ejecutamos `./bin/hadoop` y dará un mensaje de error. 

`/opt/hadoop/bin/hadoop` (desde el directorio principal). 

- Escribimos un fichero con cualquier contenido en `/home/Texto.txt`
- `./bin/hadoop jar share/hadoop/tools/lib/hadoop-streaming-2.8.1.jar -input /home/Texto.txt -output /home/salida -mapper cat -reducer wc`
- `/opt/hadoop/bin/hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.8.1.jar -input /home/Texto.txt -output /home/salida -mapper cat -reducer wc`

Si todo ha ido bien debería de aparece algo como 

```
sh-4.2# echo "Hola caracola" >> /home/Texto.txt
sh-4.2# /opt/hadoop/bin/hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.8.1.jar -input /home/Texto.txt -output /home/salida -mapper cat -reducer wc
22/09/14 10:51:16 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
22/09/14 10:51:16 INFO Configuration.deprecation: session.id is deprecated. Instead, use dfs.metrics.session-id
22/09/14 10:51:16 INFO jvm.JvmMetrics: Initializing JVM Metrics with processName=JobTracker, sessionId=
22/09/14 10:51:16 INFO jvm.JvmMetrics: Cannot initialize JVM Metrics with processName=JobTracker, sessionId= - already initialized
22/09/14 10:51:16 INFO mapred.FileInputFormat: Total input files to process : 1
22/09/14 10:51:16 INFO mapreduce.JobSubmitter: number of splits:1
22/09/14 10:51:16 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_local229123260_0001
22/09/14 10:51:16 INFO mapreduce.Job: The url to track the job: http://localhost:8080/
22/09/14 10:51:16 INFO mapred.LocalJobRunner: OutputCommitter set in config null
22/09/14 10:51:16 INFO mapred.LocalJobRunner: OutputCommitter is org.apache.hadoop.mapred.FileOutputCommitter
22/09/14 10:51:16 INFO mapreduce.Job: Running job: job_local229123260_0001
22/09/14 10:51:16 INFO output.FileOutputCommitter: File Output Committer Algorithm version is 1
22/09/14 10:51:16 INFO output.FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
22/09/14 10:51:16 INFO mapred.LocalJobRunner: Waiting for map tasks
22/09/14 10:51:16 INFO mapred.LocalJobRunner: Starting task: attempt_local229123260_0001_m_000000_0
22/09/14 10:51:16 INFO output.FileOutputCommitter: File Output Committer Algorithm version is 1
22/09/14 10:51:16 INFO output.FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
22/09/14 10:51:16 INFO mapred.Task:  Using ResourceCalculatorProcessTree : [ ]
22/09/14 10:51:16 INFO mapred.MapTask: Processing split: file:/home/Texto.txt:0+14
22/09/14 10:51:16 INFO mapred.MapTask: numReduceTasks: 1
22/09/14 10:51:16 INFO mapred.MapTask: (EQUATOR) 0 kvi 26214396(104857584)
22/09/14 10:51:16 INFO mapred.MapTask: mapreduce.task.io.sort.mb: 100
22/09/14 10:51:16 INFO mapred.MapTask: soft limit at 83886080
22/09/14 10:51:16 INFO mapred.MapTask: bufstart = 0; bufvoid = 104857600
22/09/14 10:51:16 INFO mapred.MapTask: kvstart = 26214396; length = 6553600
22/09/14 10:51:16 INFO mapred.MapTask: Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
22/09/14 10:51:16 INFO streaming.PipeMapRed: PipeMapRed exec [/usr/bin/cat]
22/09/14 10:51:16 INFO Configuration.deprecation: mapred.task.id is deprecated. Instead, use mapreduce.task.attempt.id
22/09/14 10:51:16 INFO Configuration.deprecation: mapred.task.is.map is deprecated. Instead, use mapreduce.task.ismap
22/09/14 10:51:16 INFO Configuration.deprecation: mapred.skip.on is deprecated. Instead, use mapreduce.job.skiprecords
22/09/14 10:51:16 INFO Configuration.deprecation: mapred.local.dir is deprecated. Instead, use mapreduce.cluster.local.dir
22/09/14 10:51:16 INFO Configuration.deprecation: map.input.file is deprecated. Instead, use mapreduce.map.input.file
22/09/14 10:51:16 INFO Configuration.deprecation: mapred.job.id is deprecated. Instead, use mapreduce.job.id
22/09/14 10:51:16 INFO Configuration.deprecation: user.name is deprecated. Instead, use mapreduce.job.user.name
22/09/14 10:51:16 INFO Configuration.deprecation: map.input.start is deprecated. Instead, use mapreduce.map.input.start
22/09/14 10:51:16 INFO Configuration.deprecation: mapred.tip.id is deprecated. Instead, use mapreduce.task.id
22/09/14 10:51:16 INFO Configuration.deprecation: mapred.task.partition is deprecated. Instead, use mapreduce.task.partition
22/09/14 10:51:16 INFO Configuration.deprecation: map.input.length is deprecated. Instead, use mapreduce.map.input.length
22/09/14 10:51:16 INFO Configuration.deprecation: mapred.work.output.dir is deprecated. Instead, use mapreduce.task.output.dir
22/09/14 10:51:16 INFO streaming.PipeMapRed: R/W/S=1/0/0 in:NA [rec/s] out:NA [rec/s]
22/09/14 10:51:16 INFO streaming.PipeMapRed: MRErrorThread done
22/09/14 10:51:16 INFO streaming.PipeMapRed: Records R/W=1/1
22/09/14 10:51:16 INFO streaming.PipeMapRed: mapRedFinished
22/09/14 10:51:16 INFO mapred.LocalJobRunner: 
22/09/14 10:51:16 INFO mapred.MapTask: Starting flush of map output
22/09/14 10:51:16 INFO mapred.MapTask: Spilling map output
22/09/14 10:51:16 INFO mapred.MapTask: bufstart = 0; bufend = 15; bufvoid = 104857600
22/09/14 10:51:16 INFO mapred.MapTask: kvstart = 26214396(104857584); kvend = 26214396(104857584); length = 1/6553600
22/09/14 10:51:16 INFO mapred.MapTask: Finished spill 0
22/09/14 10:51:16 INFO mapred.Task: Task:attempt_local229123260_0001_m_000000_0 is done. And is in the process of committing
22/09/14 10:51:16 INFO mapred.LocalJobRunner: Records R/W=1/1
22/09/14 10:51:16 INFO mapred.Task: Task 'attempt_local229123260_0001_m_000000_0' done.
22/09/14 10:51:16 INFO mapred.LocalJobRunner: Finishing task: attempt_local229123260_0001_m_000000_0
22/09/14 10:51:16 INFO mapred.LocalJobRunner: map task executor complete.
22/09/14 10:51:16 INFO mapred.LocalJobRunner: Waiting for reduce tasks
22/09/14 10:51:16 INFO mapred.LocalJobRunner: Starting task: attempt_local229123260_0001_r_000000_0
22/09/14 10:51:16 INFO output.FileOutputCommitter: File Output Committer Algorithm version is 1
22/09/14 10:51:16 INFO output.FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
22/09/14 10:51:16 INFO mapred.Task:  Using ResourceCalculatorProcessTree : [ ]
22/09/14 10:51:16 INFO mapred.ReduceTask: Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@e5404f1
22/09/14 10:51:17 INFO reduce.MergeManagerImpl: MergerManager: memoryLimit=333971456, maxSingleShuffleLimit=83492864, mergeThreshold=220421168, ioSortFactor=10, memToMemMergeOutputsThreshold=10
22/09/14 10:51:17 INFO reduce.EventFetcher: attempt_local229123260_0001_r_000000_0 Thread started: EventFetcher for fetching Map Completion Events
22/09/14 10:51:17 INFO reduce.LocalFetcher: localfetcher#1 about to shuffle output of map attempt_local229123260_0001_m_000000_0 decomp: 19 len: 23 to MEMORY
22/09/14 10:51:17 INFO reduce.InMemoryMapOutput: Read 19 bytes from map-output for attempt_local229123260_0001_m_000000_0
22/09/14 10:51:17 INFO reduce.MergeManagerImpl: closeInMemoryFile -> map-output of size: 19, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->19
22/09/14 10:51:17 INFO reduce.EventFetcher: EventFetcher is interrupted.. Returning
22/09/14 10:51:17 INFO mapred.LocalJobRunner: 1 / 1 copied.
22/09/14 10:51:17 INFO reduce.MergeManagerImpl: finalMerge called with 1 in-memory map-outputs and 0 on-disk map-outputs
22/09/14 10:51:17 INFO mapred.Merger: Merging 1 sorted segments
22/09/14 10:51:17 INFO mapred.Merger: Down to the last merge-pass, with 1 segments left of total size: 3 bytes
22/09/14 10:51:17 INFO reduce.MergeManagerImpl: Merged 1 segments, 19 bytes to disk to satisfy reduce memory limit
22/09/14 10:51:17 INFO reduce.MergeManagerImpl: Merging 1 files, 23 bytes from disk
22/09/14 10:51:17 INFO reduce.MergeManagerImpl: Merging 0 segments, 0 bytes from memory into reduce
22/09/14 10:51:17 INFO mapred.Merger: Merging 1 sorted segments
22/09/14 10:51:17 INFO mapred.Merger: Down to the last merge-pass, with 1 segments left of total size: 3 bytes
22/09/14 10:51:17 INFO mapred.LocalJobRunner: 1 / 1 copied.
22/09/14 10:51:17 INFO streaming.PipeMapRed: PipeMapRed exec [/usr/bin/wc]
22/09/14 10:51:17 INFO Configuration.deprecation: mapred.map.tasks is deprecated. Instead, use mapreduce.job.maps
22/09/14 10:51:17 INFO Configuration.deprecation: mapred.job.tracker is deprecated. Instead, use mapreduce.jobtracker.address
22/09/14 10:51:17 INFO streaming.PipeMapRed: R/W/S=1/0/0 in:NA [rec/s] out:NA [rec/s]
22/09/14 10:51:17 INFO streaming.PipeMapRed: MRErrorThread done
22/09/14 10:51:17 INFO streaming.PipeMapRed: Records R/W=1/1
22/09/14 10:51:17 INFO streaming.PipeMapRed: mapRedFinished
22/09/14 10:51:17 INFO mapred.Task: Task:attempt_local229123260_0001_r_000000_0 is done. And is in the process of committing
22/09/14 10:51:17 INFO mapred.LocalJobRunner: 1 / 1 copied.
22/09/14 10:51:17 INFO mapred.Task: Task attempt_local229123260_0001_r_000000_0 is allowed to commit now
22/09/14 10:51:17 INFO output.FileOutputCommitter: Saved output of task 'attempt_local229123260_0001_r_000000_0' to file:/home/salida/_temporary/0/task_local229123260_0001_r_000000
22/09/14 10:51:17 INFO mapred.LocalJobRunner: Records R/W=1/1 > reduce
22/09/14 10:51:17 INFO mapred.Task: Task 'attempt_local229123260_0001_r_000000_0' done.
22/09/14 10:51:17 INFO mapred.LocalJobRunner: Finishing task: attempt_local229123260_0001_r_000000_0
22/09/14 10:51:17 INFO mapred.LocalJobRunner: reduce task executor complete.
22/09/14 10:51:17 INFO mapreduce.Job: Job job_local229123260_0001 running in uber mode : false
22/09/14 10:51:17 INFO mapreduce.Job:  map 100% reduce 100%
22/09/14 10:51:17 INFO mapreduce.Job: Job job_local229123260_0001 completed successfully
22/09/14 10:51:17 INFO mapreduce.Job: Counters: 30
	File System Counters
		FILE: Number of bytes read=268090
		FILE: Number of bytes written=913206
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
	Map-Reduce Framework
		Map input records=1
		Map output records=1
		Map output bytes=15
		Map output materialized bytes=23
		Input split bytes=72
		Combine input records=0
		Combine output records=0
		Reduce input groups=1
		Reduce shuffle bytes=23
		Reduce input records=1
		Reduce output records=1
		Spilled Records=2
		Shuffled Maps =1
		Failed Shuffles=0
		Merged Map outputs=1
		GC time elapsed (ms)=0
		Total committed heap usage (bytes)=505413632
	Shuffle Errors
		BAD_ID=0
		CONNECTION=0
		IO_ERROR=0
		WRONG_LENGTH=0
		WRONG_MAP=0
		WRONG_REDUCE=0
	File Input Format Counters 
		Bytes Read=14
	File Output Format Counters 
		Bytes Written=37
22/09/14 10:51:17 INFO streaming.StreamJob: Output directory: /home/salida
sh-4.2# 
```

### Fuentes 

Consultadas el 9 de septiembre de 2022: 

1. [Framework wikipedia](https://es.wikipedia.org/wiki/Framework) 
2. [Hadoop wikipedia](https://es.wikipedia.org/wiki/Apache_Hadoop)
3. [Hadoop download](https://hadoop.apache.org/release/2.8.1.html) 
4. [Docker CentOS](https://hub.docker.com/_/centos)
5. [rsync Hostinger information](https://www.hostinger.es/tutoriales/rsync-linux)
6. [ssh vs rsync stackoverflow](https://superuser.com/questions/329424/which-is-more-efficient-rsync-over-ssh-or-rsync-to-a-drive-mounted-locally-via)
7. [directorios Ubuntu](https://help.ubuntu.com/kubuntu/desktopguide/es/directories-file-systems.html)
8. [Instalar Hadoop medium](https://medium.com/@hema-chandra/cannot-execute-libexec-hdfs-config-sh-ec7c3b1a45bd)  
9. [Pseudo-distributed Operations](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html)  
10. [Docker Expose](https://www.cloudbees.com/blog/docker-expose-port-what-it-means-and-what-it-doesnt-mean)  
11. [HDFS de Aprender big data](https://aprenderbigdata.com/hdfs/)