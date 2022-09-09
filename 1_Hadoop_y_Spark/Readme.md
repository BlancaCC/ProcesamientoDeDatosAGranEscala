# Hadoop y Spark 

## Hadoop  

Hadoop [2] es un *framework* (conjunto estadarizado de conceptos, prácricas y criterios para 
enfocar un tipo concreto de problema particular [1]) para programar aplicaciones 
distribuidas que requieren de un gran volumen de datos. 

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
9. Control de versiones en Hadoop. Para gestionar las versiones creamos el link simbólico `hadoop`.   
`ln -s hadoop-2.8.1/ hadoop`

10. Indicamos la versión de Java. 
`vi etc/hadoop/hadoop-env.sh`
Pulse `i` para insertar 
`export JAVA_HOME=/usr/lib/jvm/jre-1.7.0-openjdk` (cambiar la variable a esto). 

Veamos que todo e proceso es correcto: 
- Nos movemos a la carpeta `cd /opt/hadoop/` ejecutamos `./bin/hadoop` y dará un mensaje de error. 
- Escribimos un fichero con cualquier contenido en `/home/Texto.txt`
- `./bin/hadoop jar share/hadoop/tools/lib/hadoop-streaming-2.8.1.jar -input /home/Texto.txt -output /home/salida -mapper cat -reducer wc`



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