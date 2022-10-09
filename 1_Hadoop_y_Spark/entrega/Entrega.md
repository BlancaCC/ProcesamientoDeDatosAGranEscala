---
title: "Práctica 1. Hadoop y Spark"
author: [Blanca Cano Camarero y Iker Villegas Labairu]
date: "Madrid 8 de octubre del 2022"
subject: "Procesamiento de datos a gran escala"
keywords: [Hadoop, Spark]
subtitle: ""
lang: "es"
titlepage: true,
titlepage-text-color: "FFFFFF"
titlepage-rule-color: "360049"
titlepage-rule-height: 0
titlepage-background: "background.pdf"
...

\newpage

# Práctica 1 Hadoop y Spark

## 1. Instalación de Hadoop

Para la instalación se ha seguido estrictamente el tutorial de clase, además para agilizar el proceso y puesto que en ambiente actual es interesante el uso de contenedores, hemos realizado un script de bash, para automatizar el proceso de instalación hasta la parte de ssh en un contenedor de docker con `CentOS:7`:

```bash
echo "Task 1: Updating system"
yum -y update && yum clean all
echo "Task 2: Installing packages"
yum -y install wget
yum -y install which
yum -y install rsync
yum -y install java-1.7.0-openjdk
echo "Task 3: Hadoop installation (may take a while)"
wget https://archive.apache.org/dist/hadoop/common/hadoop-2.8.1/hadoop-2.8.1.tar.gz
tar xvzf hadoop-2.8.1.tar.gz
mv hadoop-2.8.1 opt/
cd opt/ && ln -s hadoop-2.8.1 hadoop
export JAVA_HOME=/usr/lib/jvm/jre-1.7.0-openjdk
echo 'export JAVA_HOME=/usr/lib/jvm/jre-1.7.0-openjdk' >> ./hadoop/etc/hadoop/hadoop-env.sh
# Comando de prueba
#./opt/hadoop/bin/hadoop 

yum -y install openssh-server openssh-clients
```

El respectivo Dockerfile sería el siguiente:

```Dockerfile
FROM centos:7
COPY ./initial.sh /
COPY ./material /
RUN bash ./initial.sh
EXPOSE 50070
```

Donde la carpeta material contiene:

```shell
(main)> tree material/                                                                          
material/
|--WordCount_final.java
|-- quijote.txt
```

Para llevar a cabo la instalación de hadoop se han llevado a cabo los siguientes requisitos:  

- En primer lugar, hemos instalado la versión 1.7 de java en OpenJDK la cual ya viene con CentOS.
```yum -y install java-1.7.0-openjdk```

- Por otro lado, también se ha instalado el paquete `rsync`, el cual está incluido en Linux.
```yum -y install rsync```

- Por último, hemos obtenido hadoop a través del siguiente modo:
```wget apacha.rediris.es/hadoop/common/hadoop-2.8.1/hadoop-2.8.1.tar.gz```

Una vez llevado a cabo estos puntos hemos descomprimido el archivo `hadoop-2.8.1.tar.gz` y lo hemos movido a la carpeta `/opt`. Para finalizar se ha llevado una modificación en el fichero `/opt/hadoop/etc/hadoop/hadoop-env.sh` a través del comando `vim`, donde el código `export JAVA_HOME=${JAVA_HOME}` ha pasado a ser `export JAVA_HOME=/usr/lib/jvm/jre-1.7.0-openjdk`.

A continuación, hemos clonado la máquina virtual para llevar a cabo dicha instalación de dos formas distintas: Standalone y pseudo-distributed.
Comenzamos con el modo standalone. Para ello veamos el siguiente ejemplo.
Desde la carpeta `/opt/hadoop`, creamos un directorio al que llamaremos `prueba` y lo rellenamos con los datos.

```cp etc/hadoop/*.xml prueba```

Y ejecutamos el comando siguiente

```bin/hadoop jar share/hadoop/tools/lib/hadoop-streaming-2.8.1.jar -input prueba -output salida -mapper cat -reducer wc```  

Obteniendo así la siguiente salida:  `757    2803   27305`.

Pasamos ahora al caso donde la instalación se ha llevado a cabo en modo pseudo-distributed.
Aquí, hemos comenzado configurando el `ssh` de la siguiente forma para que funcione sin contraseña con conexiones en localhost:

```
ssh-keygen
ssh-copy-id localhost
```

A continuación, hemos modificado los siguientes ficheros a través del comando `vim`:  

- `/opt/hadoop/etc/hadoop/core-site.xml`, donde se ha llevado a cabo la modificación

```xml
<configuration>
        <property>
                <name>fs.defaultFS</name>
                <value>hdfs://localhost:9000</value>
        </property>
</configuration>
```

- `/opt/hadoop/etc/hadoop/core-site.xml`, en el cual hemos cambiado lo siguiente:

```xml
<configuration>
        <property>
                <name>dfs.replication</name>
                <value>1</value>
        </property>
</configuration>
```

Para finalizar hemos formateado el sistema de ficheros y hemos iniciado el NameNode y DataNode de la forma siguiente:

```
/opt/hadoop/bin/hdfs namenode -format
/opt/hadoop/sbin/start-dfs.sh
```

Comprobamos que podemos acceder a través de la web al NameNode con dirección: `http://localhost:50070`.

![Visualización de webnode desde navegador](Imagen_3.png)

Para corroborar su funcionamiento hemos creado los siguientes directorios en HDFS desde la carpeta hadoop:

```
bin/hdfs dfs -mkdir /user
bin/hdfs dfs -mkdir /user/root
bin/hdfs dfs -mkdir /user/bigdata
bin/hdfs dfs -mkdir /user/bigdata/prueba
```

Y hemos ejecutado el siguiente código para copiar los archivos de extensión .xml situados en la carpeta `etc/hadoop` al directorio `/user/bigdata`

```
bin/hdfs dfs -put etc/hadoop/*.xml /user/bigdata
```

Se plantean las siguientes cuestiones:

### ¿En qué directorio del HDFS se copian los ficheros?

Los ficheros se copian en el directorio `/user/bigdata`, tal y como se había indicado en el comando anterior.

![Directorio donde se copian ficheros HDFS](Imagen_2.png)

### ¿Qué ocurre si no hubiéramos creado el directorio?

Si no hubiéramos creado el directorio `/user/bigdata` del HDFS no se llevaría a cabo la copia de ficheros y saltaría el siguiente mensaje de error:

```
put: `/user/bigdata': No such file or directory
```

Procedemos a responder los siguientes ejercicios

## Ejecicio 1.1  

### ¿Qué ficheros ha modificado para activar la configuración del HDFS? ¿Qué líneas ha sido necesario modificar?

Para activar la configuración HDFS se han modificado los archivos `hdfs-site.xml` y `core-site.xml`, de la forma que se ha explicado anteriormente.
Es neceario además ejecutar el comando `bin/dfs namenode -format` que afecta a ciertos ficheros así que estos también se ven modificados.

## Ejercicio 1.2

#### Para pasar a la ejecución de Hadoop sin HDFS ¿es suficiente con parar el servicio con `stop-dfs.sh`? ¿Cómo se consigue?

No, pese a que con el comando `sbin/stop-dfs.sh` se detienen todos los en particular la ejecución de Hadoop con HDFS; esto no es suficiente. Deberíamos eliminar la propiedades:

- `dfs.replication` de `hdfs-site.xml`.
- `fs.defaultFS` de `core-site.xml`.  

## 2.Ejecución de la máquina de ejemplo

Comenzaremos para el caso donde la instalación se ha llevado del modo standalone. Simplemente ejecutaremos un wordcount desde hadoop

```
bin/hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-2.8.1.jar wordcount prueba salida1
```

Obteniendo así el archivo `/opt/hadoop/salida1/part-r-00000` con el resultado siguiente, donde mostramos las primeras líneas del archivo:

```
"*" 18
"AS 8
"License"); 8
"alice,bob 18
"clumping" 1
&quot;kerberos&quot;. 1
&quot;simple&quot; 1
'HTTP/' 1
'none' 1
'random' 1
'sasl' 1
'string' 1
'zookeeper' 2
'zookeeper'. 1
(ASF) 1
(Kerberos). 1
(default) 1
(default), 1
```

Tal y como vemos el programa realiza un conteo de la aparición de las cadenas de carácteres que aparecen sin ningún tipo de filtro.

Para la ejecución de la aplicación de ejemplo desde el modo hdfs, descargamos del mismo modo el fichero `quijote.txt` y lo guardamos en una carpeta local siguiendo la siguiente ruta `/home/bigdata/quijote.txt`.
A continuación, lo pasamos a un directorio de HDFS, desde la carpeta hadoop, del siguiente modo:

```
bin/hadoop dfs -copyFromLocal /home/bigdata/quijote.txt /user/bigdata/prueba
```

Vemos que aparece el fichero `quijote.txt` en el nuevo directorio

Y llevamos a cabo el proceso del wordcount

```
bin/hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-2.8.1.jar wordcount /user/bigdata/prueba/quijote.txt /user/bigdata/prueba/salida
```

Obteniendo de nuevo un archivo `/user/bigdada/prueba/salida/part-r-00000` cuyas primeras líneas son:

```
"Apenas 1
"Caballero 4
"Conde 1
"Ea, 1
"Miau", 1
"Rastrea 1
"Ricamonte", 1
"Tablante", 1
"dichosa 1
"el 8
"y 1
```

Como se puede observar, cuenta de manera diferente palabras que van precedidas o seguidas de signos de puntuación, así como, los casos en los que su primera letra es mayúscula. Este problema no nos permite obtener un análisis muy fino del número de veces que aparece cada palabra en el fragmento estudiado.

## 3.Programación de aplicaciones Hadoop con java

Para ambos casos, a parte de los requisitos expuestos anteriormente, necesitaremos instalar el entorno de desarrollo java, de la siguiente forma

```
sudo yum install java-1.7.0-openjdk-devel.x86_64
```

Empezaremos para el caso en el que no vamos a usar HDFS.
A partir de ahora necesitaremos ser super usuario, así que aplicamos el comando su. Crearemos un nuevo directorio desde la carpeta opt al que llamaremos work, y dentro del mismo, otro que denominaremos WordCount. Además, descargaremos y guardaremos los ficheros `quijote.txt` y `WordCount.java` en la ruta `/opt/work`.
A continuación, compilaremos el archivo de java del siguiente modo:

```
Javac -classpath /opt/hadoop/share/hadoop/common/*:/opt/hadoop/share/hadoop/mapreduce/* -d WordCount WordCount.java
```

Y creamos el archive .jar

```
jar -cvf WordCount.jar -C WordCount/ .
```

Ahora que ya tenemos todo listo, ejecutaremos el WordCount desde `/opt/hadoop`

```
bin/hadoop jar /opt/work/WordCount.jar uam.WordCount /opt/work/quijote.txt /opt/work/salida1
```

Obteniendo así un nuevo archivo con el conteo de todas las veces que aparece cada palabra en dicho fragmento del Quijote.

Ahora tomamos el caso donde utilizamos HDFS. Tomaremos el fichero `WordCount.java` anteriormente descargado y guardado en el directorio `/opt/work` el cual contiene las clases mapper y reducer, así como el método main ya completos. Además, crearemos el siguiente script, al que llamaremos `compilar.bash`:

```
#!/bin/bash

file=$1

HADOOP_CLASSPATH=/opt/hadoop/share/hadoop/common/*:/opt/hadoop/share/hadoop/mapreduce/*
echo $HADOOP_CLASSPATH

rm -rf ${file}
mkdir -p ${file}

javac -classpath $HADOOP_CLASSPATH -d ${file} ${file}.java
jar -cvf ${file}.jar -C ${file} .
```

Para mejorarlo, y que podamos elegir el nombre que le damos al archivo .jar creado, añadiremos una segunda entrada, quedando el código ahora de la siguiente forma:

```
#!/bin/bash

file=$1
name=$2

HADOOP_CLASSPATH=/opt/hadoop/share/hadoop/common/*:/opt/hadoop/share/hadoop/mapreduce/*
echo $HADOOP_CLASSPATH

rm -rf ${file}
mkdir -p ${file}

javac -classpath $HADOOP_CLASSPATH -d ${file} ${file}.java
jar -cvf ${name}.jar -C ${file} .
```

A continuación, compilamos el archivo como se sigue

```
./compilar.bash WordCount result
```

Obteniendo así el archivo `result.jar`, que lanzaremos invocando la aplicación del siguiente modo

```
bin/hadoop jar /opt/work/result.jar uam.WordCount /user/bigdata/prueba/quijote.txt /user/bigdata/prueba/salida_128MB
```

Ahora disponemos de las siguientes cuestiones:

### 1. Modificar el ejemplo de WordCount que hemos tomado como partida, para que no tenga en cuenta signos de puntuación ni las mayúsculas ni las minúsculas volver a ejecutar la aplicación

Para conseguir esto hemos modificado el método map de la siguiente manera

```java
public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String minimized_value = value.toString().toLowerCase();
      String processed_value = minimized_value.replaceAll("[^áéíóúüña-z0-9]+","");
      StringTokenizer itr = new StringTokenizer(processed_value));
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
```

Tras compilar el archivo y ejecutarlo, podemos observar que ya no se tienen en cuenta, ni mayúsculas ni signos de puntuación. Aquí una muestra de las primeras líneas:

```
a 1428
aa 1
abadejo 2
abades 1
abajo 11
abalánzase 1
abejas 1
abencerraje 1
abiertas 1
abierto 3
abiertos 2
abindarráez 3
abismo 2
ablande 1
aborrascadas 1
aborrece 1
aborrecido 3
aborrecimiento 2
```

### 2. Comparar resultados de la aplicación desarrollada con la que se puede ejecutar directamente en los ejemplos hadoop map-reduce

Observamos claramente que el conteo de palabras en este último caso se ha realizado de una manera muy fina, dónde se han ignorado carácteres de puntuación y mayúsculas. De este modo, se muestra de una mejor forma el número real de veces que aparece cada cadena de caracteres.

Por otro lado, se plantean los siguientes ejercicios:

### 3.1 ¿Dónde se crea hdfs? ¿Cómo se puede decidir su localización?

La localización de los archivos hdfs viene determinada por la variable `dfs.datanode.data.dir`, cuyo valor por defecto es `file://${hadoop.tmp.dir}/dfs/data`. Dicha variable se puede modificar desde el archivo `/opt/hadoop/etc/hadoop/hdfs-site.xml` del siguiente modo:

```xml
<property>
 <name> dfs.datanode.data.dir</name>
 <value> file://${hadoop.tmp.dir}/dfs/data</value>
</property>
```

Además, cabe destacar, que la variable `hadoop.tmp.dir` podemos modificarla desde el archivo /opt/hadoop/etc/hadoop/core-site.xml, cuyo valor por defecto viene dado por `/tmp/hadoop-${user.name}`.

### 3.2 ¿Cómo se puede borrar todo el contenido del HDFS, incluido su estructura?

Todo el contenido de HDFS, incluyendo su estructura, podría borrarse formateando el Namenode con el comando `bin/hdfs namenode -format` una vez que se hayan parado todos los servicios del HDFS (`sbin/stop.dfs.sh`). De este modo, una vez que se proceda a iniciar el Namenode y el Datanode, todo el contenido, tanto ficheros, como estructura del HDFS, habrá desaparecido.

### 3.3 Si estás utilizando hdfs, ¿Cómo puedes volver a ejecutar WordCount como si fuese single.node?

Para volver a ejecutar WorCount como si fuese single.node, se deberán quitar los cambios que hemos realizado en los archivos `core-site.xml` y `hdfs-site.xml` contenidos en el directorio `/opt/hadoop/etc/hadoop`.

Respecto al fragmento del Quijote, y tomando el código java modificado,

### 3.4 ¿Cuáles son las 10 palabras más utilizadas?

Las 10 palabras más utilizadas son, en orden decreciente: que (3055), de (2816), y (2585), a (1428), la (1423), el (1232), en (1155), no (915), se (753) y los (696).

### 3.5 ¿Cuántas veces aparece el artículo “el” y la palabra “dijo”?

El artículo el aparece un total de 1232 veces, mientras que la palabra dijo 272.

### 3.6 ¿El resultado coincide utilizando la aplicación wordcount que se da en los ejemplos? Justifique la respuesta

Claramente no coincide. En este último caso el análisis se realiza de una forma más rápida, así como, hemos podido obviar la aparición de mayúsculas y signos de puntuación en las cadenas de caractéres que estorbaban la obtención del número real de veces que aparecía la palabra en el texto. Todo ello no se veía reflejado en la aplicación wordcount dada en los ejemplos.

## 4. Modificación de parámetros *mapreduce*  

En primer lugar, vamos a comenzar creando un segundo archivo de quijote.txt, pero esta vez de mayor tamaño. Para ello concatenamos el fichero varias veces de la forma siguiente:

```
cat quijote.txt quijote.txt >> quijotex15.txt
```

Llevaremos a cabo los pasos citados a continuación:

### 4.1 Usar el tamaño de bloque por defecto de HDFS

Ya hemos situado anteriormente el archivo `quijote.txt` en el directorio de HDFS `/user/bigdata/prueba`. Cabe destacar que el tamaño del archivo es de 128 Mb.

### 4.2 Indicar el tamaño de bloque en la línea de comandos al escribir el fichero

Tomamos como parámetro de tamaño, `dfs.blocksize =2097152`  y pasamos este nuevo archivo `quijotex15.txt` directamente al directorio HDFS `/user/bigdata/prueba`.

```
sudo bin/hdfs dfs -D dfs.blocksize=2097152 -put /home/bigdata/quijotex15.txt /user/bigdata/prueba
```

Tal y como se muestra en la imagen en comparación con el quijote.txt, el tamaño de este archivo es de 2 Mb.

![image](Imagen_1.png)

### 4.3 Editar el fichero de configuración `hdfs-site.xml` y modificar el tamaño de bloque con el parámetro

Mediante el comando `vim` llevamos a cabo el siguiente cambio en el archivo `hdfs-site.xml`

```xml
<configuration>
        <property>
                <name>dfs.replication</name>
                <value>1</value>
        </property>
        <property>
                <name>dfs.block.size</name>
                <value>2097152</value>
        </property>
</configuration>
```

### 4.4 Comprobar el efecto del tamaño de bloques en el funcionamiento de la aplicación WordCount. ¿Cuántos procesos Maps se lanzan en cada caso? Indique como lo ha comprobado

Tras realizar una serie de lanzamientos a ambos archivos del quijote de distinto tamaño vemos lo siguiente: para el archivo de `quijote.txt` el cual poseía un tamaño de 128 Mb, el número de procesos Maps que se han lanzado a sido de 1, tal y como muestra el siguiente código de salida:

```
  Shuffled Maps =1
  Failed Shuffles=0
  Merged Map outputs=1
```

Mientras que para el caso del `quijotex15.txt`, cuyo tamaño es de 2 Mb, el número de Maps es de 3:

```
  Shuffled Maps =3
  Failed Shuffles=0
  Merged Map outputs=3
```

Por lo tanto, el tamaño de los bloques afecta al número de procesos de Maps, y por ende de procesos reduce, que se realizan, así como, al número de bloques creados.

<a href="https://colab.research.google.com/github/BlancaCC/ProcesamientoDeDatosAGranEscala/blob/spark/SparkColabEjercicios2022.ipynb" target="_parent"><img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/></a>

# Parte 2:  PySpark en Google Colab








## Pregunta TS1.1 ¿Cómo hacer para obtener una lista de los elementos al cuadrado?


```python
#¿Cómo hacer para obtener una lista de los elementos al cuadrado?
numeros = sc.parallelize([1,2,3,4,5,6,7,8,9,10])
rdd = numeros.map(lambda e: e**2)
print(rdd.collect())
```

    [1, 4, 9, 16, 25, 36, 49, 64, 81, 100]


## Pregunta TS1.2 ¿Cómo filtrar los impares?


```python
#¿Cómo filtrar los impares?
rddi = numeros.filter(lambda n: n%2 == 0) # Mostrará los números impares
print(rddi.collect())

```

    [2, 4, 6, 8, 10]


## Pregunta TS1.3 ¿Tiene sentido cambiar la suma por una diferencia? ¿Si se repite se obtiene siempre el mismo resultado?


```python
#Tiene sentido la reducción(elem1-elem2)?
numeros = sc.parallelize([1,2,3,4,5,6,7,8,9,10])

print (numeros.reduce(lambda elem1,elem2: elem1-elem2))
```

    15


# Respuesta TS1.3 

Tal operación **no** tiene sentido ya que la operación obtenida no es la esperada. 

Para entender qué es lo que está ocurriendo es necesario consultar la documentación oficial de spark [última consulta 30 de septiembre del 2022](https://spark.apache.org/docs/latest/rdd-programming-guide.html)

al realizar parallelize por defecto se hacen dos particiones de RRD, por tanto al hacer el reduce se calcularía la operación en las respectivas dos particiones y tras esto se haría la resta de los resultados de ambas. 

Si quisiéramos que el resultado fuera el esperado deberiamos de especificar de que solo hubiera una partición (ver la celda siguiente)



```python
# Celda de ejemplo de cómo habría que programarlo para que el resultado fuera el correcto: 

#Tiene sentido la reducción(elem1-elem2)?
numeros = sc.parallelize([1,2,3,4,5,6,7,8,9,10],1)

print (numeros.reduce(lambda elem1,elem2: elem1-elem2))
```

    -53


Como es de esperar si variamos el número de particiones la salida se verá afectada. Esto nos hace pensar que para poder paralelizar este tipo de operaciones es necesario que las operaciones sean **asociativas y conmutativas**. 


```python
#Tiene sentido la reducción(elem1-elem2) si la repetimos y los elementos están en otro orden?
numeros = sc.parallelize([2,1,4,3,5,6,7,8,9,10])

print (numeros.reduce(lambda elem1,elem2: elem1-elem2))
```

    17


Como podemos ver no tiene sentido el resultado, ya que debería ser negativo.

```python
#Tiene sentido la reducción(elem1-elem2)? ¿qué pasa si cambiamos la paralelización a 5? ¿y si es 2?
numeros = sc.parallelize([1,2,3,4,5,6,7,8,9,10],5)

print (numeros.reduce(lambda elem1,elem2: elem1-elem2))
```

    3

Ya hemos respondido a estas preguntas en **Respuesta TS1.3**. 
#Acciones


```python
numeros = sc.parallelize([5,3,1,2,4])

print (numeros.take(3))
#¿Qué sucede si ponemos 30 en vez de 3 elementos?
```

    [5, 3, 1]


### Solución 

Tomará los 30 primeros, en nuestro caso, puesto que solo hay 5 tomará los 5 prieros. 


```python
numeros = sc.parallelize([3,2,1,4,5])
# (La segunda función se utiliza como índice de ordenación)
print (numeros.takeOrdered(3, lambda elem: -elem))
# La función lambda se está utilizando para crear el índice de la lista de ordenación 
```

    [5, 4, 3]


## Pregunta TS1.4 ¿Cómo lo ordenarías para que primero aparezcan los impares y luego los pares?


```python
#¿Cómo lo ordenarías para que primero aparezcan los impares y luego los pares?
numeros = sc.parallelize([3,2,1,4,5])
#print(numeros...
print (numeros.takeOrdered(5, lambda elem: -(elem %2)*elem))

```

    [5, 3, 1, 2, 4]



## Pregunta TS1.5 ¿Cuántos elementos tiene cada rdd? ¿Cuál tiene más?


```python
#¿Cuántos elementos tiene cada rdd? ¿Cuál tiene más?
print (palabras_flat.collect())
print (palabras_map.collect())
```

    ['a', 'a', 'b', 'a', 'b', 'c']
    [[], ['a'], ['a', 'b'], ['a', 'b', 'c']]


### Respuesta TS1.5
El primero posee 6, mientras que el otro 4. Es evidente que el primero posee más. 

## Pregunta TS1.6 ¿De qué tipo son los elementos del rdd palabras_map? ¿Por qué palabras_map tiene el primer elemento vacío?

### Respuesta TS1.6

Son listas. 

Está vacía porque se generado a partir de un string sin ningún carácter ni espacio, que tras ser *separados* por la función  `split` se traduce en la lista vacía. 


```python
palabras_flat.take(1)
```




    ['a']




```python
palabras_map.take(1)

```




    [[]]



## Pregunta TS1.7. Prueba la transformación distinct si lo aplicamos a cadenas.


```python
# Prueba la transformación distinct si lo aplicamos a cadenas.¿Que realiza?
log = sc.parallelize(['E: e21', 'I: i11', 'W: w12', 'I: i11', 'W: w13', 'E: e45'])
print( log.distinct().collect() )
```

    ['I: i11', 'W: w12', 'W: w13', 'E: e45', 'E: e21']


### Respuesta TS1.7 

Podemos observar que muestra los elementos de la lista sin repeticiones. 

## Pregunta TS1.8 ¿Cómo se podría obtener la misma salida pero utilizando una sola transformación y sin realizar la unión?


```python
log = sc.parallelize(['E: e21', 'I: i11', 'W: w12', 'I: i11', 'W: w13', 'E: e45'])

infos = log.filter(lambda elemento: elemento[0]=='I')
errors = log.filter(lambda elemento: elemento[0]=='E')

inferr = infos.union(errors)

print (inferr.collect())
```

    ['I: i11', 'I: i11', 'E: e21', 'E: e45']


### Respuesta TS1.8 

Para ello vamos a realizar directamente el filtro donde se admitan ambas condiciones.  


```python
#¿Cómo se podría obtener la misma salida pero utilizando una sola transformación y sin realizar la unión?
log = sc.parallelize(['E: e21', 'I: i11', 'W: w12', 'I: i11', 'W: w13', 'E: e45'])
result = log.filter(lambda elemento: elemento[0]=='I' or elemento[0]=='E')
print(result.collect())

```

    ['E: e21', 'I: i11', 'I: i11', 'E: e45']


# Diferencias entre hacer map y recuce con números y cadenas


```python
numeros = sc.parallelize([1,2,3,4,5])

print (numeros.reduce(lambda elem1,elem2: elem2+elem1))
```

    15



```python
#Tiene sentido esta operación?¿Cómo sale el resultado? pruebe los resultados
# empezando con 2 elementos e ir incrementando hasta tener 5

numeros = sc.parallelize([1,2,3,4,5])

print (numeros.reduce(lambda elem1,elem2: elem2-elem1))
```

    3


No tiene sentido porque el resultado no es correcto. 


```python
for i in range(2,6):
  print(f"Para {i} elementos, la lista que se  es {list(range(1,i+1))}")
  numeros = sc.parallelize(list(range(1,i+1)))
  print (f"Para {i} elementos", numeros.reduce(lambda elem1,elem2: elem2-elem1))
```

    Para 2 elementos, la lista que se  es [1, 2]
    Para 2 elementos 1
    Para 3 elementos, la lista que se  es [1, 2, 3]
    Para 3 elementos 0
    Para 4 elementos, la lista que se  es [1, 2, 3, 4]
    Para 4 elementos 0
    Para 5 elementos, la lista que se  es [1, 2, 3, 4, 5]
    Para 5 elementos 3



```python
palabras = sc.parallelize(['HOLA', 'Que', 'TAL', 'Bien'])

pal_minus = palabras.map(lambda elemento: elemento.lower())

print (pal_minus.reduce(lambda elem1,elem2: elem1+"-"+elem2))
#y esta tiene sentido esta operación?
#Sí tiene 
# Qué pasa si ponemos elem2+"-"+elem1
#
```

    hola-que-tal-bien



```python
print (pal_minus.reduce(lambda elem1,elem2: elem2+"-"+elem1))
```

    bien-tal-que-hola


En este ejemplo se pone de manifiesto claramente lo que se comentó en la respuesta [Respuesta TS1.3](#respuesta-ts1.3). 


```python
r = sc.parallelize([('A', 1),('C', 4),('A', 1),('B', 1),('B', 4)])
rr = r.reduceByKey(lambda v1,v2:v1+v2)
print (rr.collect())
```

    [('C', 4), ('A', 2), ('B', 5)]



```python
r = sc.parallelize([('A', 1),('C', 4),('A', 1),('B', 1),('B', 4)])
rr1 = r.reduceByKey(lambda v1,v2:v1+v2)
print (rr1.collect())
rr2 = rr1.reduceByKey(lambda v1,v2:v1)
print (rr2.collect())

```

    [('C', 4), ('A', 2), ('B', 5)]
    [('C', 4), ('A', 2), ('B', 5)]


## Pregunta TS1.9 ¿Cómo explica el funcionamiento de las celdas anteriores?


```python
#palabras = sc.parallelize(['HOLA', 'Que', 'TAL', 'Bien'])
#print (pal_minus.reduce(lambda elem1,elem2: elem1+"-"+elem2))
#y esta tiene sentido esta operación?
# Qué pasa si ponemos elem2+"-"+elem1
```


```python
r = sc.parallelize([('A', 1),('C', 4),('A', 1),('B', 1),('B', 4)])
rr = r.reduceByKey(lambda v1,v2:v1+v2)
print (rr.collect())
```

    [('C', 4), ('A', 2), ('B', 5)]



De cada dupla suma el valor de los elementos que comparten la misma clave. 


```python
r = sc.parallelize([('A', 1),('C', 4),('A', 1),('B', 1),('B', 4)])
rr1 = r.reduceByKey(lambda v1,v2:v1+v2)
print (rr1.collect())
rr2 = rr1.reduceByKey(lambda v1,v2:v1)
print (rr2.collect())

```

    [('C', 4), ('A', 2), ('B', 5)]
    [('C', 4), ('A', 2), ('B', 5)]


# TS1.10 Responda a las preguntas planteadas al hacer los cambios sugeridos en las siguiente celdas


```python
r = sc.parallelize([('A', 1),('C', 4),('A', 1),('B', 1),('B', 4)])
rr1 = r.reduceByKey(lambda v1,v2:'hola')
print (rr1.collect())
rr2 = rr1.reduceByKey(lambda v1,v2:'hola')
print (rr2.collect())
```

    [('C', 4), ('A', 'hola'), ('B', 'hola')]
    [('C', 4), ('A', 'hola'), ('B', 'hola')]



```python
r = sc.parallelize([('A', 1),('C', 2),('A', 3),('B', 4),('B', 5)])
rr = r.groupByKey()
res= rr.collect()
for k,v in res:
    print (k, list(v))
# Que operación se puede realizar al RDD rr para que la operacion sea como un reduceByKey
#¿Y simular un group con un reduceByKey y un map?
```

    C [2]
    A [1, 3]
    B [4, 5]


### Respuestas TS1.10 

#### Transformar groupByKey para que se transforme como un reduce 


```python
from functools import reduce 
r = sc.parallelize([('A', 1),('C', 2),('A', 3),('B', 4),('B', 5)])
# Transformación con sentencias de python 
rr = (r.groupByKey()).collect()
res = list(map( lambda v: (v[0], reduce( lambda x,y: "hola", list(v[1]))), rr))
print(res)
```

    [('C', 2), ('A', 'hola'), ('B', 'hola')]


### Simular un GroupBy con un reduceByKey y un map

De cada par clave valor la idea es introducir cada valor a una lista y despues realizar `reduceByKey` donde la operación de *reduce* es concaternar las listas. 


```python
r = sc.parallelize([('A', 1),('C', 4),('A', 1),('B', 1),('B', 4)])
r_map = r.map(lambda x: (x[0],[x[1]]))
rr1 = r_map.reduceByKey(lambda v1,v2: v1+v2)
print (rr1.collect())
```

    [('C', [4]), ('A', [1, 1]), ('B', [1, 4])]



```python
rdd1 = sc.parallelize([('A',1),('B',2),('C',3)])
rdd2 = sc.parallelize([('A',4),('B',5),('C',6)])

rddjoin = rdd1.join(rdd2)

print (rddjoin.collect())
# Prueba a cambiar las claves del rdd1 y rdd2 para ver cuántos elementos se crean
```

    [('A', (1, 4)), ('B', (2, 5)), ('C', (3, 6))]



```python
rdd1 = sc.parallelize([('A',1),('B',2),('C',3)])
rdd2 = sc.parallelize([('A',4),('A',5),('B',6),('D',7)])

rddjoin = rdd1.join(rdd2)

print (rddjoin.collect())
#Modifica join por leftOuterJoin, rightOuterJoin y fullOuterJoin ¿Qué sucede? 

```

    [('A', (1, 4)), ('A', (1, 5)), ('B', (2, 6))]



```python
rdd = sc.parallelize([('A',1),('B',2),('C',3),('A',4),('A',5),('B',6)])

res = rdd.sortByKey(False)

print (res.collect())
```

    [('C', 3), ('B', 2), ('B', 6), ('A', 1), ('A', 4), ('A', 5)]



```



```python
# Recupera el fichero guardado y realiza la suma
n2 = sc.textFile('salida').map(lambda a:int(a))
print(n2.reduce(lambda v1,v2: v1 + v2))

# Prueba este código y mira qué genera?
# Borra la salida y cambia las particiones en parallelize ¿Qué sucede? 
#    (pe c.parallelize(xrange(0,1000),8))

```

    499500


## Pregunta TS1.11 Borra la salida y cambia las particiones en parallelize ¿Qué sucede? 
  (pe c.parallelize(xrange(0,1000),8))


```python
# Recupera el fichero guardado y realiza la suma
n2 = sc.textFile('salida').map(lambda a:int(a))
print(n2.reduce(lambda v1,v2: v1 + v2))

# Prueba este código y mira qué genera?
# Borra la salida y cambia las particiones en parallelize ¿Qué sucede? 
sc.parallelize(range(0,1000),8)
numeros.saveAsTextFile('salida3')

```

    499500



```python
# Prueba este código y mira qué genera?
# Borra la salida y cambia las particiones en parallelize ¿Qué sucede? 
%rm -rf salida3
n = sc.parallelize(range(0,17),8)
n.saveAsTextFile('salida3')
%cat salida3/part-00000

```

    0
    1


### Respuesta TS1.11
Podemos observar que cada proceso en paralelo sobreescribe el fichero y por tanto solo se refleja la última que ha sido guardada. 


```python
%cat salida3/part-00000
```

    0
    1


    '/content'


### Procesando el QUIJOTE


Transformaciones


```python
charsPerLine = quijote.map(lambda s: len(s))
allWords = quijote.flatMap(lambda s: s.split())
allWordsNoArticles = allWords.filter(lambda a: a.lower() not in ["el", "la"])
allWordsUnique = allWords.map(lambda s: s.lower()).distinct()
sampleWords = allWords.sample(withReplacement=True, fraction=0.2, seed=666)
weirdSampling = sampleWords.union(allWordsNoArticles.sample(False, fraction=0.3))
# cómo funciona cada transformación
```

- `charsPerLine`: Caracteres por líneas. 
- `allWords`: Contiene un RRD cuyos elementos son variables de tipo `String` con las palabras del quijote.
- `allWordsNoArticles`: Minimiza las palabras del RDD `allWords` y devuelve todas menos los artículos `el` y `la`.
- `allWordsUnique`:  Minimiza las palabras del RDD `allWords` y deja solo un representante. 
- `sampleWords`: Toma una muestra aleatora de tamaño aproximadamente $20\%$  del RRD de `allWords` con remplazamiento y semilla $666$. 
- `weirdSampling`: Devuelve un RDD que contiene los elementos de `sampleWords` y una mueestra aleatoria de `allWordsNoArticles` sin remplazamiento y de tamaño alrededor del $30\%$.

Se detallarán las funciones en la respuesta a TS2.1. 



```python
allWordsUnique.take(10)
```




    ['el',
     'hidalgo',
     'don',
     'mancha',
     'saavedra',
     'que',
     'condición',
     'y',
     'del',
     'd.']



## Pregunta TS2.1 Explica la utilidad de cada transformación y detalle para cada una de ellas si cambia el número de elementos en el RDD resultante. Es decir si el RDD de partida tiene N elementos, y el de salida M elementos, indica si N>M, N=M o N<M.

-  map
- flatmap
- filter
- distinct
- sample
- union


## Respuesta TS2.1

### Map

Los argumentos de `map` son `map(f) ` donde `f` a cuyo dominio pertenecen los elementos del RDD. 

El RDD resultante son las respectivas imágenes de aplicar los elementos del RDD de entrada a `f`. 

Es por ello que N = M.

Ejemplo: 



```python
numeros = sc.parallelize(list(range(10)))
rdd = numeros.map(lambda x: x*10+x)
print(rdd.collect())
```

    [0, 11, 22, 33, 44, 55, 66, 77, 88, 99]


### Flatmap
Funciona como un `map` el cual generaría un RDD cuyos elementos deben ser iterables, a éstos se le aplica la operación de *flattening*, es decir *saca* los elementos que estén en una lista (solo actúa a un nivel, una lista de lista devolvería una lista). 

No se puede establecer que ninguna relación entre N y M ya que si hay listas  vacías entonces pudiera darse el caso que N > M (ver ejemplo 2 que se mostrará a continuación). Aunque si imponemos que no hay listas vacías entonces $N \leq M$. 




```python
# Ejemplo 1: N < M
numeros = sc.parallelize(list(range(10)))
rdd = numeros.flatMap(lambda x: [x,x*10+x])
print(rdd.collect())

# Ejemplo 2: N > M
def lista_recursiva (n):
  if n <= 1:
    return []
  else:
    return [lista_recursiva(n-1)]
numeros = sc.parallelize(list(range(1,4)))
print("Ejemplo 2: si se aplicara un map solo  solo:\n",
      numeros.map(lambda x: lista_recursiva(x)).collect())
print("Ejemplo 2: de qué habría en un flapMap:\n",
      numeros.flatMap(lambda x: lista_recursiva(x)).collect())

# Ejemplo 3: N = M
def lista_recursiva_2 (n):
  if n <= 1:
    return [n]
  else:
    return [lista_recursiva_2(n-1)]

numeros = sc.parallelize(list(range(3)))
print("Ejemplo 3: si se aplicara un map solo:\n",
      numeros.map(lambda x: lista_recursiva_2(x)).collect())

print("Ejemplo 3: si se aplicara un flapMap:\n",
      numeros.flatMap(lambda x: lista_recursiva_2(x)).collect())


```

    [0, 0, 1, 11, 2, 22, 3, 33, 4, 44, 5, 55, 6, 66, 7, 77, 8, 88, 9, 99]
    Ejemplo 2: si se aplicara un map solo  solo:
     [[], [[]], [[[]]]]
    Ejemplo 2: de qué habría en un flapMap:
     [[], [[]]]
    Ejemplo 3: si se aplicara un map solo:
     [[0], [1], [[1]]]
    Ejemplo 3: si se aplicara un flapMap:
     [0, 1, [1]]


## Filter 

Recive una función con dominio en los elemntos del RDD de partida y salida un booleano. 
Devuelve un RDD con los elementos cuya imagen sea `True`.
En este caso $N \geq M$, donde la igualdad se dará si todos los elementos tienen imagen `True`.


```python
numeros = sc.parallelize([0,1,2,3,4])
rddi = numeros.filter(lambda x: x < 2) # Mostrará los números menores de dos
print(rddi.collect())
```

    [0, 1]


## Distinct 

Elimina los elementos repetidos dejando solo una incidencia de ellos. 
Por tanto $N \geq M$ donde la igualdad se da si no hay ningún elemento repetido. 



```python
# Ejemplo sencillo
n = sc.parallelize([0,0,1,1,0,2])
rdd = n.distinct()
print(rdd.collect())

# Ejemplo complejo, compara elemento a elemento
n = sc.parallelize([('a',2),('a',2), ('a',1), ('b',1),])
rdd = n.distinct()
print(rdd.collect())
```

    [0, 2, 1]
    [('a', 1), ('a', 2), ('b', 1)]


## Sample

Es un mecanismo para extraes muestras aleatorias, sus argumentos son:  

`sample(withReplacement, fraction, seed=None`

- `withReplacement`: booleano que indica si hay reemplazamiento.   
- `fraction`: decimal en $[0,1]$ fracción de la mustra que se desea. **No se garantiza que la muestra siempre de esa fracción, puede ser menor o mayor**.  
- `seed` semilla.   

Por la observación de que no se garantice la fracción no se puede establecer ninguna relación de orden entre $N$ y $M$, mostramos tales situaciones a continuación. 


```python
# Ejemplo 1: N > M
print(f'RDD de partida {list(range(5))} N = 5')
print('Ejemplo 1: N > M')
numeros = sc.parallelize(list(range(5)))
rdd = numeros.sample(True, 0.5, 0)
print(rdd.collect())
# Ejemplo 2: N < M
print('Ejemplo 2: N < M')
numeros = sc.parallelize(list(range(5)))
rdd = numeros.sample(True, 1,1)
print(rdd.collect())
# Ejemplo 3: N = M
print('Ejemplo 3: N = M')
numeros = sc.parallelize(list(range(5)))
rdd = numeros.sample(False, 1,1)
print(rdd.collect())
```

    RDD de partida [0, 1, 2, 3, 4] N = 5
    Ejemplo 1: N > M
    [3, 4]
    Ejemplo 2: N < M
    [0, 0, 1, 1, 2, 3, 3, 4, 4, 4, 4]
    Ejemplo 3: N = M
    [0, 1, 2, 3, 4]


## Union

Dados dos RDD devuelve la unión de los dos, es decir, uno RDD con todos los elementos de ambos (cabe destacar que en caso de estar repetidos los elementos los volvería a repetir). 
Para este si $N_1, N_2$ son los respectivos tamaños de los RDD de entrada y $M$ el de salida se cumple que
$N_1 + N_2 = M$.


```python
# Ejemplos 1: 
n1 = sc.parallelize([1,2,3])
n2 = sc.parallelize([1,3,5])
m = n1.union(n2)
print (m.collect())

# Ejemplo 2: Une elementos de distinto tipo
n1 = sc.parallelize([1,2,3])
n2 = sc.parallelize(['1','3','5'])
m = n1.union(n2)
print (m.collect())
```

    [1, 2, 3, 1, 3, 5]
    [1, 2, 3, '1', '3', '5']



## Pregunta TS2.2 Explica el funcionamiento de cada acción anterior 



- Tengamos presente que `quijote` es un RDD donde cada elemento es una línea del quijote. 
Por lo que  `quijote.count()` dirá el número de líneas del quijote. 

- La variable `charsPerLine` contiene el número de caracteres que hay en cada línea, luego
`numChars = charsPerLine.reduce(lambda a,b: a+b) # also charsPerLine.sum(` devuelve el número de caracteres total. 

- La variable `allWordsNoArticles` continee las palabras minimizadas con un único representante. 
por lo que 
```python
sortedWordsByLength = allWordsNoArticles.takeOrdered(20, key=lambda x: -len(x))
```
Toma las 20 primeras ordenadas de mayor tamaño a menor. 



## Pregunta: Implementa la opción count de otra manera:


*   utilizando transformaciones map y reduce
*   utilizando solo reduce en caso de que sea posible.

### Respuesta 

#### Utilizando transformaciones map y reduce

Cada línea se mapeará por un 1 y con reduce se sumarán. 


```python
lineas = quijote.map(lambda linea : 1)
suma_lineas = lineas.reduce(lambda x,y: x+y)
print(suma_lineas)
```

    5534


### Utilizando solo reduce 

La idea clave está en la función del reduce, que debe de ser capaz de tener como entrada cadenas de caracteres y números. 

Si se encuentra con una cadena de caracter es porque se trata de una línea que debe de ser contada, por tanto la cambia a 1 y la suma. 

Si se encuentra un número es de la cuenta anteriores y por tanto lo suma tal cual. 


```python
def reduce_suma_lineas(x,y):
  if type(x) == str:
    x = 1
  if type(y) == str:
    y = 1
  return x+y

suma_lineas = quijote.reduce(reduce_suma_lineas)
print(suma_lineas)
```

    5534

## Pregunta TS2.3 Explica el proposito de cada una de las operaciones anteriores

Las operaciones en cadena son: 

1. `. joinFreq.map(lambda e: (e[0], (e[1][0] - e[1][1])/(e[1][0] + e[1][1])))`
Devuelve una tupla con la palabra y la diferencia entre el números de operaciones en la primera parte del Quijote menos las apariciones en la segunda parte entre la suma total de las apariciones en los dos libros. 

La idea que subyace en el cálculo es ver si la frecuencia de aparición de palabras es la misma. Será próxima a cero si es así, cercana a menos uno si aparece mayoritariamente el el segundo y cercana a uno si lo es en primero. 

2. `takeOrdered(10, lambda v: -v[1])`
Ordena de mayor a menor las frecuencias optenidas. 


3. `joinFreq.map(lambda e: (e[0], (e[1][0] - e[1][1])/(e[1][0] + e[1][1]))`

Repite el paso 1. 

4. `takeOrdered(10, lambda v: +v[1])`
Ahora ordena de de menor a mayor las frecuencias. 

## Pregunta TS2.4 ¿Cómo puede implementarse la frecuencia con groupByKey y transformaciones?


```python
frequencies_group = words.groupByKey().map(lambda e: (e[0],sum(list(e[1]))))
frequencies_group.take(10)
```




    [('el', 1232),
     ('hidalgo', 14),
     ('don', 370),
     ('mancha', 26),
     ('saavedra', 1),
     ('que', 3032),
     ('condición', 16),
     ('y', 2573),
     ('del', 415),
     ('d', 14)]



# Optimizaciones


## Pregunta TS2.5 ¿Cuál de las dos siguientes celdas es más eficiente? Justifique la respuesta.

La más eficiente es la segunda, puesto que se está guardando en caché los datos y la operación de cálculo de apariciones no tiene que repetirse. 


```python
joinFreq.map(lambda e: (e[0], (e[1][0] - e[1][1])/(e[1][0] + e[1][1]))).takeOrdered(10, lambda v: -v[1]), joinFreq.map(lambda e: (e[0], (e[1][0] - e[1][1])/(e[1][0] + e[1][1]))).takeOrdered(10, lambda v: +v[1])
```




    ([('pieza', 0.8),
      ('corral', 0.8),
      ('rodela', 0.7777777777777778),
      ('curar', 0.75),
      ('valle', 0.75),
      ('entierro', 0.75),
      ('oh', 0.7142857142857143),
      ('licor', 0.7142857142857143),
      ('difunto', 0.7142857142857143),
      ('pago', 0.6666666666666666)],
     [('teresa', -0.9767441860465116),
      ('roque', -0.96),
      ('paje', -0.9565217391304348),
      ('duque', -0.9565217391304348),
      ('blanca', -0.9565217391304348),
      ('gobernador', -0.9503105590062112),
      ('diego', -0.9459459459459459),
      ('tarde', -0.9428571428571428),
      ('mesmo', -0.9381443298969072),
      ('letras', -0.9354838709677419)])




```python
result = joinFreq.map(lambda e: (e[0], (e[1][0] - e[1][1])/(e[1][0] + e[1][1])))
result.cache()
result.takeOrdered(10, lambda v: -v[1]), result.takeOrdered(10, lambda v: +v[1])
```




    ([('pieza', 0.8),
      ('corral', 0.8),
      ('rodela', 0.7777777777777778),
      ('curar', 0.75),
      ('valle', 0.75),
      ('entierro', 0.75),
      ('oh', 0.7142857142857143),
      ('licor', 0.7142857142857143),
      ('difunto', 0.7142857142857143),
      ('pago', 0.6666666666666666)],
     [('teresa', -0.9767441860465116),
      ('roque', -0.96),
      ('paje', -0.9565217391304348),
      ('duque', -0.9565217391304348),
      ('blanca', -0.9565217391304348),
      ('gobernador', -0.9503105590062112),
      ('diego', -0.9459459459459459),
      ('tarde', -0.9428571428571428),
      ('mesmo', -0.9381443298969072),
      ('letras', -0.9354838709677419)])




```python
result.coalesce(numPartitions=2) # Avoids the data movement, so it tries to balance inside each machine
result.repartition(numPartitions=2) # We don't care about data movement, this balance the whole thing to ensure all machines are used
```




    MapPartitionsRDD[633] at coalesce at NativeMethodAccessorImpl.java:0




```python
result.take(10)
allWords.cache() # allWords RDD must  stay in memory after computation, we made a checkpoint (well, it's a best effort, so must might be too strong)
result.take(10)
```




    [('el', -0.5620334162815499),
     ('hidalgo', -0.5),
     ('don', -0.6255060728744939),
     ('mancha', -0.5905511811023622),
     ('saavedra', 0.0),
     ('que', -0.5361077111383109),
     ('y', -0.5789904278818621),
     ('del', -0.5281409891984082),
     ('en', -0.5704722945332837),
     ('cuyo', -0.5217391304347826)]





## Pregunta TS2.6 Antes de guardar el fichero, utilice coalesce con diferente valores ¿Cuál es la diferencia?

De acorde a la documentación de [coalescence](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.coalesce.html)



```python
allWords2.getNumPartitions()
```




    2




```python
for j in range(1,10):
  print("numero de particiones = {}".format(j))
  !rm -rf words_coalesce2
  allWords = allWords2.coalesce(numPartitions=j).saveAsTextFile("words_coalesce2")
  !ls words_coalesce2


```

    numero de particiones = 1
    part-00000  _SUCCESS
    numero de particiones = 2
    part-00000  part-00001	_SUCCESS
    numero de particiones = 3
    part-00000  part-00001	_SUCCESS
    numero de particiones = 4
    part-00000  part-00001	_SUCCESS
    numero de particiones = 5
    part-00000  part-00001	_SUCCESS
    numero de particiones = 6
    part-00000  part-00001	_SUCCESS
    numero de particiones = 7
    part-00000  part-00001	_SUCCESS
    numero de particiones = 8
    part-00000  part-00001	_SUCCESS
    numero de particiones = 9
    part-00000  part-00001	_SUCCESS


Se observa que el número de particiones de disco se estanca cuando el número de particiones llega a 2, dejando así de incrementar su valor para las sucesivas. Cabe destacar, que sería equivalente el usar la función repartition, ya que no se tiene en cuenta que los datos se muevan en el disco para relizar dichas particiones.

# Parte3: Práctica opcional Hadoop y Spark


## Dataset 1: Análisis del comportamiento local de los diferentes equipos que han jugado en la Premier League inglesa  

### Descripción del dataset

Puede encontrar los datos en [aquí](https://www.kaggle.com/datasets/zaeemnalla/premier-league), nos vamos a centrar en el fichero `results.csv` que cuenta con las siguientes 6 columnas:

- `home_team`: String, nombre del equipo que juega en casa, hay 39 valores únicos.

- `home_goals`: Number, número de goles marcados por el equipo local.

- `away_goals`:  Número de goles marcados por el equipo visitante.

- `results`: String que puede ser `H,A,D` he indica al ganador. `H` para el local, `A` para el visitante, `D` si hay empate.

En total hay $4560$ datos donde todos los atributos son correctos (ie no contiene valores perdidos o mal clasificados).

### Objetivo de la aplicación

Se pretende saber cómo se comportan en local los equipos, es decir responder a las siguientes preguntas:

1. ¿Cómo juega cada equipo en local en cada temporada?
2. ¿Existe una tendencia a ganar más jugando como local?

### Estructura de la solución propuesta

Los pasos a seguir son:

- Se cargarán los datos desde google drive.
- No se realizará un preprocesamiento ya que todos los datos son correctos.
- Con pyspark se realizará el tratamiento de los datos necesario para extraer la información.

### Subida de datos

Se ha añadido a google drive el fichero de `resultados` (puede encontrarlo en la carpeta).

y ahora vamos a proceder a leerlo instalando antes todos los paquetes necesarios como ya hicimos en la práctica de Spark.

```python
# instalar Java
!apt-get install openjdk-8-jdk-headless -qq > /dev/null
# Descargar la ultima versión de java ( comprobar que existen los path de descarga)
!wget -q https://downloads.apache.org/spark/spark-3.3.0/spark-3.3.0-bin-hadoop3.tgz
!tar xf spark-3.3.0-bin-hadoop3.tgz
# instalar pyspark
!pip install -q pyspark
# variables de entorno
import os # libreria de manejo del sistema operativo
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.3.0-bin-hadoop3"
from pyspark.sql import SparkSession
# Iniciamos sesión con spark
APP_NAME = "PDGE-AnalisisFutbol"
SPARK_URL = "local[*]"
spark = SparkSession.builder.appName(APP_NAME).master(SPARK_URL).getOrCreate()
spark
```

    <div>
        <p><b>SparkSession - in-memory</b></p>

<div>
    <p><b>SparkContext</b></p>

    <p><a href="http://82af4c36730e:4040">Spark UI</a></p>

    <dl>
      <dt>Version</dt>
        <dd><code>v3.3.0</code></dd>
      <dt>Master</dt>
        <dd><code>local[*]</code></dd>
      <dt>AppName</dt>
        <dd><code>PDGE-AnalisisFutbol</code></dd>
    </dl>
</div>

    </div>

```python
# Cargamos contexto de spark
sc = spark.sparkContext
#obtener el contexto de ejecución de Spark del Driver.
sc
```

<div>
    <p><b>SparkContext</b></p>

    <p><a href="http://82af4c36730e:4040">Spark UI</a></p>

    <dl>
      <dt>Version</dt>
        <dd><code>v3.3.0</code></dd>
      <dt>Master</dt>
        <dd><code>local[*]</code></dd>
      <dt>AppName</dt>
        <dd><code>PDGE-AnalisisFutbol</code></dd>
    </dl>
</div>

```python
# Lectura del fichero desde google drive
from google.colab import drive
#drive.flush_and_unmount()
drive.mount('/content/gdrive')
```

    Drive already mounted at /content/gdrive; to attempt to forcibly remount, call drive.mount("/content/gdrive", force_remount=True).

```python
# Nota: He tenido que añadir la ruta esta para concreta para que lo lea en mi drive
data = sc.textFile("gdrive/MyDrive/Colab\ Notebooks/results.csv")

```

```python
data.take(10)
```

    ['home_team,away_team,home_goals,away_goals,result,season',
     'Sheffield United,Liverpool,1.0,1.0,D,2006-2007',
     'Arsenal,Aston Villa,1.0,1.0,D,2006-2007',
     'Everton,Watford,2.0,1.0,H,2006-2007',
     'Newcastle United,Wigan Athletic,2.0,1.0,H,2006-2007',
     'Portsmouth,Blackburn Rovers,3.0,0.0,H,2006-2007',
     'Reading,Middlesbrough,3.0,2.0,H,2006-2007',
     'West Ham United,Charlton Athletic,3.0,1.0,H,2006-2007',
     'Bolton Wanderers,Tottenham Hotspur,2.0,0.0,H,2006-2007',
     'Manchester United,Fulham,5.0,1.0,H,2006-2007']

Vamos a procesar los datos para que podamos trabajar con ellos.

Para ello:

1. Eliminamos primera fila
2. Realizamos un split elemento a elemento.
Esto nos dará una lista de listas, donde para cada sublista los elementos serán:

| Atributo | Índice |
| --- | --- |
 home_team | 0
away_team | 1
home_goals | 2
 away_goals | 3
 result | 4
  season |  5

```python
data_without_head = data.filter(lambda x: "-" in x)
splited_data = data_without_head.map(lambda d: tuple(d.split(',')))
splited_data.take(5)
```

    [('Sheffield United', 'Liverpool', '1.0', '1.0', 'D', '2006-2007'),
     ('Arsenal', 'Aston Villa', '1.0', '1.0', 'D', '2006-2007'),
     ('Everton', 'Watford', '2.0', '1.0', 'H', '2006-2007'),
     ('Newcastle United', 'Wigan Athletic', '2.0', '1.0', 'H', '2006-2007'),
     ('Portsmouth', 'Blackburn Rovers', '3.0', '0.0', 'H', '2006-2007')]

Con esto ya estamos preparados para realizar los primeros cálculos.

Los equipos que más victorias tenga en local por años, para ello primero sacaremos los años distintos que hay.

```python
get_season = splited_data.map(lambda x: x[5]).distinct()
season_list = get_season.collect()
season_list.sort()
print(season_list)
```

    ['2006-2007', '2007-2008', '2008-2009', '2009-2010', '2010-2011', '2011-2012', '2012-2013', '2013-2014', '2014-2015', '2015-2016', '2016-2017', '2017-2018']

```python
results = {}
# Solo si se ha ganado contará como una victoria en local
letter_to_victory = lambda x :  x == 'H' if 1 else 0 
for season in season_list:
  # 1. Filtramos los datos de cada temporada
  # 2. Mapeamos cada tupla para que solo quede (clave: equipo local, valor: 1 si ganó 0 en caso contrario) 
  filtered_season = splited_data.filter(lambda x: x[5]== season).map(lambda x : (x[0],letter_to_victory(x[4])))
  # 3. Sumamos las victorias de cada equipo
  reduced = filtered_season.reduceByKey(lambda a,b: a+b)
  results[season] = reduced.collect()
  results[season].sort(key= lambda x: -x[1]) # Los mejores resultados antes
```

#### Muestra de cada victoria

Como resultado de la operación tenemos un diccionario con cada una de las vistorias en local ordenadas

```python
top = 3 # número de mejores jugadores
for season in [season_list[2],season_list[5], season_list[-3], season_list[-1]]:
  print(f'\nLos {top} mejores equipos en {season} fueron:')
  equipos = results[season][:top]
  [print(f"{i+1}. {equipos[i][0]} con {equipos[i][1]} victorias locales") for i in range(0,top)]
```

    Los 3 mejores equipos en 2008-2009 fueron:
    1. Manchester United con 16 victorias locales
    2. Manchester City con 13 victorias locales
    3. Liverpool con 12 victorias locales
    
    Los 3 mejores equipos en 2011-2012 fueron:
    1. Manchester City con 18 victorias locales
    2. Manchester United con 15 victorias locales
    3. Tottenham Hotspur con 13 victorias locales
    
    Los 3 mejores equipos en 2015-2016 fueron:
    1. Manchester United con 12 victorias locales
    2. Manchester City con 12 victorias locales
    3. Leicester City con 12 victorias locales
    
    Los 3 mejores equipos en 2017-2018 fueron:
    1. Manchester City con 16 victorias locales
    2. Manchester United con 15 victorias locales
    3. Arsenal con 15 victorias locales

```python
# Vamos a mostrar los resultados totales
#!pip install -q tabulate # para imprimir tablas bonitas
from tabulate import tabulate 

cabecera = ['Posición', 'Equipo', 'Victorias locales']
for season in season_list:
  equipos = results[season]
  tabla = [ [i+1, e[0], e[1]]for i,e in enumerate(equipos)]
  print(f"\n\n#### Resultados temporada {season}  \n")
  print(tabulate(tabla, cabecera, tablefmt="github"))
```

## La siguiente cuestión es si existe un beneficio a jugar en local

Para ello analizaremos el comportamiento de un equipo (introducir su nombre en la variable `equipo`) y después el de todas los equipos.

Formulación del experimento:

Realizaremos un [Test de los rangos con signos de Wilcoxon](https://en.wikipedia.org/wiki/Wilcoxon_signed-rank_test) ya que es el que más se adecua a la situación,
donde
$$Z = \text{victorias locales} - \text{victorias como visitante}$$
para cada temporada.

- La hipótesis nula $H_0$: $Z$ sigue una distribución de media $0$.
- La hipótesis alternativa $H_1$: $Z$ **no** sigue una distribución de media 0.

Vamos a proceder a calcular la variable $Z$ para un equipo concreto:

### Descripción del tratamiento de los datos

Se pretende calcular la variable $z$, esto es la diferencia entre victorias como local y como visitante para un cierto equipo en una cierta temporada, para ello:

1. Se filtran los datos para quedarnos solo con los concernientes al equipo buscado.
2. Se transformarán los datos obtenidos en (1) en par `(temporada, valor)` la temporada es un `str`y valor es un entero $-1,1,0$. que indica respectivamente si ganó como visitante, como local o ninguno de esos casos.
3. Se suman todos los datos de la misma temporada y el resultado es una lista con $z$ por temporada.

```python
equipo = 'Manchester United'

def traductor(x:tuple,equipo:str):
  '''Devuelve:
  1 si el equipo gana como local
  -1 si el equipo gana como visitante
  0 en caso contrario
  '''
  es_local = x[0] == equipo
  salida = 0 
  # Si gana como local
  if es_local and x[4] == 'H':
    salida = 1
  # Si gana como visitante
  if not es_local and x[4] == 'A':
    salida = -1
  return salida

def victorias_locales_menos_visitante_por_equipo(equipo):
  # Nos quedamos solo donde juegue el equipo 
  partidos_relevantes = splited_data.filter(lambda x: x[0]== equipo or x[1] == equipo)
  # nos quedamos con las victorias locales por sesión 
  victorias_locales = partidos_relevantes.map(lambda x:(x[5],traductor(x, equipo)))
  Z = victorias_locales.reduceByKey(lambda x,y: x+y).map(lambda x: x[1])
  z_list = Z.collect()
  return z_list

print(f"La diferencia entre las victorias locales y como visitante del {equipo} son: ")
print(victorias_locales_menos_visitante_por_equipo(equipo))
```

    La diferencia entre las victorias locales y como visitante del Manchester United son: 
    [8, -2, 5, 2, 7, 4, 5, 13, 2, 4, -1, 5]

Vamos a proceder a calcular el test, para ello utilizaremos la biblioteca de [scipy](https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.wilcoxon.html)

```python
from scipy.stats import wilcoxon

def test_wilcoxon(equipo, a=0.05): 
  res = wilcoxon(victorias_locales_menos_visitante_por_equipo(equipo))
  if res.pvalue < a :
    print(f"Se ha rechazado la hipótesis nula con un p-valor de {res.pvalue} para el equipo {equipo}")
  else:
    print(f"No se ha podido rechazar la hipótesis nula ya que el p-valor es de {res.pvalue} para el equipo {equipo}")
```

```python
# Vamos a mostrar algunos ejemplos concretos
equipos = ['Arsenal', 'Liverpool', 'Portsmouth', 'Manchester United']
for equipo in equipos:
  test_wilcoxon(equipo)
```

    Se ha rechazado la hipótesis nula con un p-valor de 0.00048828125 para el equipo Arsenal


    /usr/local/lib/python3.7/dist-packages/scipy/stats/morestats.py:3141: UserWarning: Exact p-value calculation does not work if there are ties. Switching to normal approximation.
      warnings.warn("Exact p-value calculation does not work if there are "


    Se ha rechazado la hipótesis nula con un p-valor de 0.010799590882009212 para el equipo Liverpool
    No se ha podido rechazar la hipótesis nula ya que el p-valor es de 0.25 para el equipo Portsmouth
    Se ha rechazado la hipótesis nula con un p-valor de 0.00341796875 para el equipo Manchester United

Cabe destacar que el equipo Porsmouth descendió de división y que por tanto se tienen poco datos para su análisis, este puede ser el causante de que se pueda recharzar la hipótesis nula.  

### Diseño del tratamiento de los datos

Vamos a ver cuáles son los equipos y el número de temporadas en las que estuvieron, para ellos:  

1. Transformamos cada tupla a un par `(equipo, temporada)`.
2. Se eliminal las filas redundantes.
3. Sumamos cada aparición de cada equipo.

```python
team_and_season = splited_data.map(lambda x: (x[0],x[5])).distinct()
team_appears = team_and_season.map(lambda x:(x[0],1)).reduceByKey(lambda x,y:x+y)
teams = team_appears.collect()
teams.sort(key=lambda x: -x[1])
## Veamos cuales son los equipos que han jugado un mínimo de temporadas
temporadas_jugadas_minimas = 11 
equipos_persistentes = team_appears.filter(lambda x: x[1] >= temporadas_jugadas_minimas).map(lambda x: x[0]).collect()
```

Vamos a realizar el experimento para todos los equipos que han jugado más de `temporadas_jugadas_minimas`.

```python
print(f'Los equipos que han jugado más de {temporadas_jugadas_minimas} son: \n{equipos_persistentes}')
print('Veamos si estos equipos tienen más a ganar en local que en visitante')
for equipo in equipos_persistentes:
  test_wilcoxon(equipo)
```

    Los equipos que han jugado más de 11 son: 
    ['Manchester United', 'Chelsea', 'Manchester City', 'Arsenal', 'Everton', 'West Ham United', 'Tottenham Hotspur', 'Liverpool']
    Veamos si estos equipos tienen más a ganar en local que en visitante
    Se ha rechazado la hipótesis nula con un p-valor de 0.00341796875 para el equipo Manchester United
    Se ha rechazado la hipótesis nula con un p-valor de 0.04046183578416871 para el equipo Chelsea
    Se ha rechazado la hipótesis nula con un p-valor de 0.007466958429676204 para el equipo Manchester City
    Se ha rechazado la hipótesis nula con un p-valor de 0.00048828125 para el equipo Arsenal
    Se ha rechazado la hipótesis nula con un p-valor de 0.00146484375 para el equipo Everton
    Se ha rechazado la hipótesis nula con un p-valor de 0.0009765625 para el equipo West Ham United
    Se ha rechazado la hipótesis nula con un p-valor de 0.00048828125 para el equipo Tottenham Hotspur
    Se ha rechazado la hipótesis nula con un p-valor de 0.010799590882009212 para el equipo Liverpool

Como vemos todos los equipos que han jugado más de 11 temporadas tienden a ganar con más partidos como locales que como visitantes.

## Experimento 2 beneficio de ganar en local más para todos los equipos

### Diseño tratamiento de los datos

Se busca como objetivo calcular la diferencia a ganar como local o como visitante de los equipos independientemente de la temporada.

1. Filtramos los empates que en nuestro caso no aportan información.
2. Transformamos los datos resultante en pares  `(equipo ganados, -1 o 1)` donde $-1$ corresponde si gana como visitante y $1$ si lo hace como local.
3. Se suman todos los datos de un mismo equipo.

```python
def traductor_dos(x):
  '''
    La clave es el nombre del equipo y el año. 
  '''
  es_local = x[4] == 'H'
  if es_local:
    return x[0]+x[5], 1 # si gana como local sumamos 1 al equipo ese año
  else:
    return x[1]+x[5], -1  # si gana como visitante restamos uno al equipo ese año

def victorias_locales_menos_visitante():
  # Nos quedamos solo donde se gane a se pierda
  partidos_relevantes = splited_data.filter(lambda x: x[4] != 'D') 
  victorias_locales = partidos_relevantes.map(traductor_dos)
  Z = victorias_locales.reduceByKey(lambda x,y: x+y).map(lambda x: x[1])
  z_list = Z.collect()
  return z_list

a = 0.05
res = wilcoxon(victorias_locales_menos_visitante())
if res.pvalue < a :
  print(f"Se ha rechazado la hipótesis nula con un p-valor de {res.pvalue}.")
else:
    print(f"No se ha podido rechazar la hipótesis nula ya que el p-valor es de {res.pvalue}.")
```

    Se ha rechazado la hipótesis nula con un p-valor de 2.0695824579590253e-34.

# Conclusiones

Hemos extraído las victorias para cada equipo en cada temporada, que han sido:

#### Resultados temporada 2006-2007  

|   Posición | Equipo        | Victorias locales |
|------------|-------------------|---------------------|
|          1 | Manchester United |                  15 |
|          2 | Liverpool     |                14 |
|          3 | Chelsea       |                12 |
|          4 | Arsenal       |                12 |
|          5 | Tottenham Hotspur |                  12 |
|          6 | Reading       |                11 |
|          7 | Everton       |                11 |
|          8 | Portsmouth    |                11 |
|          9 | Middlesbrough |                10 |
|         10 | Bolton Wanderers  |                   9 |
|         11 | Blackburn Rovers  |                   9 |
|         12 | West Ham United   |                   8 |
|         13 | Newcastle United  |                   7 |
|         14 | Aston Villa   |                 7 |
|         15 | Fulham        |                 7 |
|         16 | Sheffield United  |                   7 |
|         17 | Charlton Athletic |                   7 |
|         18 | Manchester City   |                   5 |
|         19 | Wigan Athletic|                 5 |
|         20 | Watford       |                 3 |

#### Resultados temporada 2007-2008  

|   Posición | Equipo        | Victorias locales |
|------------|-------------------|---------------------|
|          1 | Manchester United |                  17 |
|          2 | Arsenal       |                14 |
|          3 | Chelsea       |                12 |
|          4 | Liverpool     |                12 |
|          5 | Manchester City   |                  11 |
|          6 | Everton       |                11 |
|          7 | Aston Villa   |                10 |
|          8 | Sunderland    |                 9 |
|          9 | Reading       |                 8 |
|         10 | Wigan Athletic|                 8 |
|         11 | Newcastle United  |                   8 |
|         12 | Tottenham Hotspur |                   8 |
|         13 | Blackburn Rovers  |                   8 |
|         14 | Bolton Wanderers  |                   7 |
|         15 | Middlesbrough |                 7 |
|         16 | West Ham United   |                   7 |
|         17 | Portsmouth    |                 7 |
|         18 | Birmingham City   |                   6 |
|         19 | Fulham        |                 5 |
|         20 | Derby County  |                 1 |

#### Resultados temporada 2008-2009  

|   Posición | Equipo           | Victorias locales |
|------------|----------------------|---------------------|
|          1 | Manchester United|                16 |
|          2 | Manchester City  |                13 |
|          3 | Liverpool        |                12 |
|          4 | Chelsea          |                11 |
|          5 | Fulham           |                11 |
|          6 | Arsenal          |                11 |
|          7 | Stoke City       |                10 |
|          8 | Tottenham Hotspur|                10 |
|          9 | West Ham United  |                 9 |
|         10 | Wigan Athletic   |                 8 |
|         11 | Everton          |                 8 |
|         12 | Portsmouth       |                 8 |
|         13 | Bolton Wanderers |                 7 |
|         14 | Aston Villa      |                 7 |
|         15 | West Bromwich Albion |                   7 |
|         16 | Sunderland       |                 6 |
|         17 | Blackburn Rovers |                 6 |
|         18 | Middlesbrough    |                 5 |
|         19 | Newcastle United |                 5 |
|         20 | Hull City        |                 3 |

#### Resultados temporada 2009-2010  

|   Posición | Equipo              | Victorias locales |
|------------|-------------------------|---------------------|
|          1 | Chelsea             |                17 |
|          2 | Manchester United   |                16 |
|          3 | Arsenal             |                15 |
|          4 | Tottenham Hotspur   |                14 |
|          5 | Liverpool           |                13 |
|          6 | Manchester City     |                12 |
|          7 | Fulham              |                11 |
|          8 | Everton             |                11 |
|          9 | Blackburn Rovers    |                10 |
|         10 | Sunderland          |                 9 |
|         11 | Aston Villa         |                 8 |
|         12 | Birmingham City     |                 8 |
|         13 | Burnley             |                 7 |
|         14 | Stoke City          |                 7 |
|         15 | West Ham United     |                 7 |
|         16 | Bolton Wanderers    |                 6 |
|         17 | Wigan Athletic      |                 6 |
|         18 | Hull City           |                 6 |
|         19 | Wolverhampton Wanderers |                   5 |
|         20 | Portsmouth          |                 5 |

#### Resultados temporada 2010-2011  

|   Posición | Equipo              | Victorias locales |
|------------|-------------------------|---------------------|
|          1 | Manchester United   |                18 |
|          2 | Chelsea             |                14 |
|          3 | Manchester City     |                13 |
|          4 | Liverpool           |                12 |
|          5 | Arsenal             |                11 |
|          6 | Bolton Wanderers    |                10 |
|          7 | Stoke City          |                10 |
|          8 | Tottenham Hotspur   |                 9 |
|          9 | Everton             |                 9 |
|         10 | Aston Villa         |                 8 |
|         11 | Wolverhampton Wanderers |                   8 |
|         12 | Fulham              |                 8 |
|         13 | West Bromwich Albion|                 8 |
|         14 | Blackburn Rovers    |                 7 |
|         15 | Sunderland          |                 7 |
|         16 | Newcastle United    |                 6 |
|         17 | Birmingham City     |                 6 |
|         18 | Wigan Athletic      |                 5 |
|         19 | Blackpool           |                 5 |
|         20 | West Ham United     |                 5 |

#### Resultados temporada 2011-2012  

|   Posición | Equipo              | Victorias locales |
|------------|-------------------------|---------------------|
|          1 | Manchester City     |                18 |
|          2 | Manchester United   |                15 |
|          3 | Tottenham Hotspur   |                13 |
|          4 | Chelsea             |                12 |
|          5 | Arsenal             |                12 |
|          6 | Newcastle United    |                11 |
|          7 | Fulham              |                10 |
|          8 | Everton             |                10 |
|          9 | Swansea City        |                 8 |
|         10 | Queens Park Rangers |                 7 |
|         11 | Stoke City          |                 7 |
|         12 | Sunderland          |                 7 |
|         13 | Norwich City        |                 7 |
|         14 | Blackburn Rovers    |                 6 |
|         15 | Liverpool           |                 6 |
|         16 | West Bromwich Albion|                 6 |
|         17 | Wigan Athletic      |                 5 |
|         18 | Aston Villa         |                 4 |
|         19 | Bolton Wanderers    |                 4 |
|         20 | Wolverhampton Wanderers |                   3 |

#### Resultados temporada 2012-2013  

|   Posición | Equipo           | Victorias locales |
|------------|----------------------|---------------------|
|          1 | Manchester United|                16 |
|          2 | Manchester City  |                14 |
|          3 | Chelsea          |                12 |
|          4 | Everton          |                12 |
|          5 | Arsenal          |                11 |
|          6 | Tottenham Hotspur|                11 |
|          7 | Newcastle United |                 9 |
|          8 | West Bromwich Albion |                   9 |
|          9 | West Ham United  |                 9 |
|         10 | Liverpool        |                 9 |
|         11 | Norwich City     |                 8 |
|         12 | Fulham           |                 7 |
|         13 | Stoke City       |                 7 |
|         14 | Southampton      |                 6 |
|         15 | Swansea City     |                 6 |
|         16 | Aston Villa      |                 5 |
|         17 | Sunderland       |                 5 |
|         18 | Reading          |                 4 |
|         19 | Wigan Athletic   |                 4 |
|         20 | Queens Park Rangers  |                   2 |

#### Resultados temporada 2013-2014  

|   Posición | Equipo           | Victorias locales |
|------------|----------------------|---------------------|
|          1 | Manchester City  |                17 |
|          2 | Liverpool        |                16 |
|          3 | Chelsea          |                15 |
|          4 | Arsenal          |                13 |
|          5 | Everton          |                13 |
|          6 | Tottenham Hotspur|                11 |
|          7 | Stoke City       |                10 |
|          8 | Manchester United|                 9 |
|          9 | Newcastle United |                 8 |
|         10 | Southampton      |                 8 |
|         11 | Crystal Palace   |                 8 |
|         12 | Hull City        |                 7 |
|         13 | West Ham United  |                 7 |
|         14 | Aston Villa      |                 6 |
|         15 | Norwich City     |                 6 |
|         16 | Swansea City     |                 6 |
|         17 | Fulham           |                 5 |
|         18 | Sunderland       |                 5 |
|         19 | Cardiff City     |                 5 |
|         20 | West Bromwich Albion |                   4 |

#### Resultados temporada 2014-2015  

|   Posición | Equipo           | Victorias locales |
|------------|----------------------|---------------------|
|          1 | Chelsea          |                15 |
|          2 | Manchester United|                14 |
|          3 | Manchester City  |                14 |
|          4 | Arsenal          |                12 |
|          5 | Southampton      |                11 |
|          6 | Stoke City       |                10 |
|          7 | Liverpool        |                10 |
|          8 | Tottenham Hotspur|                10 |
|          9 | West Ham United  |                 9 |
|         10 | Swansea City     |                 9 |
|         11 | Newcastle United |                 7 |
|         12 | Leicester City   |                 7 |
|         13 | West Bromwich Albion |                   7 |
|         14 | Everton          |                 7 |
|         15 | Queens Park Rangers  |                   6 |
|         16 | Crystal Palace   |                 6 |
|         17 | Aston Villa      |                 5 |
|         18 | Hull City        |                 5 |
|         19 | Burnley          |                 4 |
|         20 | Sunderland       |                 4 |

#### Resultados temporada 2015-2016  

|   Posición | Equipo           | Victorias locales |
|------------|----------------------|---------------------|
|          1 | Manchester United|                12 |
|          2 | Manchester City  |                12 |
|          3 | Leicester City   |                12 |
|          4 | Arsenal          |                12 |
|          5 | Southampton      |                11 |
|          6 | Tottenham Hotspur|                10 |
|          7 | West Ham United  |                 9 |
|          8 | Stoke City       |                 8 |
|          9 | Swansea City     |                 8 |
|         10 | Liverpool        |                 8 |
|         11 | Newcastle United |                 7 |
|         12 | Everton          |                 6 |
|         13 | Norwich City     |                 6 |
|         14 | West Bromwich Albion |                   6 |
|         15 | Sunderland       |                 6 |
|         16 | Watford          |                 6 |
|         17 | Crystal Palace   |                 6 |
|         18 | Chelsea          |                 5 |
|         19 | AFC Bournemouth  |                 5 |
|         20 | Aston Villa      |                 2 |

#### Resultados temporada 2016-2017  

|   Posición | Equipo           | Victorias locales |
|------------|----------------------|---------------------|
|          1 | Chelsea          |                17 |
|          2 | Tottenham Hotspur|                17 |
|          3 | Arsenal          |                14 |
|          4 | Everton          |                13 |
|          5 | Liverpool        |                12 |
|          6 | Manchester City  |                11 |
|          7 | Burnley          |                10 |
|          8 | Leicester City   |                10 |
|          9 | AFC Bournemouth  |                 9 |
|         10 | West Bromwich Albion |                   9 |
|         11 | Hull City        |                 8 |
|         12 | Manchester United|                 8 |
|         13 | Swansea City     |                 8 |
|         14 | Watford          |                 8 |
|         15 | Stoke City       |                 7 |
|         16 | West Ham United  |                 7 |
|         17 | Southampton      |                 6 |
|         18 | Crystal Palace   |                 6 |
|         19 | Middlesbrough    |                 4 |
|         20 | Sunderland       |                 3 |

#### Resultados temporada 2017-2018  

|   Posición | Equipo               | Victorias locales |
|------------|--------------------------|---------------------|
|          1 | Manchester City      |                16 |
|          2 | Manchester United    |                15 |
|          3 | Arsenal              |                15 |
|          4 | Tottenham Hotspur    |                13 |
|          5 | Liverpool            |                12 |
|          6 | Chelsea              |                11 |
|          7 | Everton              |                10 |
|          8 | Newcastle United     |                 8 |
|          9 | Burnley              |                 7 |
|         10 | Watford              |                 7 |
|         11 | Crystal Palace       |                 7 |
|         12 | Brighton and Hove Albion |                   7 |
|         13 | AFC Bournemouth      |                 7 |
|         14 | Leicester City       |                 7 |
|         15 | West Ham United      |                 7 |
|         16 | Huddersfield Town    |                 6 |
|         17 | Swansea City         |                 6 |
|         18 | Stoke City           |                 5 |
|         19 | Southampton          |                 4 |
|         20 | West Bromwich Albion |                 3 |

Además para los equipos de los que se tienen datos suficientes se ha encontrado que con más de un 95% de confianza estos equipos tienden a ganar más como locales que como visitantes.

Finalmente se ha comprobado que de manera global este resultado también se ve avalado.

## Conclusiones sobre Spark

Nótese que para los distintos apartados de la práctica hemos debido de extraer los siguientes datos:

- Victorias como local por equipo.
- El número de temporadas que juega cada equipo.
- Los equipos que juegan más de cierta cantidad de temporadas.
- Diferencias de victorias como locales y como visitantes de cierto equipo por temporadas.
- Diferencias de victorias como locales y como visitantes de todo los equipos por temporadas.

A pesar de ser python un lenguaje orientado a objetos la redacción de éstas tareas se han hecho con herramientas funcioneles como: `reduce`, `map`, `filter` y `groupByKey` y en muy pocas líneas, lo cual demuestra la expresividad de tan lenguaje.
