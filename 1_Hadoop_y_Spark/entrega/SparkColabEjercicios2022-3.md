<a href="https://colab.research.google.com/github/BlancaCC/ProcesamientoDeDatosAGranEscala/blob/spark/SparkColabEjercicios2022.ipynb" target="_parent"><img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/></a>

# PySpark en Google Colab

Instalacion Octubre/2022

1.   Instalacion Java
2.   Instalacion de Spark
3.   Instalar PySpark

## De forma General para usar pyspark en Colab (2022) siga los siguientes pasos en una celda en Colab:


```python
# instalar Java
!apt-get install openjdk-8-jdk-headless -qq > /dev/null

```


```python
# Descargar la ultima versión de java ( comprobar que existen los path de descarga)
# Download latest release. Update if necessary

!wget -q https://downloads.apache.org/spark/spark-3.3.0/spark-3.3.0-bin-hadoop3.tgz
```


```python
%ls -la /content/


```

    total 876964
    drwxr-xr-x  1 root root      4096 Oct  9 20:40 [0m[01;34m.[0m/
    drwxr-xr-x  1 root root      4096 Oct  9 19:04 [01;34m..[0m/
    drwxr-xr-x  4 root root      4096 Oct  5 13:34 [01;34m.config[0m/
    drwx------  5 root root      4096 Oct  9 19:28 [01;34mgdrive[0m/
    drwxr-xr-x  2 root root      4096 Oct  9 20:38 [01;34msalida[0m/
    drwxr-xr-x  2 root root      4096 Oct  9 19:06 [01;34msalida2[0m/
    drwxr-xr-x  2 root root      4096 Oct  9 19:06 [01;34msalida3[0m/
    drwxr-xr-x  1 root root      4096 Oct  5 13:35 [01;34msample_data[0m/
    drwxr-xr-x 13 1000 1000      4096 Jun  9 20:37 [01;34mspark-3.3.0-bin-hadoop3[0m/
    -rw-r--r--  1 root root 299321244 Jun  9 20:39 spark-3.3.0-bin-hadoop3.tgz
    -rw-r--r--  1 root root 299321244 Jun  9 20:39 spark-3.3.0-bin-hadoop3.tgz.1
    -rw-r--r--  1 root root 299321244 Jun  9 20:39 spark-3.3.0-bin-hadoop3.tgz.2



```python
!tar xf spark-3.3.0-bin-hadoop3.tgz
```


```python
%ls -la
```

    total 876964
    drwxr-xr-x  1 root root      4096 Oct  9 20:40 [0m[01;34m.[0m/
    drwxr-xr-x  1 root root      4096 Oct  9 19:04 [01;34m..[0m/
    drwxr-xr-x  4 root root      4096 Oct  5 13:34 [01;34m.config[0m/
    drwx------  5 root root      4096 Oct  9 19:28 [01;34mgdrive[0m/
    drwxr-xr-x  2 root root      4096 Oct  9 20:38 [01;34msalida[0m/
    drwxr-xr-x  2 root root      4096 Oct  9 19:06 [01;34msalida2[0m/
    drwxr-xr-x  2 root root      4096 Oct  9 19:06 [01;34msalida3[0m/
    drwxr-xr-x  1 root root      4096 Oct  5 13:35 [01;34msample_data[0m/
    drwxr-xr-x 13 1000 1000      4096 Jun  9 20:37 [01;34mspark-3.3.0-bin-hadoop3[0m/
    -rw-r--r--  1 root root 299321244 Jun  9 20:39 spark-3.3.0-bin-hadoop3.tgz
    -rw-r--r--  1 root root 299321244 Jun  9 20:39 spark-3.3.0-bin-hadoop3.tgz.1
    -rw-r--r--  1 root root 299321244 Jun  9 20:39 spark-3.3.0-bin-hadoop3.tgz.2





```python
# instalar pyspark
!pip install -q pyspark
```

# Variables de entorno


```python
import os # libreria de manejo del sistema operativo
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.3.0-bin-hadoop3"
```

# Iniciar la sesión de Spark con pyspark en el sistema


```python
from pyspark.sql import SparkSession

APP_NAME = "PDGE-tutorialSpark1"
SPARK_URL = "local[*]"
spark = SparkSession.builder.appName(APP_NAME).master(SPARK_URL).getOrCreate()
spark
```





    <div>
        <p><b>SparkSession - in-memory</b></p>

<div>
    <p><b>SparkContext</b></p>

    <p><a href="http://d52558c4379b:4040">Spark UI</a></p>

    <dl>
      <dt>Version</dt>
        <dd><code>v3.3.0</code></dd>
      <dt>Master</dt>
        <dd><code>local[*]</code></dd>
      <dt>AppName</dt>
        <dd><code>PDGE-tutorialSpark1</code></dd>
    </dl>
</div>

    </div>





```python
sc = spark.sparkContext
#obtener el contexto de ejecución de Spark del Driver.
sc
```





<div>
    <p><b>SparkContext</b></p>

    <p><a href="http://d52558c4379b:4040">Spark UI</a></p>

    <dl>
      <dt>Version</dt>
        <dd><code>v3.3.0</code></dd>
      <dt>Master</dt>
        <dd><code>local[*]</code></dd>
      <dt>AppName</dt>
        <dd><code>PDGE-tutorialSpark1</code></dd>
    </dl>
</div>





```python
array = sc.parallelize([1,2,3,4,5,6,7,8,9,10], 2)
array
```




    ParallelCollectionRDD[363] at readRDDFromFile at PythonRDD.scala:274




```python
print(array.collect())
```

    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]



```python
print(array.count())

```

    10



```python
num3 = array.map(lambda elemento: 3*elemento)
```


```python
print(num3.collect())
```

    [3, 6, 9, 12, 15, 18, 21, 24, 27, 30]


# Ejemplos de operaciones con RDDs de Spark


```python
numeros = sc.parallelize([1,2,3,4,5,6,7,8,9,10],2)

print(numeros.reduce(lambda e1,e2: e1+e2))
```

    55



```python
pnumeros = sc.parallelize([1,2,3,4,5,6,7,8,9,10])

rdd = pnumeros.map(lambda e: 2*e)

print(rdd.collect())

```

    [2, 4, 6, 8, 10, 12, 14, 16, 18, 20]


## Pregunta TS1.1 ¿Cómo hacer para obtener una lista de los elementos al cuadrado?


```python
#¿Cómo hacer para obtener una lista de los elementos al cuadrado?
numeros = sc.parallelize([1,2,3,4,5,6,7,8,9,10])

#rdd = numeros...
rdd = numeros.map(lambda e: e**2)

print(rdd.collect())

```

    [1, 4, 9, 16, 25, 36, 49, 64, 81, 100]


## Pregunta TS1.2 ¿Cómo filtrar los impares?


```python
#¿Cómo filtrar los impares?
#rddi = numeros.
rddi = numeros.filter(lambda n: n%2 == 0) # Mostrará los números impares
print(rddi.collect())

```

    [2, 4, 6, 8, 10]


# Ejemplos de operaciones con números

Sumar n numeros


```python
rddnum=sc.range(1000000)
rddnum.reduce(lambda a,b: a+b)
```




    499999500000




```python
numeros = sc.parallelize([1,1,2,2,5])

unicos = numeros.distinct()

print (unicos.collect())
```

    [2, 1, 5]



```python
numeros = sc.parallelize([1,2,3,4,5])

rdd = numeros.flatMap(lambda elemento: [elemento, 10*elemento])

print (rdd.collect())
```

    [1, 10, 2, 20, 3, 30, 4, 40, 5, 50]


Muestreo


```python
numeros = sc.parallelize([1,2,3,4,5,6,7,8,9,10])

rdd = numeros.sample(True, 1.0)
# True = sin reemplazar
# 1.0 = fracción de elementos = 100%
print (rdd.collect())
```

    [2, 3, 5, 5, 8, 9, 10]


Union 


```python
pares = sc.parallelize([2,4,6,8,10])
impares = sc.parallelize([1,3,5,7,9])

numeros = pares.union(impares)

print (numeros.collect())
```

    [2, 4, 6, 8, 10, 1, 3, 5, 7, 9]


reducción



```python
numeros = sc.parallelize([1,2,3,4,5])

print (numeros.reduce(lambda elem1,elem2: elem2+elem1))
```

    15


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



```python
#Tiene sentido la reducción(elem1-elem2)? ¿qué pasa si cambiamos la paralelización a 5? ¿y si es 2?
numeros = sc.parallelize([1,2,3,4,5,6,7,8,9,10],5)

print (numeros.reduce(lambda elem1,elem2: elem1-elem2))
```

    3


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


# RDDs con string


```python
palabras = sc.parallelize(['HOLA', 'Que', 'TAL', 'Bien'])

pal_minus = palabras.map(lambda elemento: elemento.lower())

print (pal_minus.collect())
```

    ['hola', 'que', 'tal', 'bien']



```python
palabras = sc.parallelize(['HOLA', 'Que', 'TAL', 'Bien'])

pal_long = palabras.map(lambda elemento: len(elemento))

print (pal_long.collect())
```

    [4, 3, 3, 4]



```python
log = sc.parallelize(['E: e21', 'W: w12', 'W: w13', 'E: e45'])

errors = log.filter(lambda elemento: elemento[0]=='E')

print (errors.collect())
```

    ['E: e21', 'E: e45']



```python
lineas = sc.parallelize(['', 'a', 'a b', 'a b c'])

palabras = lineas.flatMap(lambda elemento: elemento.split())

print (palabras.collect())
```

    ['a', 'a', 'b', 'a', 'b', 'c']



```python
lineas = sc.parallelize(['', 'a', 'a b', 'a b c'])

palabras_flat = lineas.flatMap(lambda elemento: elemento.split())
palabras_map = lineas.map(lambda elemento: elemento.split())

print (palabras_flat.collect())
print (palabras_map.collect())

```

    ['a', 'a', 'b', 'a', 'b', 'c']
    [[], ['a'], ['a', 'b'], ['a', 'b', 'c']]


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


# Utilización de ficheros


```python

# Crea una lista de 1000 números y la guarda.
# Da error si 'salida' existe, descomenta la linea de borrar en ese caso.
%rm -rf salida
numeros = sc.parallelize(range(0,1000))
numeros.saveAsTextFile('salida')

```


```python
ls -la
```

    total 876964
    drwxr-xr-x  1 root root      4096 Oct  9 20:41 [0m[01;34m.[0m/
    drwxr-xr-x  1 root root      4096 Oct  9 19:04 [01;34m..[0m/
    drwxr-xr-x  4 root root      4096 Oct  5 13:34 [01;34m.config[0m/
    drwx------  5 root root      4096 Oct  9 19:28 [01;34mgdrive[0m/
    drwxr-xr-x  2 root root      4096 Oct  9 20:41 [01;34msalida[0m/
    drwxr-xr-x  2 root root      4096 Oct  9 19:06 [01;34msalida2[0m/
    drwxr-xr-x  2 root root      4096 Oct  9 19:06 [01;34msalida3[0m/
    drwxr-xr-x  1 root root      4096 Oct  5 13:35 [01;34msample_data[0m/
    drwxr-xr-x 13 1000 1000      4096 Jun  9 20:37 [01;34mspark-3.3.0-bin-hadoop3[0m/
    -rw-r--r--  1 root root 299321244 Jun  9 20:39 spark-3.3.0-bin-hadoop3.tgz
    -rw-r--r--  1 root root 299321244 Jun  9 20:39 spark-3.3.0-bin-hadoop3.tgz.1
    -rw-r--r--  1 root root 299321244 Jun  9 20:39 spark-3.3.0-bin-hadoop3.tgz.2



```python
%ls -la salida/*

```

    -rw-r--r-- 1 root root 1890 Oct  9 20:41 salida/part-00000
    -rw-r--r-- 1 root root 2000 Oct  9 20:41 salida/part-00001
    -rw-r--r-- 1 root root    0 Oct  9 20:41 salida/_SUCCESS



```python
#%cat salida/part-00000
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



    ---------------------------------------------------------------------------

    Py4JJavaError                             Traceback (most recent call last)

    <ipython-input-207-bb0984187062> in <module>
          6 # Borra la salida y cambia las particiones en parallelize ¿Qué sucede?
          7 sc.parallelize(range(0,1000),8)
    ----> 8 numeros.saveAsTextFile('salida2')
    

    /usr/local/lib/python3.7/dist-packages/pyspark/rdd.py in saveAsTextFile(self, path, compressionCodecClass)
       2203             keyed._jrdd.map(self.ctx._jvm.BytesToString()).saveAsTextFile(path, compressionCodec)
       2204         else:
    -> 2205             keyed._jrdd.map(self.ctx._jvm.BytesToString()).saveAsTextFile(path)
       2206 
       2207     # Pair functions


    /usr/local/lib/python3.7/dist-packages/py4j/java_gateway.py in __call__(self, *args)
       1320         answer = self.gateway_client.send_command(command)
       1321         return_value = get_return_value(
    -> 1322             answer, self.gateway_client, self.target_id, self.name)
       1323 
       1324         for temp_arg in temp_args:


    /usr/local/lib/python3.7/dist-packages/pyspark/sql/utils.py in deco(*a, **kw)
        188     def deco(*a: Any, **kw: Any) -> Any:
        189         try:
    --> 190             return f(*a, **kw)
        191         except Py4JJavaError as e:
        192             converted = convert_exception(e.java_exception)


    /usr/local/lib/python3.7/dist-packages/py4j/protocol.py in get_return_value(answer, gateway_client, target_id, name)
        326                 raise Py4JJavaError(
        327                     "An error occurred while calling {0}{1}{2}.\n".
    --> 328                     format(target_id, ".", name), value)
        329             else:
        330                 raise Py4JError(


    Py4JJavaError: An error occurred while calling o2892.saveAsTextFile.
    : org.apache.hadoop.mapred.FileAlreadyExistsException: Output directory file:/content/salida2 already exists
    	at org.apache.hadoop.mapred.FileOutputFormat.checkOutputSpecs(FileOutputFormat.java:131)
    	at org.apache.spark.internal.io.HadoopMapRedWriteConfigUtil.assertConf(SparkHadoopWriter.scala:299)
    	at org.apache.spark.internal.io.SparkHadoopWriter$.write(SparkHadoopWriter.scala:71)
    	at org.apache.spark.rdd.PairRDDFunctions.$anonfun$saveAsHadoopDataset$1(PairRDDFunctions.scala:1091)
    	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
    	at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:151)
    	at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:112)
    	at org.apache.spark.rdd.RDD.withScope(RDD.scala:406)
    	at org.apache.spark.rdd.PairRDDFunctions.saveAsHadoopDataset(PairRDDFunctions.scala:1089)
    	at org.apache.spark.rdd.PairRDDFunctions.$anonfun$saveAsHadoopFile$4(PairRDDFunctions.scala:1062)
    	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
    	at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:151)
    	at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:112)
    	at org.apache.spark.rdd.RDD.withScope(RDD.scala:406)
    	at org.apache.spark.rdd.PairRDDFunctions.saveAsHadoopFile(PairRDDFunctions.scala:1027)
    	at org.apache.spark.rdd.PairRDDFunctions.$anonfun$saveAsHadoopFile$3(PairRDDFunctions.scala:1009)
    	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
    	at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:151)
    	at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:112)
    	at org.apache.spark.rdd.RDD.withScope(RDD.scala:406)
    	at org.apache.spark.rdd.PairRDDFunctions.saveAsHadoopFile(PairRDDFunctions.scala:1008)
    	at org.apache.spark.rdd.PairRDDFunctions.$anonfun$saveAsHadoopFile$2(PairRDDFunctions.scala:965)
    	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
    	at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:151)
    	at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:112)
    	at org.apache.spark.rdd.RDD.withScope(RDD.scala:406)
    	at org.apache.spark.rdd.PairRDDFunctions.saveAsHadoopFile(PairRDDFunctions.scala:963)
    	at org.apache.spark.rdd.RDD.$anonfun$saveAsTextFile$2(RDD.scala:1599)
    	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
    	at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:151)
    	at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:112)
    	at org.apache.spark.rdd.RDD.withScope(RDD.scala:406)
    	at org.apache.spark.rdd.RDD.saveAsTextFile(RDD.scala:1599)
    	at org.apache.spark.rdd.RDD.$anonfun$saveAsTextFile$1(RDD.scala:1585)
    	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
    	at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:151)
    	at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:112)
    	at org.apache.spark.rdd.RDD.withScope(RDD.scala:406)
    	at org.apache.spark.rdd.RDD.saveAsTextFile(RDD.scala:1585)
    	at org.apache.spark.api.java.JavaRDDLike.saveAsTextFile(JavaRDDLike.scala:564)
    	at org.apache.spark.api.java.JavaRDDLike.saveAsTextFile$(JavaRDDLike.scala:563)
    	at org.apache.spark.api.java.AbstractJavaRDDLike.saveAsTextFile(JavaRDDLike.scala:45)
    	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
    	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
    	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
    	at java.lang.reflect.Method.invoke(Method.java:498)
    	at py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244)
    	at py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:357)
    	at py4j.Gateway.invoke(Gateway.java:282)
    	at py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)
    	at py4j.commands.CallCommand.execute(CallCommand.java:79)
    	at py4j.ClientServerConnection.waitForCommands(ClientServerConnection.java:182)
    	at py4j.ClientServerConnection.run(ClientServerConnection.java:106)
    	at java.lang.Thread.run(Thread.java:750)




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



```python
%rm -rf salida
%ls -la

```

    total 876960
    drwxr-xr-x  1 root root      4096 Oct  9 20:41 [0m[01;34m.[0m/
    drwxr-xr-x  1 root root      4096 Oct  9 19:04 [01;34m..[0m/
    drwxr-xr-x  4 root root      4096 Oct  5 13:34 [01;34m.config[0m/
    drwx------  5 root root      4096 Oct  9 19:28 [01;34mgdrive[0m/
    drwxr-xr-x  2 root root      4096 Oct  9 19:06 [01;34msalida2[0m/
    drwxr-xr-x  2 root root      4096 Oct  9 20:41 [01;34msalida3[0m/
    drwxr-xr-x  1 root root      4096 Oct  5 13:35 [01;34msample_data[0m/
    drwxr-xr-x 13 1000 1000      4096 Jun  9 20:37 [01;34mspark-3.3.0-bin-hadoop3[0m/
    -rw-r--r--  1 root root 299321244 Jun  9 20:39 spark-3.3.0-bin-hadoop3.tgz
    -rw-r--r--  1 root root 299321244 Jun  9 20:39 spark-3.3.0-bin-hadoop3.tgz.1
    -rw-r--r--  1 root root 299321244 Jun  9 20:39 spark-3.3.0-bin-hadoop3.tgz.2


# El quijote

Montar el directorio de trabajo utilizando Google Drive
Loading Your Data into Google Colaboratory.
1. First of all, Upload your Data to your Google Drive.
2. Run the following script in colab shell.
3. Copy the authorization code of your account.
4. Paste the authorization code into the output shell.
5. Congrats! Now your Google Drive is mounted to this location /content/gdrive/MyDrive/





```python
from google.colab import drive
#drive.flush_and_unmount()
drive.mount('/content/gdrive')
```

    Drive already mounted at /content/gdrive; to attempt to forcibly remount, call drive.mount("/content/gdrive", force_remount=True).



```python
%ls /content/gdrive/MyDrive/
```

    'CentOS 64-bit 2GB IMAGEN_DE_PARTIDA_HADOOP_CM.zip'
    [0m[01;34m'Colab Notebooks'[0m/
    'Documento sin título.gdoc'
    'Explicaciones - Semana1.xlsx'
     [01;34mMathlab[0m/
     [01;34mtfg[0m/



```python
#!cp "/content/gdrive/My Drive/quijote.txt" .
# Nota: He tenido que añadir la ruta esta para concreta para que lo lea en mi drive
!cp "./gdrive/MyDrive/Colab\ Notebooks/quijote.txt" .
```

    cp: cannot stat './gdrive/MyDrive/Colab\ Notebooks/quijote.txt': No such file or directory



```python
%ls -la
```

    total 876960
    drwxr-xr-x  1 root root      4096 Oct  9 20:41 [0m[01;34m.[0m/
    drwxr-xr-x  1 root root      4096 Oct  9 19:04 [01;34m..[0m/
    drwxr-xr-x  4 root root      4096 Oct  5 13:34 [01;34m.config[0m/
    drwx------  5 root root      4096 Oct  9 19:28 [01;34mgdrive[0m/
    drwxr-xr-x  2 root root      4096 Oct  9 19:06 [01;34msalida2[0m/
    drwxr-xr-x  2 root root      4096 Oct  9 20:41 [01;34msalida3[0m/
    drwxr-xr-x  1 root root      4096 Oct  5 13:35 [01;34msample_data[0m/
    drwxr-xr-x 13 1000 1000      4096 Jun  9 20:37 [01;34mspark-3.3.0-bin-hadoop3[0m/
    -rw-r--r--  1 root root 299321244 Jun  9 20:39 spark-3.3.0-bin-hadoop3.tgz
    -rw-r--r--  1 root root 299321244 Jun  9 20:39 spark-3.3.0-bin-hadoop3.tgz.1
    -rw-r--r--  1 root root 299321244 Jun  9 20:39 spark-3.3.0-bin-hadoop3.tgz.2



```python
%pwd


```




    '/content'



# Procesando el QUIJOTE


```python
# Nota: He tenido que añadir la ruta esta para concreta para que lo lea en mi drive
quijote = sc.textFile("gdrive/MyDrive/Colab\ Notebooks/quijote.txt")
quijote.take(10)
```




    ['EL INGENIOSO HIDALGO DON QUIJOTE DE LA MANCHA',
     '',
     'Miguel de Cervantes Saavedra',
     '',
     '     Capítulo primero',
     '',
     '     Que trata de la condición y ejercicio del famoso hidalgo D.',
     'Quijote de la ',
     '     Mancha',
     '']



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
$N_1 + N_2 = M$


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



```python
numLines = quijote.count()
numChars = charsPerLine.reduce(lambda a,b: a+b) # also charsPerLine.sum()
sortedWordsByLength = allWordsNoArticles.takeOrdered(20, key=lambda x: -len(x))
numLines, numChars, sortedWordsByLength
```




    (5534,
     305678,
     ['procuremos.Levántate,',
      'estrechísimamente,',
      'Pintiquiniestra,',
      'entretenimiento,',
      'maravillosamente',
      'descansadamente;',
      'desenfadadamente',
      'quebrantamientos',
      'quebrantamiento,',
      'alternativamente',
      'encantamientos,',
      'Placerdemivida,',
      'encantamientos;',
      'malbaratándolas',
      'regocijadamente',
      'consentimiento,',
      'desaconsejaban,',
      'acontecimiento.',
      'agradeciéndoles',
      'encantamientos,'])



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


# Operaciones K-V (Clave Valor)


```python
import requests
import re
allWords = allWords.flatMap(lambda w: re.sub(""";|:|\.|,|-|–|"|'|\s"""," ", w.lower()).split(" ")).filter(lambda a: len(a)>0)
allWords2 = sc.parallelize(requests.get("https://gist.githubusercontent.com/jsdario/9d871ed773c81bf217f57d1db2d2503f/raw/585de69b0631c805dabc6280506717943b82ba4a/el_quijote_ii.txt").iter_lines())
allWords2 = allWords2.flatMap(lambda w: re.sub(""";|:|\.|,|-|–|"|'|\s"""," ", w.decode("utf8").lower()).split(" ")).filter(lambda a: len(a)>0)
```


```python
allWords.take(10)
```




    ['el',
     'ingenioso',
     'hidalgo',
     'don',
     'quijote',
     'de',
     'la',
     'mancha',
     'miguel',
     'de']




```python
allWords2.take(10)
```




    ['don',
     'quijote',
     'de',
     'la',
     'mancha',
     'miguel',
     'de',
     'cervantes',
     'saavedra',
     'segunda']




```python
words = allWords.map(lambda e: (e,1))
words2 = allWords2.map(lambda e: (e,1))

words.take(10)
```




    [('el', 1),
     ('ingenioso', 1),
     ('hidalgo', 1),
     ('don', 1),
     ('quijote', 1),
     ('de', 1),
     ('la', 1),
     ('mancha', 1),
     ('miguel', 1),
     ('de', 1)]




```python
frequencies = words.reduceByKey(lambda a,b: a+b)
frequencies2 = words2.reduceByKey(lambda a,b: a+b)
frequencies.takeOrdered(10, key=lambda a: -a[1])
```




    [('que', 3032),
     ('de', 2809),
     ('y', 2573),
     ('a', 1426),
     ('la', 1423),
     ('el', 1232),
     ('en', 1155),
     ('no', 903),
     ('se', 753),
     ('los', 696)]




```python
res = words.groupByKey().takeOrdered(10, key=lambda a: -len(a))
res # To see the content, res[i][1].data
#res[i][1].data
```




    [('el', <pyspark.resultiterable.ResultIterable at 0x7f2083796890>),
     ('hidalgo', <pyspark.resultiterable.ResultIterable at 0x7f2083796f10>),
     ('don', <pyspark.resultiterable.ResultIterable at 0x7f2083796c10>),
     ('mancha', <pyspark.resultiterable.ResultIterable at 0x7f2083796450>),
     ('saavedra', <pyspark.resultiterable.ResultIterable at 0x7f20837967d0>),
     ('que', <pyspark.resultiterable.ResultIterable at 0x7f2083796b10>),
     ('condición', <pyspark.resultiterable.ResultIterable at 0x7f2083796650>),
     ('y', <pyspark.resultiterable.ResultIterable at 0x7f2083796f50>),
     ('del', <pyspark.resultiterable.ResultIterable at 0x7f2083796790>),
     ('d', <pyspark.resultiterable.ResultIterable at 0x7f2083796110>)]




```python
joinFreq = frequencies.join(frequencies2)
joinFreq.take(10)
```




    [('el', (1232, 4394)),
     ('hidalgo', (14, 42)),
     ('don', (370, 1606)),
     ('mancha', (26, 101)),
     ('saavedra', (1, 1)),
     ('que', (3032, 10040)),
     ('y', (2573, 9650)),
     ('del', (415, 1344)),
     ('en', (1155, 4223)),
     ('cuyo', (11, 35))]




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




```python
from pyspark import StorageLevel
# https://spark.apache.org/docs/2.2.0/rdd-programming-guide.html#rdd-persistence
allWords2.persist(StorageLevel.MEMORY_AND_DISK) # Now it will be preserved on disk also
```




    PythonRDD[637] at RDD at PythonRDD.scala:53




```python
!rm -rf palabras_parte2
allWords2.saveAsTextFile("palabras_parte2")
```

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
