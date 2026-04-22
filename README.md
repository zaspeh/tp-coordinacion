# Trabajo Práctico - Coordinación

En este trabajo se busca familiarizar a los estudiantes con los desafíos de la coordinación del trabajo y el control de la complejidad en sistemas distribuidos. Para tal fin se provee un esqueleto de un sistema de control de stock de una verdulería y un conjunto de escenarios de creciente grado de complejidad y distribución que demandarán mayor sofisticación en la comunicación de las partes involucradas.

## Ejecución

`make up` : Inicia los contenedores del sistema y comienza a seguir los logs de todos ellos en un solo flujo de salida.

`make down`:   Detiene los contenedores y libera los recursos asociados.

`make logs`: Sigue los logs de todos los contenedores en un solo flujo de salida.

`make test`: Inicia los contenedores del sistema, espera a que los clientes finalicen, compara los resultados con una ejecución serial y detiene los contenederes.

`make switch`: Permite alternar rápidamente entre los archivos de docker compose de los distintos escenarios provistos.

## Elementos del sistema objetivo

![ ](./imgs/diagrama_de_robustez.jpg  "Diagrama de Robustez")
*Fig. 1: Diagrama de Robustez*

### Client

Lee un archivo de entrada y envía por TCP/IP pares (fruta, cantidad) al sistema.
Cuando finaliza el envío de datos, aguarda un top de pares (fruta, cantidad) y vuelca el resultado en un archivo de salida csv.
El criterio y tamaño del top dependen de la configuración del sistema. Por defecto se trata de un top 3 de frutas de acuerdo a la cantidad total almacenada.

### Gateway

Es el punto de entrada y salida del sistema. Intercambia mensajes con los clientes y las colas internas utilizando distintos protocolos.

### Sum
 
Recibe pares  (fruta, cantidad) y aplica la función Suma de la clase `FruitItem`. Por defecto esa suma es la canónica para los números enteros, ej:

`("manzana", 5) + ("manzana", 8) = ("manzana", 13)`

Pero su implementación podría modificarse.
Cuando se detecta el final de la ingesta de datos envía los pares (fruta, cantidad) totales a los Aggregators.

### Aggregator

Consolida los datos de las distintas instancias de Sum.
Cuando se detecta el final de la ingesta, se calcula un top parcial y se envía esa información al Joiner.

### Joiner

Recibe tops parciales de las instancias del Aggregator.
Cuando se detecta el final de la ingesta, se envía el top final hacia el gateway para ser entregado al cliente.

## Limitaciones del esqueleto provisto

La implementación base respeta la división de responsabilidades de los distintos controles y hace uso de la clase `FruitItem` como un elemento opaco, sin asumir la implementación de las funciones de Suma y Comparación.

No obstante, esta implementación no cubre los objetivos buscados tal y como es presentada. Entre sus falencias puede destactarse que:

 - No se implementa la interfaz del middleware. 
 - No se dividen los flujos de datos de los clientes más allá del Gateway, por lo que no se es capaz de resolver múltiples consultas concurrentemente.
 - No se implementan mecanismos de sincronización que permitan escalar los controles Sum y Aggregator. En particular:
   - Las instancias de Sum se dividen el trabajo, pero solo una de ellas recibe la notificación de finalización en la ingesta de datos.
   - Las instancias de Sum realizan _broadcast_ a todas las instancias de Aggregator, en lugar de agrupar los datos por algún criterio y evitar procesamiento redundante.
  - No se maneja la señal SIGTERM, con la salvedad de los clientes y el Gateway.

## Condiciones de Entrega

El código de este repositorio se agrupa en dos carpetas, una para Python y otra para Golang. Los estudiantes deberán elegir **sólo uno** de estos lenguajes y realizar una implementación que funcione correctamente ante cambios en la multiplicidad de los controles (archivo de docker compose), los archivos de entrada y las implementaciones de las funciones de Suma y Comparación del `FruitItem`.

![ ](./imgs/mutabilidad.jpg  "Mutabilidad de Elementos")
*Fig. 2: Elementos mutables e inmutables*

A modo de referencia, en la *Figura 2* se marcan en tonos oscuros los elementos que los estudiantes no deben alterar y en tonos claros aquellos sobre los que tienen libertad de decisión.
Al momento de la evaluación y ejecución de las pruebas se **descartarán** o **reemplazarán** :

- Los archivos de entrada de la carpeta `datasets`.
- El archivo docker compose principal y los de la carpeta `scenarios`.
- Todos los archivos Dockerfile.
- Todo el código del cliente.
- Todo el código del gateway, salvo `message_handler`.
- La implementación del protocolo de comunicación externo y `FruitItem`.

Redactar un breve informe explicando el modo en que se coordinan las instancias de Sum y Aggregator, así como el modo en el que el sistema escala respecto a los clientes y a la cantidad de controles.


## Informe

### Coordinación de instancias Sum y Aggregator

Para coordinar múltiples instancias de Sum y Aggregator se utiliza un conteo de mensajes procesados.
Cada Sum va a recibir mensajes de:
* Una cola que representa el flujo de datos.
* Un exchange que representa el flujo de control.
  
Cada cliente genera un identificador único (queryID) al momento de conectarse (esto se puede ver en message_handler). Todos los mensajes que envía llevan ese identificador, lo que permite que el sistema procese múltiples clientes concurrentemente sin mezclar datos.

Las instancias de Sum consumen el flujo de datos de una cola compartida, lo que garantiza que el trabajo se distribuya casi equitativamente entre ellas (RabbitMQ utiliza una especie de Round-Robin), cada instancia acumula los pares (fruta, cantidad) que recibe, aplicando la función de Sum(...) del FruitItem. 
cuando una instancia de Sum recibe el EOF del cliente a través del flujo de datos, propaga esa señal a las demás instancias de Sum a través de un exchange de control a modo de broadcast, de modo que todas sepan que deben enviar sus datos acumulados. A su vez envía a todos los Aggregators (con los cuales hay un exchange por Aggregator) la cantidad de mensajes totales que envío un cliente (total_count). Esto permite al Aggregator conocer la cantidad de mensajes de deberían haber procesado las instancias de Sum en total.

Al momento de hacer flush, cada Sum envía sus frutas al Aggregator que corresponde según un hash del nombre de la fruta (esto permite dividir el trabajo para las distintas instancias de Aggregators) y reporta la cantidad de mensajes que procesó (partial_count) a todos los Aggregators.
Para garantizar consistencia, cada instancia de Sum envía primero los datos y luego el correspondiente partial_count. Esto asegura que el Aggregator no considere completa una query antes de haber recibido todos los datos asociados.

El Aggregator que recibe los datos de una query espera hasta que la suma acumulada de todos los partial_count recibidos iguale el total_count informado por el gateway en el EOF. Este mecanismo garantiza que el Aggregator solo calcula el top parcial cuando tiene la certeza de haber recibido todos los datos de esa query. Una vez verificado, envía el top parcial al Joiner.
El Joiner consolida los tops parciales de todas las instancias de Aggregator. Cuando recibe un EOF de cada una de ellas, calcula el top final (utilizando la función Less(...) de FruitItem) y lo envía al gateway para ser entregado al cliente.

### Manejo de late data

Si un mensaje de datos llega a un Sum después de que este ya hizo flush de una query (esto podría pasar ya que cada instancia de Sum utiliza un 'Select' para escuchar la cola de datos y el exchange de control, es posible que mensajes de datos se procesen luego de haberse procesado el EOF, generando late data), se trata como late data: el dato se envía directamente al Aggregator correspondiente y se reporta un partial_count de 1 a todos los Aggregators. Los Aggregators suman ese 1 al acumulado, lo que eventualmente va a permitir que se llegue a la condición de haber procesado total_count mensajes.

### Manejo de memoria

Luego de realizar el flush de una query, cada instancia de Sum elimina los datos asociados a la misma de sus estructuras internas.
De igual forma, las instancias de Aggregator liberan la información una vez que envían su resultado parcial al Joiner y así mismo lo hace Joiner una vez enviado el top final.

Este enfoque evita la acumulación indefinida de estado en memoria, por ende, el consumo de memoria queda acotado a las queries activas.

### Escalabilidad

El sistema escala horizontalmente en todas sus dimensiones:
* La cantidad de clientes concurrentes está desacoplada mediante el uso de queryID, permitiendo procesar múltiples flujos de datos en paralelo sin interferencias.
* Las instancias de Sum escalan consumiendo de una cola compartida, lo que permite distribuir automáticamente la carga de trabajo entre ellas. A medida que se agregan más instancias, disminuye la cantidad de mensajes procesados por cada una sin necesidad de modificar la lógica del sistema.
* Las instancias de Aggregator escalan mediante particionamiento por hash del nombre de la fruta, garantizando que cada clave sea procesada por una única instancia, esto permite distribuir el cómputo sin redundancia. A su vez no necesitan conocer la cantidad de instancias de Sum, ya que simplemente acumulan los partial_count recibidos y los comparan contra el total_count.
* El Joiner recibe un EOF por cada instancia de Aggregator, por lo que también se adapta automáticamente a cambios en esa cantidad.
