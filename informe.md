# Informe - TP Coordinación

## Arquitectura general

El Gateway es el punto de entrada y salida. Recibe pares (fruta, cantidad) por TCP desde los clientes, los inyecta en una cola compartida (`input_queue`) y espera el resultado final en otra cola (`results_queue`). Los Sum acumulan las cantidades por fruta y al terminar envían sus sumas parciales a los Aggregation. Los Aggregation consolidan los datos de todos los Sums, calculan un top parcial y lo envían al Join. El Join recibe los tops parciales de cada Aggregation, los mergea y envía el top final al Gateway para que se lo devuelva al cliente.

## Protocolo de mensajes internos

Todos los mensajes internos se serializan como arrays JSON. El tipo de mensaje se distingue por la longitud del array:

- `[client_id, fruta, cantidad]` (largo 3): mensaje de datos.
- `[client_id]` (largo 1): EOF, indica que un cliente terminó de enviar datos.
- `[client_id, [[fruta, cantidad], ...]]` (largo 2): resultado (top parcial o final).

Este esquema permite diferenciar datos, EOFs y resultados sin necesidad de un campo de tipo explícito.

## Identificación de clientes

Para soportar múltiples clientes concurrentes, el `MessageHandler` del Gateway asigna un `client_id` incremental a cada conexión nueva. Todos los mensajes que salen del Gateway llevan este `client_id` como primer campo. Esto permite mantener estados separados por cliente (diccionarios indexados por `client_id`) y que el Gateway filtre las respuestas para entregarle a cada cliente solo su resultado.

## Coordinación de los Sum

### Acumulación local y particionamiento

Cada instancia de Sum consume mensajes de la cola compartida `input_queue`. RabbitMQ distribuye los mensajes en round-robin entre los Sums que estén consumiendo. Cada Sum acumula localmente las cantidades por fruta para cada cliente en un diccionario (`amount_by_fruit`). Cuando llega la señal de flush, envía sus sumas parciales a los Aggregation.

El envío a los Aggregation es deterministico y no es un broadcast: cada fruta se envía a un único Aggregation determinado por un hash determinístico (`hashlib.md5`). Esto garantiza que todas las apariciones de una misma fruta (provenientes de distintos Sums) terminen en el mismo Aggregation, que es quien las consolida, no hacerlo de esta forma haría que tengamos que agregar una instancia mas donde se sumen todas las frutas del mismo tipo y quitaría la utilidad de la etapa de aggregation.

### Propagación del EOF entre Sums

El problema principal de la coordinación entre Sums es que la cola `input_queue` es compartida: cuando el Gateway envía un EOF, solo un Sum lo recibe (round-robin). Los demás Sums nunca se enteran de que deben flushear sus datos.

La solución usada es comunicar los Sum workers entre si con un exchange de broadcast y usar un segundo thread en cada Sum para esperar el mensaje de ese broadcast:

1. El Sum que recibe el EOF de la cola lo publica en un exchange (`{SUM_PREFIX}_eof`) con una routing key común (`"eof"`). Todos los Sums tienen una cola exclusiva bindeada a esta misma key, así que el mensaje llega a todos (funciona como fanout usando un exchange direct).

2. Cada Sum levanta un thread secundario (el "control thread") que escucha este exchange. Cuando recibe un broadcast de EOF, adquiere un lock, extrae los datos acumulados del cliente correspondiente, y los envía a los Aggregation usando sus propias conexiones.

3. Un `threading.Lock` protege el diccionario `amount_by_fruit`, que es el único estado compartido entre el main thread (que acumula datos) y el control thread (que flushea).

4. El control thread crea sus propias conexiones a RabbitMQ, tanto para el exchange de EOF como para los exchanges de los Aggregation.

Tras el flush, cada Sum también envía un EOF a cada Aggregation. Así, con N Sums, cada Aggregation recibe exactamente N EOFs y sabe cuándo tiene todos los datos.

Estas decisiones las considero validas porque por definicion de la cátedra (en las consultas por campus virtual) dijeron que podemos asumir que ningun nodo caería entonces asumo que la cantidad de Sum y Aggregators se mantiene conocida para todos los involucrados. Si no fuera así entonces habría que coordinar una forma de que se descubran que servicios estan activos y cuales no. En cuanto a la decision de usar 2 threads por cada Sum, seguimos atados (en esta version de python) al Global Interpreter Lock. Esto no es un problema porque el segundo thread (que escucha el broadcast del eof) es i/o bound. La parte "dura" del computo se hace en workers separados, en procesos separadas, asi que el GIL no es un problema.

## Coordinación de los Aggregation

Cada instancia de Aggregation tiene su propia cola exclusiva, bindeada a un exchange direct con una routing key que incluye su ID (`{AGGREGATION_PREFIX}_{ID}`). Esto permite que los Sums envíen datos a un Aggregation específico según el hash de la fruta.

El Aggregation mantiene una lista ordenada de `FruitItem` por cliente (usando `bisect.insort`). Cuando recibe datos de una fruta que ya existe, la elimina de la lista y la reinserta con el valor acumulado para mantener el orden. Esto es necesario porque con múltiples Sums, una misma fruta puede llegar varias veces al mismo Aggregation.

El Aggregation cuenta los EOFs recibidos por cliente. Cuando el contador llega a `SUM_AMOUNT`, sabe que todos los Sums ya flushearon. En ese momento toma los últimos `TOP_SIZE` elementos de la lista ordenada (los de mayor cantidad) y envía este top parcial a la cola del Join.

## Join

El Join es el componente más simple. Recibe tops parciales de cada Aggregation por una cola compartida (`join_queue`). Cuenta cuántos tops llegaron por cliente y cuando tiene `AGGREGATION_AMOUNT`, mergea todos los items, los ordena de mayor a menor y toma los primeros `TOP_SIZE`. El resultado final se envía a `results_queue` para que el Gateway lo entregue al cliente.

Como cada Aggregation maneja un subconjunto disjunto de frutas (determinado por el hash), el merge en el Join no necesita consolidar duplicados, solo ordenar y truncar.

## Manejo de SIGTERM

Al recibir SIGTERM, cada instancia detiene su consumo (`stop_consuming`), cierra sus conexiones y termina normalmente. No se notifica a otros nodos: cada componente es autónomo en su shutdown.
