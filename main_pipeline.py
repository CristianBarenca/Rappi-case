# Librería estándar de Python para programación asíncrona.
import asyncio
# Importamos la función productora: se encarga de generar alertas y colocarlas en la cola
from modulo2_motor_alertas.motor_alertas import alerts_producer
# Importamos la función consumidora: toma las alertas de la cola y las envía a Telegram
from modulo3_agente_telegram.alerts_notify import alerts_consumer

async def main():
    """
    Función principal del sistema asíncrono.

    Orquesta la ejecución concurrente de:
    - Un productor de alertas (alerts_producer)
    - Un consumidor de alertas (alerts_consumer)

    Ambos se comunican mediante una cola asíncrona compartida.
    """

    # Crea una cola asíncrona para comunicación entre tareas
    # Esta cola actúa como buffer entre el productor y el consumidor
    queue = asyncio.Queue()

    # Crea y programa la tarea del productor
    # El productor genera alertas y las inserta en la cola
    producer_task = asyncio.create_task(alerts_producer(queue))

    # Crea y programa la tarea del consumidor
    # El consumidor espera alertas en la cola y las procesa
    consumer_task = asyncio.create_task(alerts_consumer(queue))

    # Ejecutar ambas tareas de forma concurrente
    # asyncio.gather mantiene la ejecución hasta que ambas tareas finalicen
    await asyncio.gather(producer_task, consumer_task)


# Punto de entrada del script
if __name__=="__main__":
    # Inicializa el event loop de asyncio y ejecuta la función principal
    # Maneja automáticamente la creación y cierre del loop
    asyncio.run(main())