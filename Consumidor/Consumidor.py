"""
______________________________________________
Módulo: Consumidor.py
Descripción: El Consumidor es un proceso autónomo que se encarga de ejecutar simulaciones Montecarlo a partir de los escenarios enviados 
por el productor. Principalmente se encarga de recibir una configuración y luego procesar escenarios usando mensajes intercambiados con 
RabbitMQ que es un sistema de mensajería basado en colas. El consumidor procesa dos tipos de mensajes, despues ejecuta la formula con las 
variables recibidas y finalmente se publica el resultado en otra cola.
    1.Configuración inicial con formula matematica. 
    2. escenarios con valores variables. 
"""
import pika
import json


class Consumidor:
    """
    Clase que implementa un consumidor de mensajes con RabbitMQ para procesar escenarios de simulación.

    Cuenta con dos funcionalidades, son las siguientes:
    1. Recibe una configuración que incluye una fórmula y constantes.
    2. Procesa escenarios usando dicha fórmula y publica los resultados en otra cola.

    Atributos:
        _conexion (pika.BlockingConnection): Establece conexión al servidor RabbitMQ.
        _canal (pika.channel.Channel): Canal de comunicación con RabbitMQ.
        _nom_exchange (str): Recibe la configuración  utilizando un nombre del exchange.
        _nom_queue_escenarios (str): Nombre de la cola desde donde se reciben los escenarios.
        _nom_queue_resultados (str): Nombre de la cola donde se publican los resultados.
        _formula (str): Fórmula matemática que se evalua.
        _constantes (dict): Diccionario con las constantes necesarias para la evaluación.
    """
    def _init_(self, ip: str, nom_exchange: str, nom_queue_escenarios: str, nom_queue_resultados: str):
        """
        Inicializa la instancia del consumidor y establece la conexión con RabbitMQ.
        
            ip (str): Dirección IP del servidor RabbitMQ.
            nom_exchange (str): Nombre del exchange donde se recibe la configuración.
            nom_queue_escenarios (str): Nombre de la cola que entrega los escenarios a procesar.
            nom_queue_resultados (str): Nombre de la cola donde se publican los resultados.
        """
        self.conexion = pika.BlockingConnection(pika.ConnectionParameters(host=ip))
        self.canal = self.conexion.channel()
        self.nom_exchange = nom_exchange
        self.nom_queue_escenarios = nom_queue_escenarios
        self.nom_queue_resultados = nom_queue_resultados
        self.formula = None
        self.constantes = {}

    def callback_configuracion(self, ch, method, properties, body):
        """
        El metodo callback maneja la recepción de mensajes de configuración.
        Tambien, extrae la fórmula y las constantes del mensaje.
        
            ch: Canal que recibe el mensaje.
            method: Información del método de entrega.
            properties: Propiedades del mensaje.
            body (bytes): Contenido del mensaje en formato JSON.
        """
        configuracion = json.loads(body.decode("utf-8"))
        self.formula = configuracion.get("formula")
        self.constantes = {
            nombre: valor for nombre, valor in configuracion.items()
            if nombre != "formula"
        }
        print(f"[CONSUMIDOR] Configuración recibida: formula = {self.formula}, constantes = {self.constantes}")

        ch.stop_consuming()

    def callback_escenario(self, ch, method, properties, body):
        """
        Maneja la recepción de un escenario, evalúa la fórmula y publica el resultado.

        Args:
            ch: Canal que recibe el mensaje.
            method: Información del método de entrega.
            properties: Propiedades del mensaje.
            body (bytes): Contenido del mensaje en formato JSON.
        """
        escenario = json.loads(body.decode("utf-8"))

        simulacion = {}
        simulacion.update(self.constantes)
        simulacion.update(escenario)

        try:
            resultado = eval(
                self.formula,
                {"_builtins_": None},
                simulacion
            )
        except Exception as e:
            print(f"[CONSUMIDOR - ERROR]: error al evaluar fórmula: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        mensaje = json.dumps({"resultado": resultado})
        self.canal.basic_publish(
            exchange="",
            routing_key=self.nom_queue_resultados,
            body=mensaje,
            properties=pika.BasicProperties(delivery_mode=2)
        )

        ch.basic_ack(delivery_tag=method.delivery_tag)
        print(f"[CONSUMIDOR] Procesado escenario → resultado={resultado}")

    def configurar_conexion(self):
         """
        Declara el exchange y las colas necesarias, y configura el control de flujo de mensajes.
        """
        self.canal.exchange_declare(exchange=self.nom_exchange, exchange_type="fanout")
        self.canal.queue_declare(queue=self.nom_queue_escenarios, durable=True)
        self.canal.queue_declare(queue=self.nom_queue_resultados)
        self.canal.basic_qos(prefetch_count=1)

    def recibir_configuracion(self):
        """
        Escucha temporalmente el exchange para recibir un único mensaje de configuración.
        Tambien, lanza una excepción si no se recibe la fórmula o las constantes.
        """

        cola = self.canal.queue_declare(queue="", exclusive=True)
        cola_configuracion = cola.method.queue

        self.canal.queue_bind(exchange=self.nom_exchange, queue=cola_configuracion)

        print("[CONSUMIDOR] Esperando configuración...")
        self.canal.basic_consume(
            queue=cola_configuracion,
            on_message_callback=self.callback_configuracion,
            auto_ack=True
        )

        self.canal.start_consuming()

        if not self.formula or not self.constantes:
            raise RuntimeError("No se recibió fórmula o constantes en la configuración.")

    def procesar_escenarios(self):
        """
        Comienza a consumir escenarios desde la cola correspondiente y los procesa.
        """
        print("[CONSUMIDOR]: Esperando escenarios...")

        self.canal.basic_qos(prefetch_count=1)
        self.canal.basic_consume(
            queue=self.nom_queue_escenarios,
            on_message_callback=self.callback_escenario
        )
        self.canal.start_consuming()

    def iniciar_consumidor(self):
        """
        Método principal que inicia todo el flujo del consumidor:
        - Configura conexión y colas.
        - Purga la cola de escenarios.
        - Espera y procesa la configuración.
        - Comienza el procesamiento de escenarios.
        """
        self.configurar_conexion()
        self.canal.queue_purge(queue=self.nom_queue_escenarios)
        self.recibir_configuracion()
        self.procesar_escenarios()
