"""
servidor.py
PFO 3 - Servidor TCP concurrente con pool de workers.
"""

import queue
import random
import socket
import threading
import time

# Configuración de red y concurrencia
HOST = "127.0.0.1"
PUERTO = 5000
BUFFER = 4096            # Tamaño de lectura por recv()
MAX_LINEA = 64 * 1024    # Límite de bytes sin salto de línea (protección ante clientes defectuosos)
NUM_WORKERS = 4
PALABRAS_SALIDA = ("éxito", "exito")


class Tarea:
    """Unidad de trabajo que viaja por la cola: contenido + destino de la respuesta."""

    def __init__(self, contenido, conexion, candado_envio):
        self.contenido = contenido
        self.conexion = conexion
        self.candado_envio = candado_envio


class ServidorTCP:
    def __init__(self, host=HOST, puerto=PUERTO, num_workers=NUM_WORKERS):
        self.host = host
        self.puerto = puerto
        self.num_workers = num_workers
        self.cola_tareas = queue.Queue()      # Cola thread-safe: sincroniza productores y consumidores
        self.detener = threading.Event()      # Señal de apagado compartida por todos los hilos
        self.socket_servidor = None
        self.workers = []

    # ---------------------------------------------------------------- Red
    def iniciar(self):
        """Crea el socket de escucha, lanza el pool de workers y acepta clientes."""
        try:
            # Socket TCP (SOCK_STREAM): conexión confiable, ordenada y sin errores
            self.socket_servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket_servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.socket_servidor.bind((self.host, self.puerto))
            self.socket_servidor.listen()
            # Timeout para que accept() no bloquee indefinidamente y se pueda detectar el apagado
            self.socket_servidor.settimeout(1.0)
        except OSError as error:
            print(f"[ERROR] No se pudo iniciar el servidor en {self.host}:{self.puerto}: {error}")
            return

        self._iniciar_workers()
        print(f"[SERVIDOR] Escuchando en {self.host}:{self.puerto} con {self.num_workers} workers")

        try:
            self._bucle_aceptacion()
        except KeyboardInterrupt:
            print("\n[SERVIDOR] Interrupción recibida, cerrando...")
        finally:
            self.cerrar()

    def _bucle_aceptacion(self):
        """Hilo principal: solo acepta conexiones; nunca procesa tareas."""
        while not self.detener.is_set():
            try:
                conexion, direccion = self.socket_servidor.accept()
            except socket.timeout:
                continue
            except OSError as error:
                print(f"[ERROR] Fallo en accept(): {error}")
                break

            print(f"[SERVIDOR] Cliente conectado: {direccion[0]}:{direccion[1]}")
            # Un hilo por conexión: la espera en recv() es E/S y libera el GIL
            hilo = threading.Thread(
                target=self._atender_cliente,
                args=(conexion, direccion),
                daemon=True,
            )
            hilo.start()

    def _atender_cliente(self, conexion, direccion):
        """Productor: lee el socket en bloques de 4096 bytes y encola cada tarea."""
        # Un candado por conexión evita que dos workers intercalen bytes en un mismo socket
        candado_envio = threading.Lock()
        acumulado = b""

        try:
            while not self.detener.is_set():
                bloque = conexion.recv(BUFFER)
                if not bloque:
                    break  # El cliente cerró la conexión

                # TCP es un flujo de bytes: se acumula hasta completar mensajes terminados en '\n'
                acumulado += bloque
                if len(acumulado) > MAX_LINEA and b"\n" not in acumulado:
                    self._enviar(conexion, candado_envio, "ERROR: mensaje demasiado largo")
                    break

                while b"\n" in acumulado:
                    linea, acumulado = acumulado.split(b"\n", 1)
                    contenido = linea.decode("utf-8", errors="replace").strip()
                    if not contenido:
                        continue

                    if contenido.lower() in PALABRAS_SALIDA:
                        self._enviar(conexion, candado_envio, "Sesión finalizada")
                        return
                    # Se encola la tarea y se vuelve a leer el socket sin esperar al worker
                    self.cola_tareas.put(Tarea(contenido, conexion, candado_envio))
        except (ConnectionResetError, BrokenPipeError):
            print(f"[SERVIDOR] Conexión perdida con {direccion[0]}:{direccion[1]}")
        except OSError as error:
            print(f"[ERROR] Error de socket con {direccion[0]}:{direccion[1]}: {error}")
        finally:
            print(f"[SERVIDOR] Cliente desconectado: {direccion[0]}:{direccion[1]}")
            try:
                conexion.close()
            except OSError:
                pass

    @staticmethod
    def _enviar(conexion, candado_envio, mensaje):
        """Envía un mensaje delimitado por '\\n' de forma exclusiva por conexión."""
        try:
            with candado_envio:
                conexion.sendall((mensaje + "\n").encode("utf-8"))
        except OSError:
            pass  # El cliente ya se desconectó; se descarta el resultado

    # ------------------------------------------------------------ Workers
    def _iniciar_workers(self):
        """Crea el pool fijo de hilos consumidores."""
        for i in range(self.num_workers):
            hilo = threading.Thread(target=self._bucle_worker, name=f"Worker-{i + 1}", daemon=True)
            hilo.start()
            self.workers.append(hilo)

    def _bucle_worker(self):
        """Consumidor: extrae tareas de la cola, las procesa y responde al cliente."""
        while True:
            tarea = self.cola_tareas.get()  # Bloquea sin consumir CPU hasta que haya trabajo
            if tarea is None:               # Centinela de apagado
                self.cola_tareas.task_done()
                break
            try:
                resultado = self._procesar(tarea.contenido)
                self._enviar(tarea.conexion, tarea.candado_envio, resultado)
            except Exception as error:      # Un fallo en una tarea no debe matar al worker
                self._enviar(tarea.conexion, tarea.candado_envio, f"ERROR al procesar: {error}")
            finally:
                self.cola_tareas.task_done()

    @staticmethod
    def _procesar(contenido):
        """Simula una tarea de E/S (consulta a BD, llamada a API) con time.sleep.

        sleep() libera el GIL, por lo que los demás hilos siguen ejecutándose.
        Para trabajo intensivo en CPU se usaría multiprocessing.
        """
        nombre = threading.current_thread().name
        duracion = random.uniform(1, 3)
        time.sleep(duracion)
        return f"Tarea '{contenido}' completada por {nombre} en {duracion:.1f}s"

    # ------------------------------------------------------------- Cierre
    def cerrar(self):
        """Apagado ordenado: detiene el aceptador y despierta a los workers."""
        self.detener.set()
        for _ in self.workers:
            self.cola_tareas.put(None)  # Un centinela por worker
        if self.socket_servidor:
            try:
                self.socket_servidor.close()
            except OSError:
                pass
        print("[SERVIDOR] Servidor detenido")


if __name__ == "__main__":
    ServidorTCP().iniciar()
