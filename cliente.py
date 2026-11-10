"""
cliente.py
PFO 3 - Cliente TCP que envía tareas al servidor distribuido y muestra los resultados.
"""

import socket
import time

# Configuración de red
HOST = "127.0.0.1"
PUERTO = 5000
BUFFER = 4096
MAX_REINTENTOS = 5
ESPERA_REINTENTO = 2      # Segundos entre intentos de conexión
PALABRAS_SALIDA = ("éxito", "exito")


class ClienteTCP:
    def __init__(self, host=HOST, puerto=PUERTO):
        self.host = host
        self.puerto = puerto
        self.socket_cliente = None
        self.pendiente = b""   # Bytes recibidos que aún no forman una línea completa

    def conectar(self):
        """Conecta al servidor con reintentos temporizados ante fallos temporales."""
        for intento in range(1, MAX_REINTENTOS + 1):
            try:
                # Socket TCP: el connect() realiza el saludo de tres vías (3-way handshake)
                self.socket_cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.socket_cliente.connect((self.host, self.puerto))
                self.pendiente = b""
                print(f"[CLIENTE] Conectado a {self.host}:{self.puerto}")
                return True
            except (ConnectionRefusedError, socket.timeout, OSError) as error:
                print(f"[CLIENTE] Intento {intento}/{MAX_REINTENTOS} fallido: {error}")
                self._cerrar_socket()
                if intento < MAX_REINTENTOS:
                    time.sleep(ESPERA_REINTENTO)  # Espera antes de reintentar
        print("[CLIENTE] No se pudo establecer la conexión con el servidor")
        return False

    def enviar_tarea(self, tarea):
        """Envía una tarea terminada en salto de línea (delimitador de mensaje)."""
        self.socket_cliente.sendall((tarea + "\n").encode("utf-8"))

    def recibir_resultado(self):
        """Lee en bloques de 4096 bytes hasta completar una línea de respuesta."""
        while b"\n" not in self.pendiente:
            bloque = self.socket_cliente.recv(BUFFER)
            if not bloque:
                raise ConnectionError("El servidor cerró la conexión")
            self.pendiente += bloque
        linea, self.pendiente = self.pendiente.split(b"\n", 1)
        return linea.decode("utf-8", errors="replace")

    def _cerrar_socket(self):
        if self.socket_cliente:
            try:
                self.socket_cliente.close()
            except OSError:
                pass
            self.socket_cliente = None

    def cerrar(self):
        self._cerrar_socket()
        print("[CLIENTE] Conexión cerrada")

    def ejecutar(self):
        """Bucle interactivo: envía tareas hasta que el usuario escribe 'éxito' o 'exito'."""
        if not self.conectar():
            return

        try:
            while True:
                tarea = input("Tarea (escribir 'éxito' o 'exito' para salir): ").strip()
                if not tarea:
                    continue

                try:
                    self.enviar_tarea(tarea)
                    print("[RESULTADO]", self.recibir_resultado())
                except (ConnectionError, BrokenPipeError, OSError) as error:
                    # Fallo durante la sesión: se intenta reconectar y se reenvía la tarea
                    print(f"[CLIENTE] Conexión perdida: {error}")
                    self._cerrar_socket()
                    if not self.conectar():
                        break
                    self.enviar_tarea(tarea)
                    print("[RESULTADO]", self.recibir_resultado())

                if tarea.lower() in PALABRAS_SALIDA:
                    break
        except (KeyboardInterrupt, EOFError):
            print("\n[CLIENTE] Interrumpido por el usuario")
        except (ConnectionError, OSError) as error:
            print(f"[CLIENTE] Error de comunicación: {error}")
        finally:
            self.cerrar()


if __name__ == "__main__":
    ClienteTCP().ejecutar()
