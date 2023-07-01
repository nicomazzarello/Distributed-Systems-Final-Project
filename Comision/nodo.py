import socket
import threading
import os
import psycopg2
from datetime import datetime

class DataNode:
    def __init__(self, comision):
        self.comision = f"comision{comision}"

    def start(self):
        # Iniciar el servidor del nodo de datos
        try:
            # Crear el socket TCP
            server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            # Vincular el socket al host y puerto del nodo de datos
            server_sock.bind(('0.0.0.0', 8005))

            # Escuchar conexiones entrantes
            server_sock.listen()

            localIP = server_sock.getsockname()[0]
            localPort = server_sock.getsockname()[1]
            print(f"Nodo {self.comision} en ejecución en {localIP}:{localPort}...")                    
            
            while True:
                # Aceptar una conexión entrante
                client_sock, client_addr = server_sock.accept()
                print(f"Cliente conectado desde {client_addr}")

                # Manejar la solicitud del cliente en un hilo separado
                client_handler = DataNodeClientHandler(client_sock, self)
                client_handler.start()            
           
        except KeyboardInterrupt:
            print(f"Nodo de datos detenido.")

        finally:
            # Cerrar el socket del servidor
            server_sock.close()

    def clienteSubscrito(self,clienteNom):
        cursor = conn.cursor()
        cursor.execute(f"""SELECT 1
                            FROM sys.suscripciones sus
                            JOIN auth.usuarios usu
                            ON sus.usuarioid = usu.id
                            JOIN sys.areas ar
                            ON sus.areaid = ar.id 
                            WHERE usu.nombre = (\'{clienteNom}\')""")

        if cursor.rowcount == 0:
            return False
        return True
    
    def obtenerNoticias(self, clienteNom, areanom, ultimoID):
        cursor = conn.cursor()
        resu = self.clienteSubscrito(clienteNom)
        if not resu:
            return False
        
        if resu:
            cursor.execute(f"""SELECT texto, noti.id
                                FROM sys.noticias noti 
                                JOIN sys.areas ON noti.areaid=areas.id
                                WHERE nombre = (\'{areanom}\') AND noti.id > {ultimoID}""")
            rows = cursor.fetchall()       
            
        # Cerrar el cursor
        cursor.close()
        return rows
    
    def enviarMensaje(self,socket, mensaje):
        if len(mensaje.encode()) < 65507:
            socket.send(mensaje.encode())

    def agregarNoticia(self, cliente, area, noticia):
        cursor = conn.cursor()
        idCliente = self.obtenerIDdCliente(cliente)
        idArea = self.obtenerIDdArea(area)
        resu = self.clienteSubscrito(cliente)
        if resu:
            cursor.execute(f"""INSERT INTO sys.noticias(areaid, authorid, fecha, texto)
                            VALUES ({idArea},{idCliente},\'{datetime.now()}\',\'{noticia}\')""")
            conn.commit()
            cursor.close()
            return True
        else:
            return False

    def eliminarNoticia(self, cliente, area, noticia):
        salida = bool
        cursor = conn.cursor()
        idCliente = self.obtenerIDdCliente(cliente)
        idArea = self.obtenerIDdArea(area)
        resu = self.clienteSubscrito(cliente)
        if resu:
            cursor.execute(f'DELETE FROM sys.noticias WHERE areaid = \'{idArea}\' AND authorid = \'{idCliente}\' AND texto = \'{noticia}\'')
            if cursor.rowcount > 0:
                salida = True
                conn.commit()
            else:
                salida = False
                cursor.close() 
        else:
            salida = False
        return salida

    def obtenerIDdCliente(self, nombreCliente):
        cursor = conn.cursor()
        cursor.execute(f"""SELECT usu.id 
                            FROM auth.usuarios usu
                            WHERE usu.nombre =  \'{nombreCliente}\'""")

        if cursor.rowcount == 0:
            return False    
        
        id = cursor.fetchone()
        return id[0]

    def subArea(self, idcliente, idarea):
        cursor = conn.cursor()
        cursor.execute(f'SELECT usuarioid,areaid FROM sys.suscripciones WHERE usuarioid = \'{idcliente}\' AND areaid = \'{idarea}\'')
        id = cursor.fetchone()
        if cursor.rowcount == 0:           
            cursor.execute(f'INSERT INTO sys.suscripciones (usuarioid, areaid) VALUES (\'{idcliente}\', \'{idarea}\') RETURNING usuarioid, areaid')
            conn.commit()
            id = cursor.fetchone()
        else:
            id = False
        # Cerrar el cursor
        cursor.close()
        return id
    
    def unsubArea(self, idcliente, idarea):
        salida =""
        cursor = conn.cursor()
        cursor.execute(f'DELETE FROM sys.suscripciones WHERE usuarioid = \'{idcliente}\' AND areaid = \'{idarea}\'')
        conn.commit()
        if cursor.rowcount > 0:
            salida = "DELETE realizado con exito"
        else:
            salida = False
        cursor.close()
        
        return salida
    
    def obtenerIDdArea(self, nombreCliente):
        cursor = conn.cursor()
        cursor.execute(f'SELECT id FROM sys.areas WHERE nombre = \'{nombreCliente}\'')
        id = cursor.fetchone()
        if cursor.rowcount == 0:
            return False
        # Cerrar el cursor
        cursor.close()
        return id[0]
    
    def procesar_operacion(self, operacion, socket):
        datos = operacion.split("|")        
        
        accion = datos[0]

        if accion == "HEALTHCHECK":            
            print(f"Healthcheck")
            self.enviarMensaje(socket,"OK")
            return f"OK"
        
        elif accion == "SUB":
            cliente = datos[1]
            area = datos[2]
            clienteid = self.obtenerIDdCliente(cliente)
            areaid = self.obtenerIDdArea(area)
            if (areaid == False):
                msj = "Nodo: No existe el area a subscribirse."
                self.enviarMensaje(socket,msj)
                return False           
            resp = self.subArea(clienteid,areaid)
            if resp:
                msj =f"Nodo: Subscripcion exitosa del cliente: {cliente} al area {area}"
                self.enviarMensaje(socket,msj)
            else:
                msj =f"Nodo: Cliente {cliente} ya esta subscripto  al area {area}"
                self.enviarMensaje(socket,msj)

        elif accion == "UNSUB":
            cliente = datos[1]
            area = datos[2]
            clienteid = self.obtenerIDdCliente(cliente)
            areaid = self.obtenerIDdArea(area)
            if (areaid == False):
                print("Nodo Maestro: No existe el area a desubscribirse.")
                return "No existe el area a desubscribirse."
            
            resp = self.unsubArea(clienteid,areaid)
            if resp:
                msj =f"Nodo: Desubscripcion exitosa del cliente: {cliente} al area {area}"
                self.enviarMensaje(socket,msj)
            else:
                msj ="Nodo: Error en desubscripcion"
                self.enviarMensaje(socket,msj)   
            
        elif accion == "NEWS":
            cliente = datos[1]
            area = datos[2]
            ultimoID = datos[3]
            resp = self.obtenerNoticias(cliente, area, ultimoID)
            if resp == False:
                print(f"Nodo: el cliente {cliente} no esta subscripto")
                self.enviarMensaje(socket, "-1")
                return -1
            
            aux = f'{len(resp)}'
            self.enviarMensaje(socket, aux)
            socket.recv(65507).decode()

            if len(resp) == 0:
                return 0
            
            for noticia in resp:
                msj = f"{noticia[0]}|{noticia[1]}"
                print(msj)
                self.enviarMensaje(socket,msj) 
                socket.recv(65507).decode()

            return True
        
        elif accion == "ADD":
            cliente = datos[1]
            area = datos[2]
            noticia = datos[3]
            resp = self.agregarNoticia(cliente, area, noticia)
            if resp == False:
                msj = f"El cliente {cliente} no pudo agregar la noticia al area {area}" 
                self.enviarMensaje(socket,msj)
                return False
            else:
                msj = f"El cliente {cliente} agrego correctamente la noticia al area {area}"
                self.enviarMensaje(socket,msj)
                return True
        
        elif accion == "DEL":
            cliente = datos[1]
            area = datos[2]
            noticia = datos[3]
            resp = self.eliminarNoticia(cliente, area, noticia)
            if resp == False:
                msj = f"El cliente {cliente} no pudo eliminar la noticia al area {area}" 
                self.enviarMensaje(socket,msj)
                return False
            else:
                msj = f"El cliente {cliente} elimino correctamente la noticia al area {area}"
                self.enviarMensaje(socket,msj)
                return True


class DataNodeClientHandler(threading.Thread):
    def __init__(self, client_sock, data_node):
        super().__init__()
        self.client_sock = client_sock
        self.data_node = data_node

    def run(self):
        try:
            # Realizar las operaciones necesarias en el nodo de datos
            # Por ejemplo, enviar las noticias solicitadas al cliente
            # Recibir la solicitud del cliente
            request = self.client_sock.recv(1024).decode()
            operacion = request.strip()

            self.data_node.procesar_operacion(operacion,self.client_sock)
            
        except socket.error as e:
            print(f"Error en la conexión con el cliente: {e}")

        finally:
            # Cerrar la conexión con el cliente
            self.client_sock.close()

if __name__ == "__main__":
    # Crear el nodo de datos y comenzar el servidor
    node = DataNode(os.environ["COMISION"])  # Cambia 'localhost' y 8001 por la dirección IP y el puerto del nodo de datos
    
    #Conectamos la base de datos
    conn = psycopg2.connect(
    host=os.environ["DBHOST"],
    port=os.environ["DBPORT"],
    database=os.environ["DBNAME"],
    user=os.environ["DBUSER"],
    password=os.environ["DBPASSWORD"]
    )   
    
    node.start()