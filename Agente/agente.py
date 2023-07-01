import socket
import threading
import psycopg2
import random
import os

class MasterNode:        
    def identificarUsuario(self, nombre):
        cursor = conn.cursor()
        cursor.execute(f'SELECT id, nombre, isadmin FROM auth.usuarios WHERE nombre = \'{nombre}\'')
        id = cursor.fetchone()
        if cursor.rowcount == 0:
            # Ejecutar una consulta INSERT
            cursor.execute(f'INSERT INTO auth.usuarios (nombre) VALUES (\'{nombre}\') RETURNING id, nombre, isadmin')
            conn.commit()
            id = cursor.fetchone()

        # Cerrar el cursor
        cursor.close()
        return id
    
    def identificarArea(self, area):
        cursor = conn.cursor()
        cursor.execute(f'SELECT id FROM sys.areas WHERE nombre = \'{area}\'')
        id = cursor.fetchone()
        if cursor.rowcount == 0:
            return False
        # Cerrar el cursor
        cursor.close()
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
    
    def obtenerAreas(self, clienteid):
        salida = "\nLas areas son:\n"
        cursor = conn.cursor()
        cursor.execute(f'SELECT nombre FROM sys.suscripciones JOIN sys.areas ON (areaid = id) WHERE usuarioid = \'{clienteid}\'')
        rows = cursor.fetchall()
        if cursor.rowcount == 0:
            return False

        for row in rows:
            aux = row[0]
            salida +="* "+ aux + "\n"
        # Cerrar el cursor
        cursor.close()
        return salida
    
    def obtenerAreasTodas(self):
        salida = "\nLas areas son: \n"
        cursor = conn.cursor()
        cursor.execute(f'SELECT nombre FROM sys.areas ')
        rows = cursor.fetchall()
        if cursor.rowcount == 0:
            return False

        for row in rows:
            aux = row[0]
            salida +="* "+ aux + "\n"
        # Cerrar el cursor
        cursor.close()
        return salida
    
    def obtenerNodoLibre(self):
        cursor = conn.cursor()
        query = ("""SELECT auth.comisiones.nombre
                    FROM auth.comisiones
                    JOIN sys.asignaciones 
                    ON comisiones.id = asignaciones.comisionId
                    WHERE asignaciones.comisionId = 
                    (SELECT comisionId
                    FROM sys.asignaciones
                    GROUP BY comisionId
                    ORDER BY COUNT(*) ASC
                    LIMIT 1) LIMIT 1
                    """)
        
        cursor.execute(query)
        salida = cursor.fetchone()
        if cursor.rowcount == 0:
            return False
                
        # Cerrar el cursor
        cursor.close()
        return salida[0]   

    def procesar_operacion_cliente(self, operacion):
        datos = operacion.split("|")
        if len(datos) < 2:
            return "Operacion invalida. Datos insuficientes."
        
        accion = datos[0]

        if accion == "USER":
            cliente = datos[1]
            ident = master_node.identificarUsuario(cliente)
            return f"{ident[2]}"
    
        if accion == "AREASTODAS":
            resp = master_node.obtenerAreasTodas()
            if resp:
                print(f"Nodo Maestro: Las areas son: {resp}")
                return f"{resp}"
            else:
                print("Nodo Maestro: No hay areas")
                return "No hay areas creadas"
         
        elif accion == "SUB":
            area = datos[1]
            resp = master_node.obtenerNodoArea(area)
            if resp:
                print(f"Nodo Maestro: el nodo correspondiente es: {resp}")
                return f"{resp}"
            else:
                print("Nodo Maestro: ERROR")
                return "ERROR"    
            
        elif accion == "UNSUB":
            area = datos[1]
            resp = master_node.obtenerNodoArea(area)
            if resp:
                print(f"Nodo Maestro: el nodo correspondiente es: {resp}")
                return f"{resp}"
            else:
                print("Nodo Maestro: ERROR")
                return "ERROR" 
            
        elif accion == "AREASSUB":
            cliente = datos[1]
            clienteid = master_node.identificarUsuario(cliente)[0]
            resp = master_node.obtenerAreas(clienteid)
            if resp:
                print(f"Nodo Maestro: Las areas son: {resp}")
                return f"{resp}"
            else:
                print("Nodo Maestro: Cliente no esta subscripto a ningun area")
                return "Cliente no esta subscripto a ningun area"   
            
        elif accion == "NEWS":
            area = datos[2]
            resp = master_node.obtenerNodoArea(area)
            if resp:
                print(f"Nodo Maestro: el nodo correspondiente es: {resp}")
                return f"{resp}"
            else:
                print("Nodo Maestro: ERROR")
                return "ERROR"

        elif accion == "ADD":
            area = datos[1]
            resp = master_node.obtenerNodoArea(area)
            if resp:
                print(f"Nodo Maestro: el nodo correspondiente es: {resp}")
                return f"{resp}"
            else:
                print("Nodo Maestro: ERROR")
                return "ERROR"
            
        elif accion == "DEL": 
            area = datos[1]
            resp = master_node.obtenerNodoArea(area)
            if resp:
                print(f"Nodo Maestro: el nodo correspondiente es: {resp}")
                return f"{resp}"
            else:
                print("Nodo Maestro: ERROR")
                return "ERROR"
            
        elif accion == "ADD_AREA":
            area = datos[1]
            resp = master_node.obtenerNodoLibre()
            if resp:
                print(f"Nodo Maestro: el nodo correspondiente es: {resp}")
                return f"{resp}"
            else:
                print("Nodo Maestro: ERROR")
                return "ERROR"
            
        elif accion == "DEL_AREA": 
            area = datos[1]
            resp = master_node.obtenerNodoLibre()
            if resp:
                print(f"Nodo Maestro: el nodo correspondiente es: {resp}")
                return f"{resp}"
            else:
                print("Nodo Maestro: ERROR")
                return "ERROR"        

        else:
            return "Operación inválida."         

    def obtenerNodoArea(self,area):
        salida =""
        cursor = conn.cursor()
        cursor.execute(f"""SELECT nod.nombre 
                            FROM sys.asignaciones asi 
                            JOIN sys.areas ar
                            ON (asi.areaid = ar.id)
                            JOIN auth.comisiones nod
                            ON (asi.comisionid = nod.id)
                            WHERE ar.nombre = \'{area}\'""")
        rows = cursor.fetchall()
        if cursor.rowcount == 0:
            return False

        comisiones = []
        for comision in rows:
            try:   
                # Crear el socket TCP
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                # Conectar al nodo maestro
                sock.connect((comision[0], 8005))

                # Enviar la solicitud de noticias al nodo maestro
                sock.send("HEALTHCHECK".encode())

                # Recibir las ubicaciones de nodos del nodo maestro
                respuesta = sock.recv(65507).decode()

                print(f"Healthcheck {respuesta} - {comision[0]}:8005")
                comisiones.append(comision)
            except Exception:
                print(f"{comision[0]} no esta disponible")

            finally:
                # Cerrar la conexión
                sock.close()


        rnd = random.randint(0,len(comisiones)-1)
        seleccionado = comisiones[rnd] 

        salida =f'{seleccionado[0]}'        
        # Cerrar el cursor
        cursor.close()
        return salida


    def start(self):
        # Iniciar el servidor del nodo maestro
        host = '0.0.0.0'
        port_clientes = 8000
        port_nodos = 8001

        try:
            # Crear el socket TCP
            server_sock_clientes = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            # Vincular el socket al host y puerto
            server_sock_clientes.bind((host, port_clientes))

            # Escuchar conexiones entrantes
            server_sock_clientes.listen()

            print(f"Nodo maestro en ejecución en {host}:{port_clientes} y {host}:{port_nodos}...")

            # Manejar la solicitud del cliente en un hilo separado
            client_handler = MasterClientHandler(server_sock_clientes, self)
            client_handler.start()            
            client_handler.join()

        except KeyboardInterrupt:
            print("Nodo maestro detenido.")

            # Cerrar el socket del servidor
            server_sock_clientes.close()
            server_sock_nodos.close()           


class MasterClientHandler(threading.Thread):
    def __init__(self, server_sock_clientes, master_node):
        super().__init__()
        self.server_sock_clientes = server_sock_clientes
        self.master_node = master_node

    def run(self):
        while True:
            try:
                # Aceptar una conexión entrante
                client_sock, client_addr = self.server_sock_clientes.accept()
                print(f"Cliente conectado desde {client_addr}")

                # Recibir la solicitud del cliente
                request = client_sock.recv(1024).decode()
                operacion = request.strip()

                respuesta = self.master_node.procesar_operacion_cliente(operacion)
                if respuesta:
                    client_sock.send(respuesta.encode())
                else:
                    client_sock.send("Not Found".encode())

            except socket.error as e:
                print(f"Error en la conexión con el cliente: {e}")

            finally:
                # Cerrar la conexión con el cliente
                client_sock.close()

if __name__ == "__main__":
    
    # Crear el nodo maestro y comenzar el servidor
    master_node = MasterNode()

    #Conectamos la base de datos
    conn = psycopg2.connect(
    host=os.environ["DBHOST"],
    port=os.environ["DBPORT"],
    database=os.environ["DBNAME"],
    user=os.environ["DBUSER"],
    password=os.environ["DBPASSWORD"]
    )
    print("Base de datos iniciada correctamente")
   
    # Comenzar el servidor del nodo maestro
    master_node.start()
