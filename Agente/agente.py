import socket
import threading
import psycopg2
import sched
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
    
    def obtenerNodos(self):
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM auth.nodos')
        nodo = cursor.fetchall()

        # Cerrar el cursor
        cursor.close()
        return nodo
    
    def reordenarNodos(self):
        cursor = conn.cursor()
        cursor.execute(f'SELECT * FROM auth.nodos')
        nodos = cursor.fetchall()

        cursor.execute(f'SELECT * FROM sys.areas')
        
        areas = cursor.fetchall()
        cursor.execute(f'DELETE FROM sys.asignaciones')
        conn.commit()

        organizacion = {}
        for nodo in nodos:
            organizacion[nodo[0]] = 0

        for area in areas:
            for i in range(area[2]):
                if (i < len(nodos)):
                    idMinimo = min(organizacion, key=lambda k: organizacion[k])
                    organizacion[idMinimo] += 1
                    cursor.execute(f'INSERT INTO sys.asignaciones (nodoid, areaid) VALUES ({idMinimo},{area[0]})')

        conn.commit()
        print("REORDENADO")
        # Cerrar el cursor
        cursor.close()
        return None
    
    def borrarNodo(self, ip, puerto):
        print(f'Borrando nodo {ip}:{puerto}')
        cursor = conn.cursor()
        query = f'DELETE FROM auth.nodos WHERE ip = \'{ip}\' AND puerto = \'{puerto}\''
        cursor.execute(query)
        conn.commit()
        # Cerrar el cursor
        cursor.close()
        return True
    
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
    
    def obtenerIdNodoLibre(self):
        cursor = conn.cursor()
        query = ("""SELECT nodos.id 
                FROM auth.nodos
                JOIN sys.asignaciones 
                ON nodos.id = asignaciones.nodoId
                WHERE asignaciones.areaId = 
                (SELECT MIN(areaId)
                FROM sys.asignaciones) LIMIT 1""")
        
        cursor.execute(query)
        salida = cursor.fetchone()
        if cursor.rowcount == 0:
            return False
                
        # Cerrar el cursor
        cursor.close()
        return salida[0]
    
    def borrarArea(self, area):
        cursor = conn.cursor()

        cursor.execute(f'SELECT id FROM sys.areas WHERE nombre = \'{area}\'')
        id = cursor.fetchone()
        if cursor.rowcount == 0:
            return False
        
        cursor.execute(f'DELETE FROM sys.areas WHERE id = {id[0]}')
        conn.commit()

        self.reordenarNodos()
        return True        

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
            cursor = conn.cursor()
            cursor.execute(f'SELECT COUNT(*) FROM sys.areas WHERE nombre = \'{area}\'')
            id = cursor.fetchone()
            if id[0] > 0:
                return f"Error, ya existe esa seccion de noticias..."
            else:
                cursor.execute(f'INSERT INTO sys.areas (nombre) VALUES (\'{area}\') RETURNING id')
                conn.commit()
                id = cursor.fetchone()
                new_area_id = id[0]
                id_nodo_asignado = self.obtenerIdNodoLibre()
                cursor.execute(f'INSERT INTO sys.asignaciones (nodoid, areaid) VALUES (\'{id_nodo_asignado}\', \'{new_area_id}\')')
                conn.commit()

            return f"Creacion exitosa del area de noticias: {area}"


        elif accion == "DEL_AREA":
            area = datos[1]
            resultado = self.borrarArea(area)           
            if resultado == False:
                return f"No existe esa seccion de noticias..."
                        
            return f"Eliminacion exitosa del area de noticias: {area}"

        else:
            return "Operación inválida."
        
    def procesar_operacion_nodo(self, operacion, cliente):
        datos = operacion.split("|")
        if len(datos) < 2:
            return "Operacion invalida. Datos insuficientes."
        
        accion = datos[0]

        if accion == "REGISTER":
            nodo_ip = datos[1]
            nodo_puerto = datos[2]
            
            # Crear un cursor
            cursor = conn.cursor()
            # Ejecutar una consulta INSERT
            cursor.execute('INSERT INTO auth.nodos (ip, puerto) VALUES (%s, %s)', (cliente[0], nodo_puerto))
            # Confirmar los cambios en la base de datos
            conn.commit()
            # Cerrar el cursor
            cursor.close()

            self.reordenarNodos()
            return f"Subscripcion exitosa del nodo: {cliente[0]}:{nodo_puerto}"

    def obtenerNodoArea(self,area):
        salida =""
        cursor = conn.cursor()
        cursor.execute(f"""SELECT nod.ip, nod.puerto 
                            FROM sys.asignaciones asi 
                            JOIN sys.areas ar
                            ON (asi.areaid = ar.id)
                            JOIN auth.nodos nod
                            ON (asi.nodoid = nod.id)
                            WHERE ar.nombre = \'{area}\'""")
        rows = cursor.fetchall()
        if cursor.rowcount == 0:
            return False

        rnd = random.randint(0,cursor.rowcount-1)
        seleccionado = rows[rnd] 

        salida =f'{seleccionado[0]}|{seleccionado[1]}'        
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
            server_sock_nodos = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            # Vincular el socket al host y puerto
            server_sock_clientes.bind((host, port_clientes))
            server_sock_nodos.bind((host, port_nodos))

            # Escuchar conexiones entrantes
            server_sock_clientes.listen()
            server_sock_nodos.listen()

            print(f"Nodo maestro en ejecución en {host}:{port_clientes} y {host}:{port_nodos}...")
                    
            # Manejar la solicitud del cliente en un hilo separado
            client_handler = MasterClientHandler(server_sock_clientes, self)
            client_handler.start()
            
            nodo_handler = MasterNodeHandler(server_sock_nodos, self)
            nodo_handler.start()            

            thread_nodo_checker = MasterCheckerHandler(self)
            thread_nodo_checker.start()

            client_handler.join()
            nodo_handler.join()
            thread_nodo_checker.join()

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

class MasterNodeHandler(threading.Thread):
    def __init__(self, server_sock_nodos, master_node):
        super().__init__()
        self.server_sock_nodos = server_sock_nodos
        self.master_node = master_node

    def run(self):
        while True:
            try:
                # Aceptar una conexión entrante
                nodo_sock, nodo_addr = self.server_sock_nodos.accept()
                print(f"Nodo conectado desde {nodo_addr}")

                # Recibir la solicitud del cliente
                request = nodo_sock.recv(1024).decode()
                operacion = request.strip()

                respuesta = self.master_node.procesar_operacion_nodo(operacion, nodo_addr)
                if respuesta:
                    nodo_sock.send(respuesta.encode())
                else:
                    nodo_sock.send("Not Found".encode())           

            except socket.error as e:
                print(f"Error en la conexión con el nodo: {e}")

            finally:
                # Cerrar la conexión con el cliente
                nodo_sock.close()

class MasterCheckerHandler(threading.Thread):
    def __init__(self, master_node):
        super().__init__()
        self.master_node = master_node

    def funcion(self):
        nodos = master_node.obtenerNodos()
        print(f"Inicio chequeo de nodos")
        
        for i in nodos:
            try:   
                # Crear el socket TCP
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

                # Conectar al nodo maestro
                sock.connect((i[1], i[2]))

                # Enviar la solicitud de noticias al nodo maestro
                sock.send("HEALTHCHECK".encode())

                # Recibir las ubicaciones de nodos del nodo maestro
                respuesta = sock.recv(65507).decode()

                print(f"Helthcheck {respuesta} - {i[2]}:{i[2]}")

            except ConnectionRefusedError:
                print("No se pudo conectar al nodo, eliminando.")
                self.master_node.borrarNodo(i[1], i[2])
                self.master_node.reordenarNodos()
                return None
            
            except Exception as e:
                print("No se pudo conectar al nodo, eliminando.")
                self.master_node.borrarNodo(i[1], i[2])
                self.master_node.reordenarNodos()
                return None

            finally:
                # Cerrar la conexión
                sock.close()

    def run(self):
            while True:
                sc = sched.scheduler()
                sc.enter(1, 1, self.funcion)
                sc.run()

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
