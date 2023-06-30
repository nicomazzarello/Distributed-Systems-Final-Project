import socket
import os

class Cliente:
    def __init__(self, nombre):
        self.nombre = nombre
        self.lecturas = {}

    def obtener_nombre(self):
        return self.nombre

    def establecer_nombre(self, nuevo_nombre):
        self.nombre = nuevo_nombre

class Agente:
    def __init__(self, master_ip, master_port):
        self.master_ip = master_ip
        self.master_port = master_port

    def send_and_receive_master(self, operacion):
        try:
            # Crear el socket TCP
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            # Conectar al nodo maestro
            sock.connect((self.master_ip, self.master_port))

            # Enviar la solicitud de noticias al nodo maestro
            sock.send(operacion.encode())

            # Recibir las ubicaciones de nodos del nodo maestro
            respuesta = sock.recv(1024).decode()
            
            return respuesta

        except ConnectionRefusedError:
            print("No se pudo conectar al nodo maestro.")
            return None

        finally:
            # Cerrar la conexión
            sock.close()

    def conectar_al_nodo(self, nodoIP, nodoPuerto):
        try:
            # Crear el socket TCP
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            # Obtener la dirección IP y el puerto del nodo
        
            node_port = int(nodoPuerto)

            # Conectar al nodo de datos
            sock.connect((nodoIP, node_port))
            
            return sock

        except ConnectionRefusedError:
            print("No se pudo conectar al nodo de datos.")
            return None

    def send_nodo(self, operacion, socket):
        try:
            
            # Enviar la solicitud de noticias al nodo maestro
            socket.send(operacion.encode())

        except ConnectionRefusedError:
            print("No se pudo conectar al nodo maestro.")
            return None     

    def receive_nodo(self, socket):
            try:                
                # Recibir las ubicaciones de nodos del nodo maestro
                respuesta = socket.recv(65507).decode()
                
                return respuesta

            except ConnectionRefusedError:
                print("No se pudo conectar al nodo")
                return None

    def desconectar_del_nodo(self, sock):
        try:
            sock.close()
        except ConnectionRefusedError:
            print("No se puede cerrar el socket")
            return None 
        
    def obtenerSuscripciones(self, clienteActual):
        operacion = f"AREASSUB|{clienteActual.obtener_nombre()}"
        respuesta = self.send_and_receive_master(operacion)
        datos = respuesta.split("*")
        areas = [area.strip() for area in datos[1:] if area.strip()]
        for i in areas:
            clienteActual.lecturas[f"{i}"]=0
        return respuesta
          

def mensajeInicio():
    print("==== Inicio de sesión ====")
    clienteActual.establecer_nombre(input("Ingrese el nombre del usuario: "))
    nom = clienteActual.obtener_nombre()
    print(f"==== Bienvenido {nom} ====")
    operacion = f"USER|{nom}"
    resp = agente.send_and_receive_master(operacion)
    return resp

def mensajeCliente():
    # Mostrar menú de opciones
    print("----------* Menú *----------")
    print("1. Ver áreas existentes")
    print("2. Subscribirse a un área de noticias")
    print("3. Desubscribirse de un área de noticias")
    print("4. Ver áreas subscriptas")
    print("5. Obtener noticias")
    print("6. Agregar noticia")
    print("7. Eliminar noticia")
    print("8. Cerrar sesion")
    print("10. Salir")

def mensajeAdmin():
    # Mostrar menú de opciones
    mensajeCliente()
    print("----* Opciones del administrador *----")
    print("11. Agregar nueva área de noticias")
    print("12. Eliminar área de noticias")

if __name__ == "__main__":
    # Crear el cliente
    clienteActual = Cliente("nombre")

    agente = Agente(os.environ["AGENTE_HOST"], 8000)  # Cambia 'localhost' y 8000 por la dirección IP y el puerto del nodo maestro

    isadmin = mensajeInicio()
   
    while True:
        if (isadmin == "True"):
            mensajeAdmin()

        else:
            mensajeCliente()
        
        opcion = input("\nIngrese el número de la opción deseada: ")

        if opcion == "1":   
            operacion = f"AREASTODAS|A"
            respuesta = agente.send_and_receive_master(operacion)
            print(respuesta)

        elif opcion == "2":
            cliente = clienteActual.obtener_nombre()
            area = input("Ingrese el nombre del área de noticias a la que desea subscribirse: ")
            operacion = f"SUB|{area}"
            resp = agente.send_and_receive_master(operacion)
            if resp == "ERROR":
                print(f"No se pudo obtener el nodo que contenga el area {area}")
            else:
                datos = resp.split("|")
                sock = agente.conectar_al_nodo(datos[0],datos[1])
                operacion = f"SUB|{cliente}|{area}"
                agente.send_nodo(operacion, sock)
                respuesta = agente.receive_nodo(sock)
                print(respuesta)
                agente.desconectar_del_nodo(sock)

        elif opcion == "3":
            cliente = clienteActual.obtener_nombre()
            area = input("Ingrese el nombre del área de noticias de la que desea desubscribirse: ")
            operacion = f"UNSUB|{area}"
            resp = agente.send_and_receive_master(operacion)
            if resp == "ERROR":
                print(f"No se pudo obtener el nodo que contenga el area {area}")
            else:
                datos = resp.split("|")
                sock = agente.conectar_al_nodo(datos[0],datos[1])
                operacion = f"UNSUB|{cliente}|{area}"
                agente.send_nodo(operacion, sock)
                respuesta = agente.receive_nodo(sock)
                print(respuesta)
                agente.desconectar_del_nodo(sock)

        elif opcion == "4":
            respuesta = agente.obtenerSuscripciones(clienteActual)
            print(respuesta)
            
        elif opcion == "5":
            cliente = clienteActual.obtener_nombre()
            area = input("Ingrese el area: ")
            op = input("Recibir todas las noticias ingrese → 1\nRecibir las noticias no leidas ingrese → 2 :")
            if op == "1":
                ultimoID = 0
            elif op == "2":
                if not area in clienteActual.lecturas:
                    agente.obtenerSuscripciones(clienteActual)
                if area in clienteActual.lecturas:
                    ultimoID = clienteActual.lecturas[area] 
                else: 
                    ultimoID = 0
            else:
                print("Ingreso un numero incorrecto")

            operacion = f"NEWS|{cliente}|{area}|{ultimoID}"
            resp = agente.send_and_receive_master(operacion)
            if resp == "ERROR":
                print(f"No existe el area {area}")
            else:
                datos = resp.split("|")
                sock = agente.conectar_al_nodo(datos[0],datos[1])
                agente.send_nodo(operacion, sock)
                respuesta = agente.receive_nodo(sock)
                agente.send_nodo("ok", sock)
                if respuesta == "-1":
                    print(f"Cliente: {cliente} no esta subscripto al area {area}")
            
                elif respuesta == "0":
                    print(f"El cliente: {cliente} esta al dia")
                else:
                    for i in range(int(respuesta)):
                    
                        resp = agente.receive_nodo(sock)
                        agente.send_nodo("ok", sock)
                        datos = resp.split("|")
                        clienteActual.lecturas[f"{area}"] = datos[1]
                        print(datos[0])
            
                agente.desconectar_del_nodo(sock)

        elif opcion == "6":
            cliente = clienteActual.obtener_nombre()
            area = input("Ingrese el nombre del área de noticias en la que desea agregar la noticia: ")
            noticia = input("Ingrese el texto de la noticia: ")
            operacion = f"ADD|{area}"
            resp = agente.send_and_receive_master(operacion)
            if resp == "ERROR":
                print(f"No se pudo obtener el nodo que contenga el area {area}")
            else:
                datos = resp.split("|")
                sock = agente.conectar_al_nodo(datos[0],datos[1])
                operacion = f"ADD|{cliente}|{area}|{noticia}"
                agente.send_nodo(operacion, sock)
                respuesta = agente.receive_nodo(sock)
                print(respuesta)
                agente.desconectar_del_nodo(sock)

        elif opcion == "7":
            cliente = clienteActual.obtener_nombre()
            area = input("Ingrese el nombre del área de la noticias a la que desea eliminar una noticia: ")
            noticia = input("Ingrese el texto de la noticia a eliminar: ")
            operacion = f"DEL|{area}"
            resp = agente.send_and_receive_master(operacion)  
            if resp == "ERROR":
                print(f"No se pudo obtener el nodo que contenga el area {area}")
            else:
                datos = resp.split("|")
                sock = agente.conectar_al_nodo(datos[0],datos[1])
                operacion = f"DEL|{cliente}|{area}|{noticia}"
                agente.send_nodo(operacion, sock)
                respuesta = agente.receive_nodo(sock)
                print(respuesta)
                agente.desconectar_del_nodo(sock)

        elif opcion == "8":
            print("\nCerrando sesion...\n")
            cliente = ""
            isadmin = mensajeInicio()

        elif isadmin == "True" and opcion == "11":
            area = input("Ingrese el nombre del área de noticias que desea agregar: ")
            operacion = f"ADD_AREA|{area}"
            respuesta = agente.send_and_receive_master(operacion)
            print(respuesta)

        elif isadmin == "True" and opcion == "12":
            area = input("Ingrese el nombre del área de noticias que desea eliminar: ")
            operacion = f"DEL_AREA|{area}"
            respuesta = agente.send_and_receive_master(operacion)
            print(respuesta)

        elif opcion == "10":
            break

        else:
            print("Opción inválida. Por favor, ingrese un número válido.")
   