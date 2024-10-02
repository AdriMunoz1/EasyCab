import socket

ServerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
ServerSocket.bind(('localhost', 9999))
ServerSocket.listen(5)

while True:
    conexion, addr = ServerSocket.accept()
    conexion.send("Hola, desde el Servidor")
    conexion.close()