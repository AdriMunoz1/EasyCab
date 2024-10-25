import tkinter as tk
import random

# Tamaño del mapa
size = 20
cell_size = 30

# Crear la ventana de Tkinter
root = tk.Tk()
root.title("Mapa")

# Crear un canvas para dibujar el mapa
canvas = tk.Canvas(root, width=size*cell_size, height=size*cell_size)
canvas.pack()

# Función para colorear cada celda del mapa
def draw_map(mapa):
    for i in range(size):
        for j in range(size):
            x1, y1 = j * cell_size, i * cell_size
            x2, y2 = x1 + cell_size, y1 + cell_size
            if mapa[i][j] == 'T':  # Taxi en movimiento
                canvas.create_rectangle(x1, y1, x2, y2, fill="green")
            elif mapa[i][j] == 't':  # Taxi parado
                canvas.create_rectangle(x1, y1, x2, y2, fill="red")
            elif mapa[i][j] == 'C':  # Cliente
                canvas.create_rectangle(x1, y1, x2, y2, fill="yellow")
            elif mapa[i][j] == 'L':  # Localización
                canvas.create_rectangle(x1, y1, x2, y2, fill="blue")
            else:  # Espacio vacío
                canvas.create_rectangle(x1, y1, x2, y2, outline="gray")

# Generar el mapa con taxis, localizaciones y clientes
mapa = [[' ' for _ in range(size)] for _ in range(size)]
#for _ in range(5):
#    mapa[random.randint(0, size-1)][random.randint(0, size-1)] = 'T'  # Taxi en movimiento
#    mapa[random.randint(0, size-1)][random.randint(0, size-1)] = 't'  # Taxi parado
#    mapa[random.randint(0, size-1)][random.randint(0, size-1)] = 'C'  # Cliente
#    mapa[random.randint(0, size-1)][random.randint(0, size-1)] = 'L'  # Localización

# Dibujar el mapa
draw_map(mapa)

# Ejecutar la ventana
root.mainloop()
