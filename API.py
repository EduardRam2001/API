from flask import Flask,jsonify
import pandas as pd
from azure.cosmos import CosmosClient

#-----------------------------------------------------------------------
#Parámetros de la base de datos proporcionado por la nube 
#CONEXION CON COSMO DB

# Inicialización con el cliente Cosmos
endpoint = "https://mydbnosql.documents.azure.com:443/"
key = 'YUlWZgi1ULdnFmKftJWKxuOoZTMTm0VjfHbyAbLlu2QYJyMGK5GEwD0mSbZ9mKr1MJs8oWDIzelYACDbd70nrA=='
client = CosmosClient(endpoint, key)

#Referencia a la base de datos y al contenedor
database_name = 'clientes_gastos_categorias'
container_name = 'registros_gastos_clientes'
database = client.get_database_client(database_name)
container = database.get_container_client(container_name)


# Se define la app Flask
app = Flask(__name__)




# Ruta por defecto
@app.route("/")
def root():
    return "INGRESE EL ID_CLIENTE DESPUES de 127.0.0.1:5000/#IDCLIENTE"



# Ruta para consultar un cliente por su documento
@app.route("/<documento>")
def obtener_cliente(documento):
    try:
        # Se consulta la base de datos y se almacenan los resultados en una lista
        data = []
        for item in container.query_items(
            query=f'SELECT * FROM mycontainer c WHERE c.documento = "{documento}"',
            enable_cross_partition_query=True):

            data.append(item)


        # Se convierte la lista de resultados en un DataFrame de pandas
        df = pd.DataFrame(data)

        # Se seleccionan las columnas específicas que se desean devolver
        columnas = ['documento', 'tipo_doc', 'categoria', 'mnt_trx_mm', 'num_trx', 'pct_mnt_tot', 'pct_num_trx_tot']
        df = df[columnas]

        # Se devuelve la respuesta como un objeto JSON con el valor del documento
        return jsonify({"DATOS CORRESPONDIENTE AL CLIENTE:": documento, "DESCRIPCION": df.to_dict(orient="records")})
    
    except:
        return jsonify({"ERROR!!!!": f"ID DEL CLIENTE:{documento} NO EXISTE EN LA BASE DE DATOS"})





# Se ejecuta la app
if __name__ == "__main__":
    app.run(debug=True)

