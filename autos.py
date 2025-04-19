from pyspark.sql import SparkSession
import json
import os

# Crear carpeta 'results' si no existe
os.makedirs("results", exist_ok=True)

def generar_json_mongo(df):
    # Filtro para vehículos con "Gravedad" "Leve" y "Causa" "Exceso de velocidad"
    df_filtered = df.filter((df.Gravedad == "Leve") & (df.Causa == "Exceso de velocidad"))

    # Guardar como JSON plano con .part y _SUCCESS
    output_dir = "results/mongo_json_output"
    df_filtered.write.mode("overwrite").json(output_dir)

    # Guardar como archivo .json legible (opcional)
    results = df_filtered.toJSON().collect()
    with open("results/dataMongo.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

def generar_json_pgsql(df):
    # Filtro para vehículos con "Estado" "Madrid" y "Gravedad" "Grave"
    df_filtered = df.filter((df.Estado == "Madrid") & (df.Gravedad == "Grave"))

    # Guardar como JSON plano con .part y _SUCCESS
    output_dir = "results/pgsql_json_output"
    df_filtered.write.mode("overwrite").json(output_dir)

    # Guardar como archivo .json legible (opcional)
    results = df_filtered.toJSON().collect()
    with open("results/dataPgsql.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("autos").getOrCreate()

    print("Leyendo autos.csv ...")
    df_autos = spark.read.csv("autos.csv", header=True, inferSchema=True)

    df_autos.createOrReplaceTempView("autos")

    generar_json_mongo(df_autos)
    generar_json_pgsql(df_autos)

    print("Archivos JSON y .part generados en 'results/'")
    spark.stop()
    print("Spark detenido.")
