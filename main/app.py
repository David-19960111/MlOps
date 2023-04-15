from typing import List, Dict
import pandas as pd
from fastapi import FastAPI

# Cargamos los datasets ya procesados anteriormente
df = pd.read_csv('/Users/ale_d/Desktop/MLops_Projects/MlOpsData/df.csv')

# Creamos la instancia de la aplicación FastAPI
app = FastAPI()

# Función para obtener la película (sólo película, no serie, ni documentales, etc)
# con mayor duración según año, plataforma y tipo de duración
@app.get("/max_duration")
async def get_max_duration(year: int, platform: str, duration_type: str):
    # Filtrar el dataframe según los parámetros recibidos
    df_filtered = df[(df["release_year"] == year) & (df["plataforma"] == platform) & (df["duration_type"] == duration_type) & (df["type"] == "movie")]
    
    # Encontrar la película con la mayor duración
    max_duration = df_filtered["duration_int"].max()
    movie_title = df_filtered[df_filtered["duration_int"] == max_duration]["title"].values[0]
    
    return {"movie_title": movie_title}

# Función para obtener la cantidad de películas (sólo películas, no series, ni documentales, etc)
# según plataforma, con un puntaje mayor a XX en determinado año
@app.get("/score_count")
async def get_score_count(platform: str, scored: float, year: int):
    # Filtrar el dataframe según los parámetros recibidos
    df_filtered = df[(df["release_year"] == year) & (df["plataforma"] == platform) & (df["rating_num"] >= scored) & (df["type"] == "movie")]
    
    # Contar el número de películas que cumplen las condiciones
    count = df_filtered.shape[0]
    return {'plataforma': platform,
            "cantidad": count,
            'anio': year,
            'score': scored}



# Función para obtener la cantidad de películas (sólo películas, no series, ni documentales, etc)
# según plataforma
@app.get("/count_platform")
async def get_count_platform(platform: str):
    # Filtrar el dataframe según la plataforma recibida
    df_filtered = df[df["plataforma"] == platform]
    # Contar el número de películas de la plataforma dada
    count = df_filtered[df_filtered["type"] == "movie"].shape[0]
    
    return {'plataforma': platform,
            "peliculas": count}

# Función para obtener el actor que más se repite según plataforma y año
@app.get("/actor")
async def get_actor(platform: str, year: int):
    # Filtrar el dataframe según los parámetros recibidos
    df_filtered = df[(df["release_year"] == year) & (df["plataforma"] == platform) & (df["type"] == "movie")]
    
    # Obtener la lista de actores y contar su frecuencia
    actor_counts = df_filtered["cast"].str.split(", ").explode().value_counts()
    
    # Devolver el actor con la mayor frecuencia
    top_actor = actor_counts.index[0]
    
    return {'plataforma': platform,
            'anio': year,
            "actor": top_actor}


#La cantidad de contenidos/productos (todo lo disponible en streaming) que se publicó por país y año
@app.get('/prod_per_county')
async def prod_per_county(tipo: str, pais: str, anio: int):
    # Filtramos los contenidos según el país y año dados
    filtered_data = df[(df['country'] == pais) & (df['release_year'] == anio)]
    # Filtramos los contenidos según el tipo de contenido dado (pelicula o serie)
    filtered_data = filtered_data[filtered_data['type'] == tipo]
    # Contamos la cantidad de contenidos/productos
    count = len(filtered_data)
    return {'pais': pais,
            'anio': anio, 
            tipo: count}


#La cantidad total de contenidos/productos (todo lo disponible en streaming, series, documentales, peliculas, etc) según el rating de audiencia dado.
@app.get('/get_contents')
async def get_contents(rating: str):
    # Filtramos los contenidos según el rating de audiencia dado
    filtered_data = df[df['rating'] == rating]
    # Obtenemos la cantidad total de contenidos con ese rating de audiencia
    count = len(filtered_data)
    return {'rating': rating,
            'contenido': count}


