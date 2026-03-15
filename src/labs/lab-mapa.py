"""
Test standalone: verifica la generación del mapa de municipios de Canarias
Ejecutar: python test_mapa.py
"""

import pandas as pd
import geopandas as gpd
from plotnine import (ggplot, aes, geom_polygon, scale_fill_gradient,
                      coord_fixed, theme_void, labs, theme)
from pathlib import Path

# ── Rutas ────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "images" / "test"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

GEOJSON_PATH = DATA_DIR / "Municipios-2024.json"
OUTPUT_PATH = OUTPUT_DIR / "mapa_renta_municipios_test.png"


def main():
    # 1. Cargar GeoJSON
    print(f"Cargando GeoJSON desde: {GEOJSON_PATH}")
    gdf = gpd.read_file(GEOJSON_PATH)
    print(f"✓ Municipios cargados: {len(gdf)}")
    print(f"✓ Columnas disponibles: {list(gdf.columns)}")
    print(f"✓ CRS original: {gdf.crs}")
    print(f"\n── Primeras filas (sin geometría) ──")
    print(gdf.drop(columns='geometry').head())

    # 2. Reproyectar
    gdf = gdf.to_crs(epsg=4326)
    print(f"\n✓ CRS reproyectado: {gdf.crs}")

    # ⚠️ Ajusta estos nombres según lo que imprima "Columnas disponibles" arriba
    COL_NOMBRE = 'NOMBRE'     # ← cambia si es necesario
    COL_VALOR  = 'TASA_PARO'  # ← cambia al indicador que quieras visualizar

    # Verificar que existen
    if COL_NOMBRE not in gdf.columns:
        print(f"⚠ Columna '{COL_NOMBRE}' no encontrada. Columnas: {list(gdf.columns)}")
        COL_NOMBRE = gdf.columns[0]
        print(f"  Usando '{COL_NOMBRE}' como nombre")

    if COL_VALOR not in gdf.columns:
        print(f"⚠ Columna '{COL_VALOR}' no encontrada. Columnas numéricas disponibles:")
        numericas = gdf.select_dtypes(include='number').columns.tolist()
        print(f"  {numericas}")
        COL_VALOR = numericas[0] if numericas else None
        print(f"  Usando '{COL_VALOR}' como valor")

    print(f"\n✓ Columna nombre: {COL_NOMBRE}")
    print(f"✓ Columna valor:  {COL_VALOR}")
    print(f"✓ Rango de valores: {gdf[COL_VALOR].min():.2f} – {gdf[COL_VALOR].max():.2f}")

    # 3. Extraer coordenadas
    print("\nExtrayendo coordenadas de polígonos...")
    filas = []
    for idx, row in gdf.iterrows():
        geom = row.geometry
        nombre = row[COL_NOMBRE] if COL_NOMBRE in row else str(idx)
        valor = row[COL_VALOR] if COL_VALOR else None
        if geom is None:
            continue
        polys = geom.geoms if geom.geom_type == 'MultiPolygon' else [geom]
        for poly in polys:
            xs, ys = poly.exterior.xy
            for x, y in zip(xs, ys):
                filas.append({
                    'x': x, 'y': y,
                    'municipio': nombre,
                    'valor': valor,
                    'group': f"{nombre}_{idx}"
                })

    df_coords = pd.DataFrame(filas)
    print(f"✓ Coordenadas extraídas: {len(df_coords)} puntos")
    print(f"✓ Municipios únicos: {df_coords['municipio'].nunique()}")

    # 4. Generar gráfico
    print("\nGenerando gráfico...")
    grafico = (
        ggplot(df_coords, aes(x='x', y='y', group='group', fill='valor'))
        + geom_polygon(color='white', size=0.2)
        + scale_fill_gradient(low='#d4e6f1', high='#1a5276', na_value='#cccccc')
        + coord_fixed()
        + labs(title='Indicadores Laborales por Municipio — Canarias 2024', fill=COL_VALOR)
        + theme_void()
        + theme(figure_size=(16, 10))
    )

    grafico.save(str(OUTPUT_PATH), width=16, height=10, dpi=300)
    print(f"✓ Mapa guardado en: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
