import geopandas as gpd
import pandas as pd
from pathlib import Path

gdf = gpd.read_file(r"C:\Users\laura\OneDrive\Documentos\25-26 Máster Ciberseguridad e Inteligencia de Datos\Visualización\Práctica 2\data\Municipios-2024.json")

cols = ['label','tpar_t','tpar_m','tpar_f','tsal_t','tsal_m','tsal_f',
        'ppar_t_16a24','ppar_t_25a34','ppar_t_35a44','ppar_t_45a54','ppar_t_55mas']
cols_existentes = [c for c in cols if c in gdf.columns]

df = gdf[cols_existentes].copy()
df.to_csv(r"C:\Users\laura\OneDrive\Documentos\25-26 Máster Ciberseguridad e Inteligencia de Datos\Visualización\Práctica 2\data\municipios_indicadores.csv", index=False)
print(f"✓ CSV guardado con {len(df)} filas y columnas: {cols_existentes}")
