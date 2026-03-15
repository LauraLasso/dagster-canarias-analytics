"""
Pipeline: Mapa de indicadores laborales por municipio de Canarias
Fuente: GeoJSON ISTAC - Indicadores laborales municipios 2024
"""

import subprocess
import pandas as pd
import geopandas as gpd
from plotnine import (ggplot, aes, geom_polygon, scale_fill_gradient,
                      coord_fixed, theme_void, labs, theme)
from dagster import asset, AssetExecutionContext, MaterializeResult, MetadataValue
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "images" / "dagster"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def subir_imagen_a_ghpages(imagen_path: str, context):
    import shutil, tempfile
    repo_url = subprocess.run(
        ["git", "remote", "get-url", "origin"],
        capture_output=True, text=True
    ).stdout.strip()
    context.log.info(f"gh-pages: repo_url = {repo_url}")

    with tempfile.TemporaryDirectory() as tmpdir:
        clone = subprocess.run(
            ["git", "clone", "--branch", "gh-pages", "--single-branch", repo_url, tmpdir],
            capture_output=True, text=True
        )
        if clone.returncode != 0:
            context.log.error(f"gh-pages: clone falló → {clone.stderr}")
            return

        destino = Path(tmpdir) / Path(imagen_path).name
        shutil.copy2(imagen_path, destino)

        subprocess.run(["git", "-C", tmpdir, "add", Path(imagen_path).name])
        result = subprocess.run(
            ["git", "-C", tmpdir, "commit", "-m", f"Auto: {Path(imagen_path).name} actualizado"],
            capture_output=True, text=True
        )
        context.log.info(f"gh-pages: commit → {result.stdout.strip()}")

        if "nothing to commit" in result.stdout:
            context.log.info("gh-pages: imagen sin cambios, no se hace push")
        else:
            push = subprocess.run(
                ["git", "-C", tmpdir, "push", "origin", "gh-pages"],
                capture_output=True, text=True
            )
            if push.returncode != 0:
                context.log.error(f"gh-pages: push falló → {push.stderr}")
            else:
                context.log.info(f"✓ Imagen publicada en gh-pages: {Path(imagen_path).name}")


@asset(description="Genera mapa de indicadores laborales por municipio desde GeoJSON del ISTAC", group_name="canarias")
def mapa_renta_municipios(context: AssetExecutionContext):
    output_path = str(OUTPUT_DIR / "mapa_renta_municipios.png")
    geojson_path = DATA_DIR / "indicadores-laborales-municipios-canarias-2024.geojson"

    # Cargar GeoJSON
    gdf = gpd.read_file(geojson_path)
    context.log.info(f"✓ GeoJSON cargado: {len(gdf)} municipios")
    context.log.info(f"✓ Columnas disponibles: {list(gdf.columns)}")

    # Reproyectar a WGS84
    gdf = gdf.to_crs(epsg=4326)

    # Extraer coordenadas de polígonos como DataFrame largo
    filas = []
    for idx, row in gdf.iterrows():
        geom = row.geometry
        # ⚠️ Ajusta 'NOMBRE' y 'TASA_PARO' a los nombres reales del GeoJSON
        nombre = row.get('NOMBRE', row.get('nombre', str(idx)))
        valor = row.get('TASA_PARO', row.get('tasa_paro', None))
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
    context.log.info(f"✓ Coordenadas extraídas: {len(df_coords)} puntos")

    grafico = (
        ggplot(df_coords, aes(x='x', y='y', group='group', fill='valor'))
        + geom_polygon(color='white', size=0.2)
        + scale_fill_gradient(low='#d4e6f1', high='#1a5276', na_value='#cccccc')
        + coord_fixed()
        + labs(title='Indicadores Laborales por Municipio — Canarias 2024', fill='Valor')
        + theme_void()
        + theme(figure_size=(16, 10))
    )

    grafico.save(output_path, width=16, height=10, dpi=300)
    context.log.info("✓ Mapa guardado")

    subprocess.run(["git", "add", output_path])
    subprocess.run(["git", "commit", "-m", "Auto: mapa_renta_municipios actualizado"])
    subprocess.run(["git", "push"])
    subir_imagen_a_ghpages(output_path, context)

    return MaterializeResult(
        metadata={
            "output_path": MetadataValue.path(output_path),
            "url_publica": MetadataValue.url(
                "https://LauraLasso.github.io/dagster-canarias-analytics/mapa_renta_municipios.png"
            ),
            "municipios": MetadataValue.int(len(gdf))
        }
    )
