"""
Pipeline Dagster: Análisis de Renta en Canarias
"""

import re, requests, subprocess
import pandas as pd
from plotnine import *
from dagster import (asset, asset_check, AssetExecutionContext, AssetCheckResult,
                     MaterializeResult, MetadataValue, Output)
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "images" / "dagster"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

MEDIDAS_DICT = {
    'Sueldos y salarios': 'Sueldos',
    'Otros ingresos': 'Otros',
    'Otras prestaciones': 'Prestaciones',
    'Pensiones': 'Pensiones',
    'Prestaciones por desempleo': 'Desempleo'
}

CODIGO_FALLBACK_CANARIAS = """
from plotnine import ggplot, aes, geom_line, geom_point, scale_color_manual, theme, theme_minimal, labs

def generar_plot(df):
    plot = (ggplot(df, aes(x='TIME_PERIOD#es', y='OBS_VALUE', color='MEDIDA_CORTA'))
            + geom_line(size=1.2)
            + geom_point(size=3)
            + labs(title='Evolución de Ingresos en Canarias (2015-2023)', x='Año', y='Valor (€)', color='Medida')
            + theme_minimal()
            + theme(figure_size=(14, 8)))
    return plot
"""

def subir_imagen_a_ghpages(imagen_path: str, context):
    """Sube una imagen a la rama gh-pages para publicarla via GitHub Pages."""
    import shutil, tempfile
    repo_url = subprocess.run(
        ["git", "remote", "get-url", "origin"],
        capture_output=True, text=True
    ).stdout.strip()

    with tempfile.TemporaryDirectory() as tmpdir:
        subprocess.run(["git", "clone", "--branch", "gh-pages", "--single-branch", repo_url, tmpdir])
        destino = Path(tmpdir) / Path(imagen_path).name
        shutil.copy2(imagen_path, destino)
        subprocess.run(["git", "-C", tmpdir, "add", Path(imagen_path).name])
        result = subprocess.run(
            ["git", "-C", tmpdir, "commit", "-m", f"Auto: {Path(imagen_path).name} actualizado"],
            capture_output=True, text=True
        )
        if "nothing to commit" in result.stdout:
            context.log.info("gh-pages: imagen sin cambios, no se hace push")
        else:
            subprocess.run(["git", "-C", tmpdir, "push", "origin", "gh-pages"])
            context.log.info(f"✓ Imagen publicada en gh-pages: {Path(imagen_path).name}")


@asset(description="Carga el dataset de distribución de renta en Canarias desde CSV")
def raw_renta_canarias(context: AssetExecutionContext) -> pd.DataFrame:
    file_path = DATA_DIR / "distribucion-renta-canarias.csv"
    df = pd.read_csv(file_path)
    context.log.info(f"✓ Datos cargados: {len(df)} registros")
    context.log.info(f"✓ Columnas: {list(df.columns)}")
    context.log.info(f"✓ Códigos territoriales únicos: {df['TERRITORIO_CODE'].nunique()}")
    return df

@asset_check(asset="raw_renta_canarias", description="Verifica que el CSV tiene registros y columnas obligatorias")
def check_raw_renta_canarias(raw_renta_canarias: pd.DataFrame) -> AssetCheckResult:
    columnas_requeridas = {'TERRITORIO_CODE', 'MEDIDAS#es', 'TIME_PERIOD#es', 'OBS_VALUE'}
    cols_faltantes = columnas_requeridas - set(raw_renta_canarias.columns)
    if cols_faltantes:
        return AssetCheckResult(passed=False, metadata={"error": MetadataValue.text(f"Columnas faltantes: {cols_faltantes}")})
    if len(raw_renta_canarias) == 0:
        return AssetCheckResult(passed=False, metadata={"error": MetadataValue.text("El dataset está vacío")})
    nulos = raw_renta_canarias['OBS_VALUE'].isna().sum()
    return AssetCheckResult(
        passed=bool(nulos == 0),  # ✅ bool() nativo
        metadata={
            "num_registros": MetadataValue.int(len(raw_renta_canarias)),
            "nulos_obs_value": MetadataValue.int(int(nulos)),
        }
    )


@asset(description="Filtra datos agregados de Canarias (código ES70)")
def cleaned_canarias(context: AssetExecutionContext, raw_renta_canarias: pd.DataFrame) -> pd.DataFrame:
    df_canarias = raw_renta_canarias[raw_renta_canarias['TERRITORIO_CODE'] == 'ES70'].copy()
    context.log.info(f"✓ Registros Canarias (ES70): {len(df_canarias)}")
    return df_canarias

@asset_check(asset="cleaned_canarias", description="Verifica que existen datos para ES70")
def check_cleaned_canarias(cleaned_canarias: pd.DataFrame) -> AssetCheckResult:
    passed = bool(len(cleaned_canarias) > 0)  # ✅
    return AssetCheckResult(
        passed=passed,
        metadata={
            "registros_ES70": MetadataValue.int(len(cleaned_canarias)),
            "error": MetadataValue.text("No hay registros para ES70" if not passed else "OK")
        }
    )


@asset(description="Transforma y enriquece los datos de Canarias con nombres cortos")
def transformed_canarias(context: AssetExecutionContext, cleaned_canarias: pd.DataFrame) -> pd.DataFrame:
    df = cleaned_canarias.copy()
    df['MEDIDA_CORTA'] = df['MEDIDAS#es'].replace(MEDIDAS_DICT)
    context.log.info(f"✓ Periodo: {df['TIME_PERIOD#es'].min()}-{df['TIME_PERIOD#es'].max()}")
    context.log.info(f"✓ Medidas: {df['MEDIDA_CORTA'].nunique()}")
    return df

@asset_check(asset="transformed_canarias", description="Verifica que las 5 medidas esperadas están presentes")
def check_transformed_canarias(transformed_canarias: pd.DataFrame) -> AssetCheckResult:
    medidas_esperadas = set(MEDIDAS_DICT.values())
    medidas_presentes = set(transformed_canarias['MEDIDA_CORTA'].unique())
    faltantes = medidas_esperadas - medidas_presentes
    return AssetCheckResult(
        passed=bool(len(faltantes) == 0),  # ✅
        metadata={
            "medidas_presentes": MetadataValue.text(str(medidas_presentes)),
            "medidas_faltantes": MetadataValue.text(str(faltantes) if faltantes else "Ninguna"),
        }
    )


@asset(description="Prepara datos específicos para la visualización de evolución temporal")
def viz_data_canarias(context: AssetExecutionContext, transformed_canarias: pd.DataFrame) -> pd.DataFrame:
    df_viz = transformed_canarias[['TIME_PERIOD#es', 'MEDIDA_CORTA', 'OBS_VALUE']].copy()
    context.log.info(f"✓ Datos preparados: {len(df_viz)} registros")
    context.log.info(f"✓ Años únicos: {sorted(df_viz['TIME_PERIOD#es'].unique())}")
    return df_viz

@asset_check(asset="viz_data_canarias", description="Verifica rango temporal y ausencia de nulos")
def check_viz_data_canarias(viz_data_canarias: pd.DataFrame) -> AssetCheckResult:
    nulos = viz_data_canarias.isna().sum().sum()
    años = sorted(viz_data_canarias['TIME_PERIOD#es'].unique())
    return AssetCheckResult(
        passed=bool(nulos == 0 and len(años) >= 5),  # ✅
        metadata={
            "años_disponibles": MetadataValue.text(str(años)),
            "nulos_totales": MetadataValue.int(int(nulos)),
        }
    )


@asset(description="Construye el prompt para que la IA genere el gráfico de Canarias", group_name="canarias")
def template_ia_canarias(context: AssetExecutionContext, viz_data_canarias: pd.DataFrame):
    columnas = ", ".join(viz_data_canarias.columns)
    medidas = viz_data_canarias['MEDIDA_CORTA'].unique().tolist()
    colores = {m: '#007bff' if m == 'Sueldos' else '#D3D3D3' for m in medidas}
    template_tecnico = """
def generar_plot(df):
    # plot = (ggplot(df, aes(...)) + geom_...)
    # return plot
"""
    system_content = (
        "Eres un experto en la gramática de gráficos y Plotnine. "
        "Tu tarea es traducir descripciones en lenguaje natural a código ejecutable. "
        f"Usa siempre este template: {template_tecnico}. "
        "Devuelve exclusivamente el código Python."
    )
    descripcion_grafico = f"""
    - Dataset: df con columnas [{columnas}]
    - Medidas disponibles: {medidas}
    - Estéticas:
        * Variable 'TIME_PERIOD#es' mapeada al eje X (label: 'Año').
        * Variable 'OBS_VALUE' mapeada al eje Y (label: 'Valor (€)').
        * Una línea por cada valor de 'MEDIDA_CORTA' (color/group).
    - Geometría: geom_line(size=1.2) + geom_point(size=3).
    - Etiquetas: Título 'Evolución de Ingresos en Canarias (2015-2023)'.
    - Colores: usar scale_color_manual(values={colores}).
    - Tema: escribir exactamente + theme_minimal() + theme(figure_size=(14, 8)).
      IMPORTANTE: figure_size va SIEMPRE dentro de theme(), NUNCA dentro de theme_minimal().
    """
    return {
        "model": "ollama/llama3.1:8b",
        "messages": [
            {"role": "system", "content": system_content},
            {"role": "user", "content": f"Completa el template:\n{descripcion_grafico}"}
        ],
        "temperature": 0.1,
        "stream": False
    }


@asset(description="Llama al LLM y obtiene el código Python limpio para el gráfico de Canarias", group_name="canarias")
def codigo_generado_canarias(context: AssetExecutionContext, template_ia_canarias):
    url = "http://gpu1.esit.ull.es:4000/v1/chat/completions"
    headers = {"Authorization": "Bearer sk-1234"}
    try:
        response = requests.post(url, json=template_ia_canarias, headers=headers, timeout=60)
        response.raise_for_status()
        codigo_raw = response.json()['choices'][0]['message']['content']
        match = re.search(r"```python\s+(.*?)\s+```", codigo_raw, re.DOTALL)
        codigo_final = match.group(1) if match else "\n".join(
            l for l in codigo_raw.split("\n")
            if not l.strip().startswith("###") and not l.strip().startswith("-")
        )
        codigo_final = codigo_final.strip()
        context.log.info(f"✓ Código generado por IA:\n{codigo_final}")
    except requests.exceptions.ConnectionError as e:
        context.log.warning(f"⚠ Sin conexión al LLM, usando código fallback: {e}")
        codigo_final = CODIGO_FALLBACK_CANARIAS.strip()
    return Output(
        value=codigo_final,
        metadata={"codigo": MetadataValue.md(f"```python\n{codigo_final}\n```")}
    )

@asset_check(asset="codigo_generado_canarias", description="Verifica que el código generado contiene una función generar_plot válida")
def check_codigo_generado_canarias(codigo_generado_canarias) -> AssetCheckResult:
    tiene_funcion = "def generar_plot" in codigo_generado_canarias
    tiene_ggplot = "ggplot" in codigo_generado_canarias
    tiene_return = "return" in codigo_generado_canarias
    return AssetCheckResult(
        passed=bool(tiene_funcion and tiene_ggplot and tiene_return),  # ✅
        metadata={
            "tiene_generar_plot": MetadataValue.bool(tiene_funcion),
            "tiene_ggplot": MetadataValue.bool(tiene_ggplot),
            "tiene_return": MetadataValue.bool(tiene_return),
        }
    )


@asset(description="Ejecuta el código de la IA y guarda el gráfico de Canarias como PNG", group_name="canarias")
def grafico_evolucion_canarias(
    context: AssetExecutionContext,
    codigo_generado_canarias,
    viz_data_canarias: pd.DataFrame
):
    from plotnine import (ggplot, aes, geom_line, geom_point,
                          scale_color_manual, theme, theme_minimal, labs)
    output_path = str(OUTPUT_DIR / "grafico_evolucion_canarias.png")
    medidas = viz_data_canarias['MEDIDA_CORTA'].unique().tolist()
    PALETA = {
        'Sueldos':      '#007bff',
        'Pensiones':    '#ff7f0e',
        'Desempleo':    '#e74c3c',
        'Otros':        '#2ca02c',
        'Prestaciones': '#9467bd',
    }
    colores = {m: PALETA.get(m, '#888888') for m in medidas}

    def grafico_limpio():
        return (
            ggplot(viz_data_canarias, aes(x='TIME_PERIOD#es', y='OBS_VALUE', color='MEDIDA_CORTA'))
            + geom_line(size=1.2)
            + geom_point(size=3)
            + scale_color_manual(values=colores)
            + labs(title='Evolución de Ingresos en Canarias (2015-2023)',
                   x='Año', y='Valor (€)', color='Medida')
            + theme_minimal()
            + theme(figure_size=(14, 8))
        )

    entorno = globals().copy()
    entorno.update({k: v for k, v in __import__('plotnine').__dict__.items() if not k.startswith('_')})
    entorno['pd'] = pd

    grafico = None
    try:
        exec(codigo_generado_canarias, entorno)
        grafico = entorno['generar_plot'](viz_data_canarias)
        grafico = grafico + scale_color_manual(values=colores) + theme_minimal() + theme(figure_size=(14, 8))
        context.log.info("✓ Código IA ejecutado correctamente")
    except Exception as e_ia:
        context.log.warning(f"⚠ Código IA falló, usando fallback: {e_ia}")
        grafico = grafico_limpio()

    try:
        grafico.save(output_path, width=14, height=8, dpi=300)
        context.log.info("✓ Gráfico guardado correctamente")
    except Exception as e_save:
        context.log.warning(f"⚠ Save falló ({e_save}), reconstruyendo desde cero")
        grafico_limpio().save(output_path, width=14, height=8, dpi=300)

    subprocess.run(["git", "add", output_path])
    subprocess.run(["git", "commit", "-m", "Auto: grafico_evolucion_canarias actualizado"])
    subprocess.run(["git", "push"])
    subir_imagen_a_ghpages(output_path, context)
    return MaterializeResult(
        metadata={
            "output_path": MetadataValue.path(output_path),
            "url_publica": MetadataValue.url(
                "https://LauraLasso.github.io/dagster-canarias-analytics/grafico_evolucion_canarias.png"
            ),
            "mensaje": MetadataValue.text("Gráfico Canarias generado")
        }
    )

@asset_check(asset="grafico_evolucion_canarias", description="Verifica que el PNG fue generado y tiene tamaño razonable")
def check_grafico_evolucion_canarias() -> AssetCheckResult:
    output_path = OUTPUT_DIR / "grafico_evolucion_canarias.png"
    existe = output_path.exists()
    tamaño_kb = round(output_path.stat().st_size / 1024, 1) if existe else 0
    passed = existe and tamaño_kb > 10
    return AssetCheckResult(
        passed=passed,
        metadata={
            "archivo_existe": MetadataValue.bool(existe),
            "tamaño_kb": MetadataValue.float(tamaño_kb),
        }
    )
