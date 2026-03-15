"""
Pipeline 2: Análisis por isla - Top 5 municipios
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

CODIGO_FALLBACK_ISLAS = """
from plotnine import ggplot, aes, geom_line, geom_point, facet_wrap, scale_color_manual, theme, theme_minimal, labs, element_text

def generar_plot(df):
    plot = (ggplot(df, aes(x='TIME_PERIOD#es', y='OBS_VALUE', color='MUNICIPIO_NOMBRE'))
            + geom_line(size=1.2)
            + geom_point(size=3)
            + facet_wrap('~MEDIDA_CORTA', scales='free_y', ncol=2)
            + labs(title='Evolución Top 5 Municipios de Canarias', x='Año', y='Valor (€)', color='Municipio')
            + theme_minimal()
            + theme(figure_size=(16, 10), axis_text_x=element_text(angle=45)))
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


@asset(description="Carga el catálogo de códigos de islas con nombres de municipios", group_name="islas")
def raw_codislas(context: AssetExecutionContext) -> pd.DataFrame:
    file_path = DATA_DIR / "codislas.csv"
    df = pd.read_csv(file_path, encoding='latin-1', sep=';')
    context.log.info(f"✓ Códigos cargados: {len(df)} registros")
    return df

@asset_check(asset="raw_codislas", description="Verifica columnas obligatorias y ausencia de nulos en codislas")
def check_raw_codislas(raw_codislas: pd.DataFrame) -> AssetCheckResult:
    cols_requeridas = {'CPRO', 'CMUN', 'ISLA', 'NOMBRE'}
    faltantes = cols_requeridas - set(raw_codislas.columns)
    nulos_isla = raw_codislas['ISLA'].isna().sum() if 'ISLA' in raw_codislas.columns else -1
    return AssetCheckResult(
        passed=bool(len(faltantes) == 0 and len(raw_codislas) > 0),  # ✅
        metadata={
            "columnas_faltantes": MetadataValue.text(str(faltantes) if faltantes else "Ninguna"),
            "num_registros": MetadataValue.int(len(raw_codislas)),
            "nulos_isla": MetadataValue.int(int(nulos_isla)),  # ✅
        }
    )

@asset(description="Filtra municipios individuales de la tabla principal", group_name="islas")
def cleaned_municipios(context: AssetExecutionContext, raw_renta_canarias: pd.DataFrame) -> pd.DataFrame:
    df_municipios = raw_renta_canarias[
        raw_renta_canarias['TERRITORIO_CODE'].str.match(r'^\d{5}$', na=False)
    ].copy()
    context.log.info(f"✓ Municipios únicos: {df_municipios['TERRITORIO_CODE'].nunique()}")
    return df_municipios

@asset_check(asset="cleaned_municipios", description="Verifica que se extraen municipios con código de 5 dígitos")
def check_cleaned_municipios(cleaned_municipios: pd.DataFrame) -> AssetCheckResult:
    n_municipios = int(cleaned_municipios['TERRITORIO_CODE'].nunique())  # ✅
    return AssetCheckResult(
        passed=bool(n_municipios >= 10),  # ✅
        metadata={
            "municipios_unicos": MetadataValue.int(n_municipios),
            "error": MetadataValue.text("Menos de 10 municipios detectados" if n_municipios < 10 else "OK"),
        }
    )


@asset(description="Une datos de renta con nombres de islas y municipios", group_name="islas")
def municipios_con_islas(
    context: AssetExecutionContext,
    cleaned_municipios: pd.DataFrame,
    raw_codislas: pd.DataFrame
) -> pd.DataFrame:
    df_codigos = raw_codislas.copy()
    df_codigos['TERRITORIO_CODE'] = (
        df_codigos['CPRO'].astype(str) +
        df_codigos['CMUN'].astype(str).str.zfill(3)
    )
    df_codigos['ISLA'] = df_codigos['ISLA'].str.strip()
    df_codigos['NOMBRE'] = df_codigos['NOMBRE'].str.strip()
    df_merged = pd.merge(
        cleaned_municipios,
        df_codigos[['TERRITORIO_CODE', 'ISLA', 'NOMBRE']],
        on='TERRITORIO_CODE', how='left'
    )
    df_merged['MEDIDA_CORTA'] = df_merged['MEDIDAS#es'].replace(MEDIDAS_DICT)
    context.log.info(f"✓ Total municipios: {df_merged['TERRITORIO_CODE'].nunique()}")
    context.log.info(f"✓ Municipios con isla: {df_merged['ISLA'].notna().sum()}")
    context.log.info(f"✓ Islas únicas: {df_merged['ISLA'].nunique()}")
    return df_merged

@asset_check(asset="municipios_con_islas", description="Verifica que el join no genera más del 10% de nulos en ISLA")
def check_municipios_con_islas(municipios_con_islas: pd.DataFrame) -> AssetCheckResult:
    pct_sin_isla = float(municipios_con_islas['ISLA'].isna().mean() * 100)  # ✅ float nativo
    return AssetCheckResult(
        passed=bool(pct_sin_isla < 10),  # ✅ bool nativo
        metadata={
            "pct_sin_isla": MetadataValue.float(round(pct_sin_isla, 2)),
            "islas_unicas": MetadataValue.int(int(municipios_con_islas['ISLA'].nunique())),
        }
    )



@asset(description="Selecciona top 5 municipios por sueldos en 2023", group_name="islas")
def top_5_municipios(context: AssetExecutionContext, municipios_con_islas: pd.DataFrame) -> pd.DataFrame:
    df_valid = municipios_con_islas[municipios_con_islas['ISLA'].notna()].copy()
    df_ranking = df_valid[
        (df_valid['TIME_PERIOD#es'] == 2023) &
        (df_valid['MEDIDA_CORTA'] == 'Sueldos')
    ].copy()
    top_5 = df_ranking.nlargest(5, 'OBS_VALUE')[['TERRITORIO_CODE', 'NOMBRE', 'ISLA', 'OBS_VALUE']]
    context.log.info("Top 5 municipios (por sueldos 2023):")
    for _, row in top_5.iterrows():
        context.log.info(f"  {row['NOMBRE']:30} ({row['ISLA']:15}) - {row['OBS_VALUE']:,.0f}€")
    df_viz = df_valid[df_valid['TERRITORIO_CODE'].isin(top_5['TERRITORIO_CODE'])].copy()
    df_viz['MUNICIPIO_NOMBRE'] = df_viz['NOMBRE'] + ' (' + df_viz['ISLA'] + ')'
    context.log.info(f"✓ Datos filtrados: {len(df_viz)} registros")
    return df_viz

@asset_check(asset="top_5_municipios", description="Verifica que se obtienen exactamente 5 municipios distintos")
def check_top_5_municipios(top_5_municipios: pd.DataFrame) -> AssetCheckResult:
    n = int(top_5_municipios['MUNICIPIO_NOMBRE'].nunique())  # ✅
    return AssetCheckResult(
        passed=bool(n == 5),  # ✅
        metadata={
            "municipios_seleccionados": MetadataValue.int(n),
            "municipios": MetadataValue.text(str(top_5_municipios['MUNICIPIO_NOMBRE'].unique().tolist())),
        }
    )


@asset(description="Prompt IA para gráfico Top 5 municipios", group_name="islas")
def template_ia_islas(context: AssetExecutionContext, top_5_municipios: pd.DataFrame):
    columnas = ", ".join(top_5_municipios.columns)
    municipios = top_5_municipios['MUNICIPIO_NOMBRE'].unique().tolist()
    PALETA = ['#007bff', '#e63946', '#2a9d8f', '#f4a261', '#8338ec']
    colores = {m: PALETA[i % len(PALETA)] for i, m in enumerate(municipios)}
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
    - Municipios disponibles: {municipios}
    - Estéticas:
        * Variable 'TIME_PERIOD#es' mapeada al eje X (label: 'Año').
        * Variable 'OBS_VALUE' mapeada al eje Y (label: 'Valor (€)').
        * Una línea por 'MUNICIPIO_NOMBRE' (color/group).
    - Geometría: geom_line(size=1.2) + geom_point(size=3).
    - Facetas: facet_wrap('~MEDIDA_CORTA', scales='free_y', ncol=2).
    - Etiquetas: Título 'Evolución Top 5 Municipios de Canarias'.
    - Colores: usar scale_color_manual(values={colores}).
    - Tema: escribir exactamente + theme_minimal() + theme(figure_size=(16, 10), axis_text_x=element_text(angle=45)).
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


@asset(description="Código Python generado por IA para gráfico de islas", group_name="islas")
def codigo_generado_islas(context: AssetExecutionContext, template_ia_islas):
    url = "http://gpu1.esit.ull.es:4000/v1/chat/completions"
    headers = {"Authorization": "Bearer sk-1234"}
    try:
        response = requests.post(url, json=template_ia_islas, headers=headers, timeout=60)
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
        codigo_final = CODIGO_FALLBACK_ISLAS.strip()
    return Output(
        value=codigo_final,
        metadata={"codigo": MetadataValue.md(f"```python\n{codigo_final}\n```")}
    )

@asset_check(asset="codigo_generado_islas", description="Verifica que el código generado contiene facet_wrap y generar_plot")
def check_codigo_generado_islas(codigo_generado_islas) -> AssetCheckResult:
    tiene_funcion = "def generar_plot" in codigo_generado_islas
    tiene_facet = "facet_wrap" in codigo_generado_islas
    tiene_return = "return" in codigo_generado_islas
    return AssetCheckResult(
        passed=bool(tiene_funcion and tiene_facet and tiene_return),  # ✅
        metadata={
            "tiene_generar_plot": MetadataValue.bool(tiene_funcion),
            "tiene_facet_wrap": MetadataValue.bool(tiene_facet),
            "tiene_return": MetadataValue.bool(tiene_return),
        }
    )


@asset(description="Ejecuta código IA y guarda gráfico Top 5 municipios", group_name="islas")
def grafico_top5_evolucion_medidas(
    context: AssetExecutionContext,
    codigo_generado_islas,
    top_5_municipios: pd.DataFrame
):
    from plotnine import (ggplot, aes, geom_line, geom_point, facet_wrap,
                          scale_color_manual, theme, theme_minimal, labs, element_text)
    import plotnine
    output_path = str(OUTPUT_DIR / "grafico_top5_evolucion_medidas.png")

    municipios = top_5_municipios['MUNICIPIO_NOMBRE'].unique().tolist()
    PALETA = ['#007bff', '#e63946', '#2a9d8f', '#f4a261', '#8338ec']
    colores = {m: PALETA[i % len(PALETA)] for i, m in enumerate(municipios)}
    context.log.info(f"Municipio focal: {municipios[0]}")
    context.log.info(f"Mapa de colores: {colores}")

    def grafico_limpio():
        return (
            ggplot(top_5_municipios, aes(x='TIME_PERIOD#es', y='OBS_VALUE', color='MUNICIPIO_NOMBRE'))
            + geom_line(size=1.2)
            + geom_point(size=3)
            + facet_wrap('~MEDIDA_CORTA', scales='free_y', ncol=2)
            + scale_color_manual(values=colores)
            + labs(title='Evolución Top 5 Municipios de Canarias',
                   x='Año', y='Valor (€)', color='Municipio')
            + theme_minimal()
            + theme(figure_size=(16, 10), axis_text_x=element_text(angle=45))
        )

    entorno = globals().copy()
    entorno['plotnine'] = plotnine
    entorno.update({k: v for k, v in plotnine.__dict__.items() if not k.startswith('_')})
    entorno['pd'] = pd

    grafico = None
    try:
        exec(codigo_generado_islas, entorno)
        grafico = entorno['generar_plot'](top_5_municipios)
        grafico = grafico + scale_color_manual(values=colores) + theme_minimal() + theme(figure_size=(16, 10))
        context.log.info("✓ Código IA ejecutado correctamente")
    except Exception as e_ia:
        context.log.warning(f"⚠ Código IA falló, usando fallback: {e_ia}")
        grafico = grafico_limpio()

    try:
        grafico.save(output_path, width=16, height=10, dpi=300)
        context.log.info("✓ Gráfico guardado correctamente")
    except Exception as e_save:
        context.log.warning(f"⚠ Save falló ({e_save}), reconstruyendo desde cero")
        grafico_limpio().save(output_path, width=16, height=10, dpi=300)

    subprocess.run(["git", "add", output_path])
    subprocess.run(["git", "commit", "-m", "Auto: grafico_top5 actualizado"])
    subprocess.run(["git", "push"])
    subir_imagen_a_ghpages(output_path, context)
    return MaterializeResult(
        metadata={
            "output_path": MetadataValue.path(output_path),
            "url_publica": MetadataValue.url(
                "https://LauraLasso.github.io/dagster-canarias-analytics/grafico_top5_evolucion_medidas.png"
            ),
            "municipio_focal": MetadataValue.text(municipios[0]),
            "mensaje": MetadataValue.text("Gráfico Top 5 generado")
        }
    )

@asset_check(asset="grafico_top5_evolucion_medidas", description="Verifica que el PNG del Top 5 fue generado y tiene tamaño razonable")
def check_grafico_top5_evolucion_medidas() -> AssetCheckResult:
    output_path = OUTPUT_DIR / "grafico_top5_evolucion_medidas.png"
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
