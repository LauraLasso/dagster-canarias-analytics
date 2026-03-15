"""
Pipeline 3: Análisis integrado de nivel de estudios con renta e islas
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

CODIGO_FALLBACK_ESTUDIOS = """
from plotnine import ggplot, aes, geom_bar, scale_fill_brewer, theme, theme_minimal, labs, element_text

def generar_plot(df):
    df_2023 = df[df['AÑO'] == 2023].copy()
    plot = (ggplot(df_2023, aes(x='ISLA', y='PORCENTAJE', fill='NIVEL_CORTO'))
            + geom_bar(stat='identity', position='dodge')
            + scale_fill_brewer(type='qual', palette='Set2')
            + labs(title='Perfil Educativo por Isla en 2023', x='Isla',
                   y='Porcentaje de Población (%)', fill='Nivel Educativo')
            + theme_minimal()
            + theme(figure_size=(16, 8), axis_text_x=element_text(angle=45)))
    return plot
"""

NIVELES_VALIDOS = ['Primaria', 'ESO', 'Bachillerato', 'FP', 'Universidad']


@asset(description="Carga datos de niveles de estudios desde Excel", group_name="estudios")
def raw_nivel_estudios(context: AssetExecutionContext) -> pd.DataFrame:
    file_path = DATA_DIR / "nivelestudios.xlsx"
    if not file_path.exists():
        raise FileNotFoundError(f"No se encuentra: {file_path}")
    try:
        df = pd.read_excel(file_path, engine='openpyxl')
        context.log.info(f"✓ Datos cargados: {len(df)} registros")
        context.log.info(f"✓ Columnas: {list(df.columns)}")
        return df
    except ImportError:
        raise ImportError("Instala: pip install openpyxl")

@asset_check(asset="raw_nivel_estudios", description="Verifica que el Excel de estudios tiene registros y columnas clave")
def check_raw_nivel_estudios(raw_nivel_estudios: pd.DataFrame) -> AssetCheckResult:
    cols_requeridas = {'Municipios de 500 habitantes o más', 'Periodo', 'Sexo', 'Total'}
    faltantes = cols_requeridas - set(raw_nivel_estudios.columns)
    passed = len(faltantes) == 0 and len(raw_nivel_estudios) > 0
    return AssetCheckResult(
        passed=passed,
        metadata={
            "num_registros": MetadataValue.int(len(raw_nivel_estudios)),
            "columnas_faltantes": MetadataValue.text(str(faltantes) if faltantes else "Ninguna"),
        }
    )


@asset(description="Limpia y normaliza datos de nivel de estudios", group_name="estudios")
def cleaned_nivel_estudios(
    context: AssetExecutionContext,
    raw_nivel_estudios: pd.DataFrame
) -> pd.DataFrame:
    df = raw_nivel_estudios.copy()
    df['CODIGO_MUNICIPIO'] = df['Municipios de 500 habitantes o más'].str.extract(r'(\d{5})')
    df['NOMBRE_MUNICIPIO'] = df['Municipios de 500 habitantes o más'].str.replace(r'^\d{5}\s+', '', regex=True)
    df['AÑO'] = df['Periodo'].dt.year
    df = df.rename(columns={
        'Sexo': 'SEXO', 'Nacionalidad': 'NACIONALIDAD',
        'Nivel de estudios en curso': 'NIVEL_ESTUDIOS', 'Total': 'POBLACION'
    })
    df['NIVEL_CORTO'] = df['NIVEL_ESTUDIOS'].replace({
        'Educación primaria e inferior': 'Primaria o menos',
        'Primera etapa de Educación Secundaria y similar': 'ESO',
        'Segunda etapa de educación secundaria, con orientación general': 'Bachillerato',
        'Segunda etapa de Educación Secundaria, con orientación profesional (con y sin continuidad en la educación superior); Educación postsecundaria no superior': 'FP',
        'Educación superior': 'Universidad',
        'No cursa estudios': 'No cursa',
        'Cursa estudios pero no hay información sobre los mismos': 'Sin información',
        'Total': 'Total'
    })
    df = df[(df['POBLACION'] > 0) & (~df['NIVEL_CORTO'].isin(['Total', 'Sin información', 'No cursa']))].copy()
    context.log.info(f"✓ Registros limpios: {len(df)}")
    return df

@asset_check(asset="cleaned_nivel_estudios", description="Verifica que los 5 niveles educativos esperados están presentes tras la limpieza")
def check_cleaned_nivel_estudios(cleaned_nivel_estudios: pd.DataFrame) -> AssetCheckResult:
    niveles_presentes = set(cleaned_nivel_estudios['NIVEL_CORTO'].unique())
    niveles_esperados = {'Primaria o menos', 'ESO', 'Bachillerato', 'FP', 'Universidad'}
    faltantes = niveles_esperados - niveles_presentes
    passed = len(faltantes) == 0
    return AssetCheckResult(
        passed=passed,
        metadata={
            "niveles_presentes": MetadataValue.text(str(niveles_presentes)),
            "niveles_faltantes": MetadataValue.text(str(faltantes) if faltantes else "Ninguno"),
        }
    )


@asset(description="Calcula evolución temporal del perfil educativo por isla (2021-2023)", group_name="estudios")
def educacion_temporal_por_isla(
    context: AssetExecutionContext,
    cleaned_nivel_estudios: pd.DataFrame,
    municipios_con_islas: pd.DataFrame
) -> pd.DataFrame:
    df_estudios_temporal = cleaned_nivel_estudios[
        (cleaned_nivel_estudios['AÑO'].between(2021, 2023)) &
        (cleaned_nivel_estudios['SEXO'] == 'Total') &
        (cleaned_nivel_estudios['NIVEL_CORTO'].isin(['Primaria o menos', 'ESO', 'Bachillerato', 'FP', 'Universidad'])) &
        (cleaned_nivel_estudios['POBLACION'] > 0)
    ].copy()
    df_codislas = municipios_con_islas[['TERRITORIO_CODE', 'ISLA']].drop_duplicates()
    df_codislas['TERRITORIO_CODE'] = df_codislas['TERRITORIO_CODE'].astype(str).str.zfill(5)
    df_estudios_temporal['CODIGO_MUNICIPIO'] = df_estudios_temporal['CODIGO_MUNICIPIO'].astype(str).str.zfill(5)
    df_estudios_temporal = df_estudios_temporal.merge(
        df_codislas, left_on='CODIGO_MUNICIPIO', right_on='TERRITORIO_CODE', how='left'
    )
    df_estudios_temporal = df_estudios_temporal[df_estudios_temporal['ISLA'].notna()].copy()
    df_estudios_temporal['NIVEL_CORTO'] = df_estudios_temporal['NIVEL_CORTO'].replace({'Primaria o menos': 'Primaria'})
    df_edu_isla_agg = df_estudios_temporal.groupby(['ISLA', 'AÑO', 'NIVEL_CORTO'], as_index=False)['POBLACION'].sum()
    df_total = df_edu_isla_agg.groupby(['ISLA', 'AÑO'])['POBLACION'].sum().reset_index()
    df_total = df_total.rename(columns={'POBLACION': 'POBLACION_TOTAL'})
    df_edu_isla_agg = df_edu_isla_agg.merge(df_total, on=['ISLA', 'AÑO'])
    df_edu_isla_agg['PORCENTAJE'] = (df_edu_isla_agg['POBLACION'] / df_edu_isla_agg['POBLACION_TOTAL']) * 100
    context.log.info(f"✓ Islas: {sorted(df_edu_isla_agg['ISLA'].unique())}")
    context.log.info(f"✓ Años: {sorted(df_edu_isla_agg['AÑO'].unique())}")
    return df_edu_isla_agg

@asset_check(asset="educacion_temporal_por_isla", description="Verifica que los porcentajes por isla y año suman ~100% y hay datos de 2023")
def check_educacion_temporal_por_isla(educacion_temporal_por_isla: pd.DataFrame) -> AssetCheckResult:
    tiene_2023 = 2023 in educacion_temporal_por_isla['AÑO'].values
    totales = educacion_temporal_por_isla.groupby(['ISLA', 'AÑO'])['PORCENTAJE'].sum()
    fuera_rango = totales[(totales < 95) | (totales > 105)]
    passed = tiene_2023 and len(fuera_rango) == 0
    return AssetCheckResult(
        passed=passed,
        metadata={
            "tiene_datos_2023": MetadataValue.bool(tiene_2023),
            "grupos_fuera_rango_100pct": MetadataValue.int(len(fuera_rango)),
            "islas_disponibles": MetadataValue.text(str(sorted(educacion_temporal_por_isla['ISLA'].unique()))),
        }
    )


@asset(description="Prompt IA para gráfico de perfil educativo por isla", group_name="estudios")
def template_ia_estudios(context: AssetExecutionContext, educacion_temporal_por_isla: pd.DataFrame):
    columnas = ", ".join(educacion_temporal_por_isla.columns)
    islas = educacion_temporal_por_isla['ISLA'].unique().tolist()
    niveles = educacion_temporal_por_isla['NIVEL_CORTO'].unique().tolist()
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
    - Islas disponibles: {islas}
    - Niveles educativos: {niveles}
    - Filtrar solo filas donde AÑO == 2023.
    - Estéticas: 'ISLA' → eje X, 'PORCENTAJE' → eje Y, 'NIVEL_CORTO' → fill.
    - Geometría: geom_bar(stat='identity', position='dodge').
    - Etiquetas: Título 'Perfil Educativo por Isla en 2023'. Eje Y: 'Porcentaje de Población (%)'.
    - Escala: scale_fill_brewer(type='qual', palette='Set2'). type='qual' es OBLIGATORIO.
    - Tema: + theme_minimal() + theme(figure_size=(16, 8), axis_text_x=element_text(angle=45)).
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


@asset(description="Código Python generado por IA para gráfico educativo", group_name="estudios")
def codigo_generado_estudios(context: AssetExecutionContext, template_ia_estudios):
    url = "http://gpu1.esit.ull.es:4000/v1/chat/completions"
    headers = {"Authorization": "Bearer sk-1234"}
    try:
        response = requests.post(url, json=template_ia_estudios, headers=headers, timeout=60)
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
        codigo_final = CODIGO_FALLBACK_ESTUDIOS.strip()
    return Output(
        value=codigo_final,
        metadata={"codigo": MetadataValue.md(f"```python\n{codigo_final}\n```")}
    )

@asset_check(asset="codigo_generado_estudios", description="Verifica que el código generado contiene geom_bar y generar_plot")
def check_codigo_generado_estudios(codigo_generado_estudios) -> AssetCheckResult:
    tiene_funcion = "def generar_plot" in codigo_generado_estudios
    tiene_geom_bar = "geom_bar" in codigo_generado_estudios
    tiene_return = "return" in codigo_generado_estudios
    passed = tiene_funcion and tiene_geom_bar and tiene_return
    return AssetCheckResult(
        passed=passed,
        metadata={
            "tiene_generar_plot": MetadataValue.bool(tiene_funcion),
            "tiene_geom_bar": MetadataValue.bool(tiene_geom_bar),
            "tiene_return": MetadataValue.bool(tiene_return),
        }
    )


@asset(description="Ejecuta código IA y guarda gráfico de perfil educativo", group_name="estudios")
def viz_educacion_isla_2023(
    context: AssetExecutionContext,
    codigo_generado_estudios,
    educacion_temporal_por_isla: pd.DataFrame
):
    from plotnine import (ggplot, aes, geom_bar, scale_fill_brewer,
                          theme, theme_minimal, labs, element_text)
    import plotnine
    output_path = str(OUTPUT_DIR / "perfil_educativo_isla_2023.png")

    def grafico_limpio():
        df_2023 = educacion_temporal_por_isla[educacion_temporal_por_isla['AÑO'] == 2023].copy()
        return (
            ggplot(df_2023, aes(x='ISLA', y='PORCENTAJE', fill='NIVEL_CORTO'))
            + geom_bar(stat='identity', position='dodge')
            + scale_fill_brewer(type='qual', palette='Set2')
            + labs(title='Perfil Educativo por Isla en 2023', x='Isla',
                   y='Porcentaje de Población (%)', fill='Nivel Educativo')
            + theme_minimal()
            + theme(figure_size=(16, 8), axis_text_x=element_text(angle=45))
        )

    entorno = globals().copy()
    entorno['plotnine'] = plotnine
    entorno.update({k: v for k, v in plotnine.__dict__.items() if not k.startswith('_')})
    entorno['pd'] = pd

    grafico = None
    try:
        exec(codigo_generado_estudios, entorno)
        grafico = entorno['generar_plot'](educacion_temporal_por_isla)
        context.log.info("✓ Código IA ejecutado correctamente")
    except Exception as e_ia:
        context.log.warning(f"⚠ Código IA falló, usando fallback: {e_ia}")
        grafico = grafico_limpio()

    try:
        grafico.save(output_path, width=16, height=8, dpi=300)
        context.log.info("✓ Gráfico guardado correctamente")
    except Exception as e_save:
        context.log.warning(f"⚠ Save falló ({e_save}), reconstruyendo desde cero")
        grafico_limpio().save(output_path, width=16, height=8, dpi=300)

    subprocess.run(["git", "add", output_path])
    subprocess.run(["git", "commit", "-m", "Auto: perfil_educativo_isla_2023 actualizado"])
    subprocess.run(["git", "push"])
    return MaterializeResult(
        metadata={
            "output_path": MetadataValue.path(output_path),
            "mensaje": MetadataValue.text("Gráfico educativo generado")
        }
    )

@asset_check(asset="viz_educacion_isla_2023", description="Verifica que el PNG del perfil educativo fue generado y tiene tamaño razonable")
def check_viz_educacion_isla_2023() -> AssetCheckResult:
    output_path = OUTPUT_DIR / "perfil_educativo_isla_2023.png"
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
