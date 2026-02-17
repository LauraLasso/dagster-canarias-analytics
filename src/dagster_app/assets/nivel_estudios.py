"""
Pipeline 3: Análisis integrado de nivel de estudios con renta e islas
Reutiliza assets de renta_canarias y renta_islas para análisis cruzado
"""

import pandas as pd
from plotnine import *
from dagster import asset, AssetExecutionContext, MaterializeResult, MetadataValue
from pathlib import Path

# Importar assets de otros pipelines
from src.dagster_app.assets.renta_canarias import raw_renta_canarias, transformed_canarias
from src.dagster_app.assets.renta_islas import municipios_con_islas

# Configuración
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
# DATA_DIR = Path("./data")
# OUTPUT_DIR = Path("./images/dagster")

DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "images" / "dagster"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ============================================
# CARGA Y PREPARACIÓN
# ============================================

@asset(
    description="Carga datos de niveles de estudios desde Excel",
    group_name="estudios"
)
def raw_nivel_estudios(context: AssetExecutionContext) -> pd.DataFrame:
    """Carga nivelestudios.xlsx"""
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


@asset(
    description="Limpia y normaliza datos de nivel de estudios",
    group_name="estudios"
)
def cleaned_nivel_estudios(
    context: AssetExecutionContext,
    raw_nivel_estudios: pd.DataFrame
) -> pd.DataFrame:
    """Procesa datos de estudios para integración"""
    df = raw_nivel_estudios.copy()
    
    # Extraer código de municipio y año
    df['CODIGO_MUNICIPIO'] = df['Municipios de 500 habitantes o más'].str.extract(r'(\d{5})')
    df['NOMBRE_MUNICIPIO'] = df['Municipios de 500 habitantes o más'].str.replace(r'^\d{5}\s+', '', regex=True)
    df['AÑO'] = df['Periodo'].dt.year
    
    # Renombrar columnas
    df = df.rename(columns={
        'Sexo': 'SEXO',
        'Nacionalidad': 'NACIONALIDAD',
        'Nivel de estudios en curso': 'NIVEL_ESTUDIOS',
        'Total': 'POBLACION'
    })
    
    # Categorizar nivel educativo
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
    
    # Filtrar
    df = df[
        (df['POBLACION'] > 0) & 
        (~df['NIVEL_CORTO'].isin(['Total', 'Sin información', 'No cursa']))
    ].copy()
    
    context.log.info(f"✓ Registros limpios: {len(df)}")
    return df


# ============================================
# INTEGRACIÓN DE DATOS
# ============================================

# @asset(
#     description="Integra estudios + renta + islas usando municipios_con_islas",
#     group_name="estudios"
# )
# def dataset_integrado(
#     context: AssetExecutionContext,
#     cleaned_nivel_estudios: pd.DataFrame,
#     municipios_con_islas: pd.DataFrame
# ) -> pd.DataFrame:
#     """
#     Combina nivel educativo con renta e isla.
#     municipios_con_islas ya integra distribucion-renta-canarias.csv + codislas.csv
#     """
    
#     # === 1. Preparar datos de estudios (último año) ===
#     df_estudios = cleaned_nivel_estudios[cleaned_nivel_estudios['AÑO'] == 2023].copy()
    
#     df_estudios_agg = df_estudios.groupby(
#         ['CODIGO_MUNICIPIO', 'NOMBRE_MUNICIPIO', 'NIVEL_CORTO'],
#         as_index=False
#     )['POBLACION'].sum()
    
#     # Calcular % universitarios
#     df_total = df_estudios_agg.groupby('CODIGO_MUNICIPIO')['POBLACION'].sum().reset_index()
#     df_total = df_total.rename(columns={'POBLACION': 'POBLACION_TOTAL'})
    
#     df_univ = df_estudios_agg[df_estudios_agg['NIVEL_CORTO'] == 'Universidad'].copy()
#     df_univ = df_univ[['CODIGO_MUNICIPIO', 'POBLACION']].rename(columns={'POBLACION': 'POBLACION_UNIV'})
    
#     df_estudios_final = df_total.merge(df_univ, on='CODIGO_MUNICIPIO', how='left')
#     df_estudios_final['POBLACION_UNIV'] = df_estudios_final['POBLACION_UNIV'].fillna(0)
#     df_estudios_final['PCT_UNIVERSITARIOS'] = (df_estudios_final['POBLACION_UNIV'] / df_estudios_final['POBLACION_TOTAL']) * 100
    
    
#     # === 2. Preparar datos de renta + isla ===
#     df_renta_isla = municipios_con_islas.copy()
    
#     # Ver qué columnas tiene realmente
#     context.log.info(f"✓ Columnas de municipios_con_islas: {list(df_renta_isla.columns)}")
    
#     # Calcular renta media por municipio e isla
#     df_renta_isla_agg = df_renta_isla.groupby(
#         ['TERRITORIO_CODE', 'ISLA'], 
#         as_index=False
#     )['OBS_VALUE'].mean()
    
#     df_renta_isla_agg = df_renta_isla_agg.rename(columns={
#         'TERRITORIO_CODE': 'CODIGO_MUNICIPIO',
#         'OBS_VALUE': 'RENTA_MEDIA'
#     })
    
#     # Asegurar formato de código (5 dígitos)
#     df_renta_isla_agg['CODIGO_MUNICIPIO'] = df_renta_isla_agg['CODIGO_MUNICIPIO'].astype(str).str.zfill(5)
#     df_estudios_final['CODIGO_MUNICIPIO'] = df_estudios_final['CODIGO_MUNICIPIO'].astype(str).str.zfill(5)
    
    
#     # === 3. Merge ===
#     df_integrado = df_estudios_final.merge(
#         df_renta_isla_agg,
#         on='CODIGO_MUNICIPIO',
#         how='inner'  # Solo municipios con datos completos
#     )
    
#     context.log.info(f"✓ Dataset integrado: {len(df_integrado)} municipios")
    
#     if len(df_integrado) > 0:
#         context.log.info(f"✓ Islas: {sorted(df_integrado['ISLA'].unique())}")
#         context.log.info(f"✓ Rango renta: {df_integrado['RENTA_MEDIA'].min():.0f}€ - {df_integrado['RENTA_MEDIA'].max():.0f}€")
#         context.log.info(f"✓ % Universitarios: {df_integrado['PCT_UNIVERSITARIOS'].min():.1f}% - {df_integrado['PCT_UNIVERSITARIOS'].max():.1f}%")
#     else:
#         context.log.warning("⚠️ No se encontraron coincidencias en el merge")
#         context.log.info(f"Muestra códigos estudios: {df_estudios_final['CODIGO_MUNICIPIO'].head().tolist()}")
#         context.log.info(f"Muestra códigos renta: {df_renta_isla_agg['CODIGO_MUNICIPIO'].head().tolist()}")
    
#     return df_integrado


@asset(
    description="Calcula evolución temporal del perfil educativo por isla (2021-2023)",
    group_name="estudios"
)
def educacion_temporal_por_isla(
    context: AssetExecutionContext,
    cleaned_nivel_estudios: pd.DataFrame,
    municipios_con_islas: pd.DataFrame
) -> pd.DataFrame:
    """Agrupa educación por isla, año y nivel educativo"""
    
    # Filtrar 2021-2023, Total sexo, niveles válidos
    df_estudios_temporal = cleaned_nivel_estudios[
        (cleaned_nivel_estudios['AÑO'].between(2021, 2023)) &
        (cleaned_nivel_estudios['SEXO'] == 'Total') &
        (cleaned_nivel_estudios['NIVEL_CORTO'].isin(['Primaria o menos', 'ESO', 'Bachillerato', 'FP', 'Universidad'])) &
        (cleaned_nivel_estudios['POBLACION'] > 0)
    ].copy()
    
    # Integrar isla (desde municipios_con_islas)
    df_codislas = municipios_con_islas[['TERRITORIO_CODE', 'ISLA']].drop_duplicates()
    df_codislas['TERRITORIO_CODE'] = df_codislas['TERRITORIO_CODE'].astype(str).str.zfill(5)
    
    df_estudios_temporal['CODIGO_MUNICIPIO'] = df_estudios_temporal['CODIGO_MUNICIPIO'].astype(str).str.zfill(5)
    
    df_estudios_temporal = df_estudios_temporal.merge(
        df_codislas,
        left_on='CODIGO_MUNICIPIO',
        right_on='TERRITORIO_CODE',
        how='left'
    )
    
    # Filtrar solo con isla válida
    df_estudios_temporal = df_estudios_temporal[df_estudios_temporal['ISLA'].notna()].copy()
    
    # Renombrar niveles cortos
    df_estudios_temporal['NIVEL_CORTO'] = df_estudios_temporal['NIVEL_CORTO'].replace({
        'Primaria o menos': 'Primaria'
    })
    
    # Agregar por ISLA, año y nivel educativo
    df_edu_isla_agg = df_estudios_temporal.groupby(
        ['ISLA', 'AÑO', 'NIVEL_CORTO'],
        as_index=False
    )['POBLACION'].sum()
    
    # Calcular % por nivel y año para cada isla
    df_total_edu_isla = df_edu_isla_agg.groupby(['ISLA', 'AÑO'])['POBLACION'].sum().reset_index()
    df_total_edu_isla = df_total_edu_isla.rename(columns={'POBLACION': 'POBLACION_TOTAL'})
    
    df_edu_isla_agg = df_edu_isla_agg.merge(df_total_edu_isla, on=['ISLA', 'AÑO'])
    df_edu_isla_agg['PORCENTAJE'] = (df_edu_isla_agg['POBLACION'] / df_edu_isla_agg['POBLACION_TOTAL']) * 100
    
    context.log.info(f"✓ Islas con datos educativos: {sorted(df_edu_isla_agg['ISLA'].unique())}")
    context.log.info(f"✓ Años educativos: {sorted(df_edu_isla_agg['AÑO'].unique())}")
    context.log.info(f"✓ Registros: {len(df_edu_isla_agg)}")
    
    return df_edu_isla_agg


# ============================================
# VISUALIZACIÓN ÚNICA
# ============================================

# @asset(
#     description="Gráfico integrador: Educación × Renta × Isla",
#     group_name="estudios"
# )
# def viz_integracion_completa(
#     context: AssetExecutionContext,
#     dataset_integrado: pd.DataFrame
# ) -> MaterializeResult:
#     """Scatter plot: % universitarios vs renta media por isla"""
#     output_path = OUTPUT_DIR / "integracion_estudios_renta_isla.png"
    
#     df = dataset_integrado.copy()
    
#     grafico = (
#         ggplot(df, aes(x='PCT_UNIVERSITARIOS', y='RENTA_MEDIA', color='ISLA', size='POBLACION_TOTAL')) +
#         geom_point(alpha=0.7) +
#         geom_smooth(method='lm', se=True, color='black', linetype='dashed', size=0.8, show_legend=False) +
#         labs(
#             title='Relación entre Nivel Educativo Universitario y Renta Media por Municipio',
#             subtitle='Integración: nivelestudios.xlsx + distribucion-renta-canarias.csv + codislas.csv (2023)',
#             x='% Población con Estudios Universitarios',
#             y='Renta Media del Municipio (€)',
#             color='Isla',
#             size='Población Total'
#         ) +
#         scale_color_brewer(type='qual', palette='Set2') +
#         scale_size_continuous(range=(3, 15), labels=lambda l: [f'{int(v):,}' for v in l]) +
#         theme_minimal() +
#         theme(
#             figure_size=(16, 10),
#             plot_title=element_text(size=15, weight='bold'),
#             plot_subtitle=element_text(size=11, style='italic'),
#             legend_position='right',
#             legend_box='vertical'
#         )
#     )
    
#     grafico.save(str(output_path), dpi=300)
#     context.log.info(f"✓ Gráfico guardado: {output_path}")
    
#     # Convertir numpy types a Python types
#     correlacion = float(df['PCT_UNIVERSITARIOS'].corr(df['RENTA_MEDIA']))  # ← Agregar float()
    
#     return MaterializeResult(
#         metadata={
#             "output_path": MetadataValue.path(str(output_path)),
#             "municipios_analizados": MetadataValue.int(int(len(df))),  # ← Ya estaba bien
#             "correlacion_educacion_renta": MetadataValue.float(correlacion),  # ← Ahora es float Python
#             "islas_representadas": MetadataValue.int(int(df['ISLA'].nunique())),
#         }
#     )


@asset(
    description="Gráfico: Perfil educativo por isla en 2023 (barras agrupadas)",
    group_name="estudios"
)
def viz_educacion_isla_2023(
    context: AssetExecutionContext,
    educacion_temporal_por_isla: pd.DataFrame
) -> MaterializeResult:
    """Barras agrupadas: comparación educativa 2023"""
    output_path = OUTPUT_DIR / "perfil_educativo_isla_2023.png"
    
    df_2023 = educacion_temporal_por_isla[educacion_temporal_por_isla['AÑO'] == 2023].copy()
    
    grafico = (
        ggplot(df_2023, aes(x='ISLA', y='PORCENTAJE', fill='NIVEL_CORTO')) +
        geom_bar(stat='identity', position='dodge') +
        labs(
            title='Perfil Educativo por Isla en 2023',
            subtitle='Distribución de población por nivel educativo',
            x='Isla',
            y='Porcentaje de Población (%)',
            fill='Nivel Educativo'
        ) +
        scale_fill_brewer(type='qual', palette='Set2') +
        theme_minimal() +
        theme(
            figure_size=(16, 8),
            plot_title=element_text(size=16, weight='bold'),
            plot_subtitle=element_text(size=12, style='italic'),
            legend_position='bottom',
            axis_text_x=element_text(angle=45, hjust=1, size=10)
        )
    )
    
    grafico.save(str(output_path), dpi=300)
    context.log.info(f"✓ Gráfico guardado: {output_path}")
    
    # Calcular isla con mayor % universitarios
    df_univ_2023 = df_2023[df_2023['NIVEL_CORTO'] == 'Universidad']
    isla_top_univ = df_univ_2023.loc[df_univ_2023['PORCENTAJE'].idxmax()]
    
    return MaterializeResult(
        metadata={
            "output_path": MetadataValue.path(str(output_path)),
            "islas_comparadas": MetadataValue.int(int(df_2023['ISLA'].nunique())),
            "isla_mayor_pct_universitarios": MetadataValue.text(str(isla_top_univ['ISLA'])),
            "pct_universitarios_max": MetadataValue.float(float(isla_top_univ['PORCENTAJE'])),
        }
    )