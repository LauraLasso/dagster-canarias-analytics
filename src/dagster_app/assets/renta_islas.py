"""
Pipeline 2: Análisis por isla - Top 5 municipios
Integra codislas.csv para gráfico de evolución temporal con nombres
"""

import pandas as pd
from plotnine import *
from dagster import asset, AssetExecutionContext, MaterializeResult, MetadataValue
from pathlib import Path

# Importar el asset compartido
from src.dagster_app.assets.renta_canarias import raw_renta_canarias

# Configuración
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
# DATA_DIR = Path("./data")
# OUTPUT_DIR = Path("./images/dagster")

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

# ============================================
# PIPELINE: TOP 5 MUNICIPIOS
# ============================================

@asset(
    description="Carga el catálogo de códigos de islas con nombres de municipios",
    group_name="islas"
)
def raw_codislas(context: AssetExecutionContext) -> pd.DataFrame:
    """Carga codislas.csv con mapping municipio→isla→nombre."""
    file_path = DATA_DIR / "codislas.csv"
    df = pd.read_csv(file_path, encoding='latin-1', sep=';')
    
    context.log.info(f"✓ Códigos cargados: {len(df)} registros")
    
    return df


@asset(
    description="Filtra municipios individuales de la tabla principal",
    group_name="islas"
)
def cleaned_municipios(
    context: AssetExecutionContext,
    raw_renta_canarias: pd.DataFrame
) -> pd.DataFrame:
    """Filtra solo códigos de 5 dígitos (municipios)."""
    df_municipios = raw_renta_canarias[
        raw_renta_canarias['TERRITORIO_CODE'].str.match(r'^\d{5}$', na=False)
    ].copy()
    
    context.log.info(f"✓ Municipios únicos: {df_municipios['TERRITORIO_CODE'].nunique()}")
    
    return df_municipios


@asset(
    description="Une datos de renta con nombres de islas y municipios",
    group_name="islas"
)
def municipios_con_islas(
    context: AssetExecutionContext,
    cleaned_municipios: pd.DataFrame,
    raw_codislas: pd.DataFrame
) -> pd.DataFrame:
    """Merge entre distribucion-renta-canarias.csv y codislas.csv."""
    # Preparar códigos
    df_codigos = raw_codislas.copy()
    df_codigos['TERRITORIO_CODE'] = (
        df_codigos['CPRO'].astype(str) + 
        df_codigos['CMUN'].astype(str).str.zfill(3)
    )
    df_codigos['ISLA'] = df_codigos['ISLA'].str.strip()
    df_codigos['NOMBRE'] = df_codigos['NOMBRE'].str.strip()
    
    # Merge
    df_merged = pd.merge(
        cleaned_municipios,
        df_codigos[['TERRITORIO_CODE', 'ISLA', 'NOMBRE']],
        on='TERRITORIO_CODE',
        how='left'
    )
    
    # Añadir nombres cortos de medidas
    df_merged['MEDIDA_CORTA'] = df_merged['MEDIDAS#es'].replace(MEDIDAS_DICT)
    
    context.log.info(f"✓ Total municipios: {df_merged['TERRITORIO_CODE'].nunique()}")
    context.log.info(f"✓ Municipios con isla: {df_merged['ISLA'].notna().sum()}")
    context.log.info(f"✓ Islas únicas: {df_merged['ISLA'].nunique()}")
    
    return df_merged


@asset(
    description="Selecciona top 5 municipios por sueldos en 2023",
    group_name="islas"
)
def top_5_municipios(
    context: AssetExecutionContext,
    municipios_con_islas: pd.DataFrame
) -> pd.DataFrame:
    """Identifica los 5 municipios con mayores sueldos en 2023."""
    df_valid = municipios_con_islas[municipios_con_islas['ISLA'].notna()].copy()
    
    # Ranking de municipios por sueldos 2023
    df_ranking = df_valid[
        (df_valid['TIME_PERIOD#es'] == 2023) & 
        (df_valid['MEDIDA_CORTA'] == 'Sueldos')
    ].copy()
    
    # Top 5
    top_5 = df_ranking.nlargest(5, 'OBS_VALUE')[
        ['TERRITORIO_CODE', 'NOMBRE', 'ISLA', 'OBS_VALUE']
    ]
    
    # Log del ranking
    context.log.info("Top 5 municipios (por sueldos 2023):")
    for _, row in top_5.iterrows():
        context.log.info(f"  {row['NOMBRE']:30} ({row['ISLA']:15}) - {row['OBS_VALUE']:,.0f}€")
    
    # Filtrar dataset completo con esos municipios
    df_viz = df_valid[
        df_valid['TERRITORIO_CODE'].isin(top_5['TERRITORIO_CODE'])
    ].copy()
    
    # Crear nombre completo para leyenda
    df_viz['MUNICIPIO_NOMBRE'] = df_viz['NOMBRE'] + ' (' + df_viz['ISLA'] + ')'
    
    context.log.info(f"✓ Datos filtrados: {len(df_viz)} registros")
    context.log.info(f"✓ Años: {sorted(df_viz['TIME_PERIOD#es'].unique())}")
    context.log.info(f"✓ Medidas: {sorted(df_viz['MEDIDA_CORTA'].unique())}")
    
    return df_viz


@asset(
    description="Genera gráfico de evolución temporal por medida para top 5 municipios",
    group_name="islas"
)
def grafico_top5_evolucion_medidas(
    context: AssetExecutionContext,
    top_5_municipios: pd.DataFrame
) -> MaterializeResult:
    """Visualización: Evolución temporal de todas las medidas para top 5 municipios."""
    output_path = OUTPUT_DIR / "grafico_top5_evolucion_medidas.png"
    
    df_viz = top_5_municipios.copy()
    
    # Generar gráfico
    grafico = (
        ggplot(df_viz, aes(x='TIME_PERIOD#es', y='OBS_VALUE', 
                           color='MUNICIPIO_NOMBRE', group='MUNICIPIO_NOMBRE')) +
        geom_line(size=1.2) +
        geom_point(size=3) +
        facet_wrap('~MEDIDA_CORTA', scales='free_y', ncol=2) +
        labs(
            title='Evolución Temporal de Ingresos en Top 5 Municipios de Canarias (2015-2023)',
            subtitle='Por tipo de medida - Municipios con mayores sueldos en 2023',
            x='Año',
            y='Valor (€)',
            color='Municipio'
        ) +
        scale_x_continuous(breaks=[2015, 2017, 2019, 2021, 2023]) +
        scale_y_continuous(labels=lambda l: [f'{int(v):,}' for v in l]) +
        scale_color_brewer(type='qual', palette='Set1') +
        theme_minimal() +
        theme(
            figure_size=(16, 10),
            plot_title=element_text(size=16, weight='bold'),
            plot_subtitle=element_text(size=12, style='italic'),
            legend_position='bottom',
            legend_text=element_text(size=10),
            legend_key_size=12,
            axis_text_x=element_text(angle=45, hjust=1, size=10),
            axis_text_y=element_text(size=10),
            strip_text=element_text(size=12, weight='bold'),
            strip_background=element_rect(fill='#e8e8e8')
        )
    )
    
    grafico.save(str(output_path), dpi=300)
    context.log.info(f"✓ Gráfico guardado: {output_path}")
    
    # Calcular estadísticas de crecimiento
    municipios_unicos = df_viz['TERRITORIO_CODE'].unique()
    crecimientos = {}
    
    for muni_code in municipios_unicos:
        muni_data = df_viz[
            (df_viz['TERRITORIO_CODE'] == muni_code) & 
            (df_viz['MEDIDA_CORTA'] == 'Sueldos')
        ]
        
        valor_2015 = muni_data[muni_data['TIME_PERIOD#es'] == 2015]['OBS_VALUE'].values
        valor_2023 = muni_data[muni_data['TIME_PERIOD#es'] == 2023]['OBS_VALUE'].values
        
        if len(valor_2015) > 0 and len(valor_2023) > 0:
            crecimiento = ((valor_2023[0] - valor_2015[0]) / valor_2015[0]) * 100
            nombre = muni_data['NOMBRE'].iloc[0]
            crecimientos[nombre] = crecimiento
            context.log.info(f"{nombre:30} | Crecimiento 2015-2023: {crecimiento:+.1f}%")
    
    # Calcular promedio de crecimiento
    crecimiento_promedio = sum(crecimientos.values()) / len(crecimientos) if crecimientos else 0
    
    return MaterializeResult(
        metadata={
            "output_path": MetadataValue.path(str(output_path)),
            "municipios_mostrados": MetadataValue.int(len(municipios_unicos)),
            "num_medidas": MetadataValue.int(int(df_viz['MEDIDA_CORTA'].nunique())),
            "periodo": MetadataValue.text("2015-2023"),
            "crecimiento_promedio_sueldos": MetadataValue.float(float(crecimiento_promedio)),
        }
    )
