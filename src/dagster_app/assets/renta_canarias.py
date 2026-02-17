"""
Pipeline Dagster: Análisis de Renta en Canarias
Convierte el prototipo en una cadena de assets interconectados
"""

import pandas as pd
from plotnine import *
from dagster import asset, AssetExecutionContext, MaterializeResult, MetadataValue
from pathlib import Path

# ============================================
# CONFIGURACIÓN
# ============================================

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
# ASSET 1: CARGA DE DATOS RAW
# ============================================

@asset(
    description="Carga el dataset de distribución de renta en Canarias desde CSV"
)
def raw_renta_canarias(context: AssetExecutionContext) -> pd.DataFrame:
    """
    Asset de carga: Lee el CSV completo de distribución de renta.
    
    Returns:
        DataFrame con todos los datos sin procesar
    """
    file_path = DATA_DIR / "distribucion-renta-canarias.csv"
    df = pd.read_csv(file_path)
    
    context.log.info(f"✓ Datos cargados: {len(df)} registros")
    context.log.info(f"✓ Columnas: {list(df.columns)}")
    context.log.info(f"✓ Códigos territoriales únicos: {df['TERRITORIO_CODE'].nunique()}")
    
    return df

# ============================================
# ASSET 2: LIMPIEZA - FILTRAR CANARIAS (ES70)
# ============================================

@asset(
    description="Filtra datos agregados de Canarias (código ES70)"
)
def cleaned_canarias(
    context: AssetExecutionContext,
    raw_renta_canarias: pd.DataFrame
) -> pd.DataFrame:
    """
    Asset de limpieza: Filtra solo el agregado de Canarias.
    
    Args:
        raw_renta_canarias: DataFrame completo
        
    Returns:
        DataFrame con solo datos de ES70
    """
    df_canarias = raw_renta_canarias[
        raw_renta_canarias['TERRITORIO_CODE'] == 'ES70'
    ].copy()
    
    context.log.info(f"✓ Registros Canarias (ES70): {len(df_canarias)}")
    
    return df_canarias

# ============================================
# ASSET 3: TRANSFORMACIÓN - ENRIQUECER CANARIAS
# ============================================

@asset(
    description="Transforma y enriquece los datos de Canarias con nombres cortos"
)
def transformed_canarias(
    context: AssetExecutionContext,
    cleaned_canarias: pd.DataFrame
) -> pd.DataFrame:
    """
    Asset de transformación: Añade columnas calculadas y limpia datos.
    
    Args:
        cleaned_canarias: DataFrame filtrado de Canarias
        
    Returns:
        DataFrame transformado con MEDIDA_CORTA
    """
    df = cleaned_canarias.copy()
    
    # Añadir nombres cortos de medidas
    df['MEDIDA_CORTA'] = df['MEDIDAS#es'].replace(MEDIDAS_DICT)
    
    # Calcular periodo
    primer_año = df['TIME_PERIOD#es'].min()
    ultimo_año = df['TIME_PERIOD#es'].max()
    
    context.log.info(f"✓ Periodo de análisis: {primer_año}-{ultimo_año}")
    context.log.info(f"✓ Medidas disponibles: {df['MEDIDA_CORTA'].nunique()}")
    
    return df

# ============================================
# ASSET 4: PREPARACIÓN - DATOS PARA VISUALIZACIÓN
# ============================================

@asset(
    description="Prepara datos específicos para la visualización de evolución temporal"
)
def viz_data_canarias(
    context: AssetExecutionContext,
    transformed_canarias: pd.DataFrame
) -> pd.DataFrame:
    """
    Asset de preparación: Selecciona columnas necesarias para el gráfico.
    
    Args:
        transformed_canarias: DataFrame transformado
        
    Returns:
        DataFrame con columnas [TIME_PERIOD#es, MEDIDA_CORTA, OBS_VALUE]
    """
    df_viz = transformed_canarias[['TIME_PERIOD#es', 'MEDIDA_CORTA', 'OBS_VALUE']].copy()
    
    context.log.info(f"✓ Datos preparados: {len(df_viz)} registros")
    context.log.info(f"✓ Años únicos: {sorted(df_viz['TIME_PERIOD#es'].unique())}")
    
    return df_viz

# ============================================
# ASSET 5: VISUALIZACIÓN - GRÁFICO EVOLUCIÓN CANARIAS
# ============================================

@asset(description="Genera gráfico de evolución temporal de ingresos en Canarias")
def grafico_evolucion_canarias(context: AssetExecutionContext, viz_data_canarias: pd.DataFrame) -> MaterializeResult:
    output_path = OUTPUT_DIR / "grafico_evolucion_canarias.png"
    
    grafico = (
        ggplot(viz_data_canarias, aes(x='TIME_PERIOD#es', y='OBS_VALUE', 
                                       color='MEDIDA_CORTA', group='MEDIDA_CORTA')) +
        geom_line(size=1.2) +
        geom_point(size=3) +
        labs(
            title='Evolución de Ingresos en Canarias (2015-2023)',
            subtitle='Datos agregados oficiales de Canarias (código ES70)',
            x='Año',
            y='Valor agregado (€)',
            color='Tipo de Ingreso'
        ) +
        scale_x_continuous(breaks=range(2015, 2024)) +
        scale_y_continuous(labels=lambda l: [f'{int(v):,}€' for v in l]) +
        scale_color_brewer(type='qual', palette='Set1') +
        theme_minimal() +
        theme(
            figure_size=(14, 8),
            plot_title=element_text(size=14, weight='bold'),
            legend_position='bottom'
        )
    )
    
    grafico.save(str(output_path), dpi=300)
    context.log.info(f"✓ Gráfico guardado: {output_path}")
    
    # Convertir valores a tipos Python nativos para metadata
    ultimo_año = int(viz_data_canarias['TIME_PERIOD#es'].max())
    primer_año = int(viz_data_canarias['TIME_PERIOD#es'].min())
    num_medidas = int(viz_data_canarias['MEDIDA_CORTA'].nunique())
    
    # Calcular crecimiento promedio (convertir a float Python)
    crecimientos = []
    for medida in viz_data_canarias['MEDIDA_CORTA'].unique():
        datos_medida = viz_data_canarias[
            viz_data_canarias['MEDIDA_CORTA'] == medida
        ].sort_values('TIME_PERIOD#es')
        valor_inicial = float(datos_medida.iloc[0]['OBS_VALUE'])  # ← Convertir a float
        valor_final = float(datos_medida.iloc[-1]['OBS_VALUE'])    # ← Convertir a float
        crecimiento = ((valor_final - valor_inicial) / valor_inicial) * 100
        crecimientos.append(crecimiento)
    
    crecimiento_promedio = float(sum(crecimientos) / len(crecimientos))  # ← Convertir a float
    
    return MaterializeResult(
        metadata={
            "output_path": MetadataValue.path(str(output_path)),
            "periodo": MetadataValue.text(f"{primer_año}-{ultimo_año}"),
            "num_medidas": MetadataValue.int(num_medidas),
            "registros_procesados": MetadataValue.int(len(viz_data_canarias)),
            "crecimiento_promedio": MetadataValue.float(round(crecimiento_promedio, 2)),
        }
    )

# ============================================
# RESUMEN DEL PIPELINE
# ============================================

"""
CADENA DE ASSETS:

1. raw_renta_canarias (CARGA)
   ↓
2. cleaned_canarias (LIMPIEZA)
   ↓
3. transformed_canarias (TRANSFORMACIÓN)
   ↓
4. viz_data_canarias (PREPARACIÓN)
   ↓
5. grafico_evolucion_canarias (VISUALIZACIÓN)

EJECUCIÓN:
- dagster dev
- Abre http://localhost:3000
- Materializa "grafico_evolucion_canarias"
- Dagster ejecutará automáticamente toda la cadena de dependencias
"""