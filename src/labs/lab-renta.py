import pandas as pd
from plotnine import *
from pathlib import Path

print("="*60)
print("PROTOTIPADO: Visualización de Renta en Canarias")
print("="*60)

# ============================================
# PASO 1: CARGA DE DATOS
# ============================================
BASE_DIR = Path(__file__).resolve().parent.parent.parent
OUTPUT_DIR = BASE_DIR / "images" / "test"

print("\n[PASO 1] Cargando datos...")
DATA_DIR = BASE_DIR / "data"
df_renta = pd.read_csv(f'{DATA_DIR}/distribucion-renta-canarias.csv')

print(f"   ✓ Datos cargados: {len(df_renta)} registros")
print(f"   ✓ Columnas: {list(df_renta.columns)}")

# ============================================
# PASO 2: LIMPIEZA Y PREPARACIÓN DE DATOS
# ============================================

print("\n[PASO 2] Limpieza y preparación de datos...")

# 2.1. Filtrar datos agregados de Canarias (ES70)
df_canarias = df_renta[df_renta['TERRITORIO_CODE'] == 'ES70'].copy()

# 2.2. Filtrar datos agregados de provincias (ES708, ES709)
df_provincias = df_renta[df_renta['TERRITORIO_CODE'].isin(['ES708', 'ES709'])].copy()

# 2.3. Filtrar municipios individuales (códigos de 5 dígitos)
df_municipios = df_renta[df_renta['TERRITORIO_CODE'].str.match(r'^\d{5}$', na=False)].copy()

print(f"   ✓ Registros Canarias (ES70): {len(df_canarias)}")
print(f"   ✓ Registros provincias (ES708, ES709): {len(df_provincias)}")
print(f"   ✓ Municipios individuales: {df_municipios['TERRITORIO_CODE'].nunique()}")

# ============================================
# PASO 3: TRANSFORMACIÓN Y ENRIQUECIMIENTO
# ============================================

print("\n[PASO 3] Transformación de datos...")

# 3.1. Crear nombres cortos para las medidas (aplicar a todos los DataFrames)
medidas_dict = {
    'Sueldos y salarios': 'Sueldos',
    'Otros ingresos': 'Otros',
    'Otras prestaciones': 'Prestaciones',
    'Pensiones': 'Pensiones',
    'Prestaciones por desempleo': 'Desempleo'
}

df_canarias['MEDIDA_CORTA'] = df_canarias['MEDIDAS#es'].replace(medidas_dict)
df_provincias['MEDIDA_CORTA'] = df_provincias['MEDIDAS#es'].replace(medidas_dict)
df_municipios['MEDIDA_CORTA'] = df_municipios['MEDIDAS#es'].replace(medidas_dict)

# 3.2. Mapear nombres de provincias
df_provincias['PROVINCIA'] = df_provincias['TERRITORIO_CODE'].map({
    'ES708': 'Las Palmas',
    'ES709': 'Santa Cruz de Tenerife'
})

# 3.3. Crear columnas auxiliares para municipios
df_municipios['provincia'] = df_municipios['TERRITORIO_CODE'].str[:2]
df_municipios['isla_temp'] = df_municipios['provincia'].map({
    '35': 'Provincia Las Palmas',
    '38': 'Provincia Santa Cruz de Tenerife'
})

# 3.4. Calcular periodo de análisis
ultimo_año = df_municipios['TIME_PERIOD#es'].max()
primer_año = df_municipios['TIME_PERIOD#es'].min()

print(f"   ✓ Periodo de análisis: {primer_año}-{ultimo_año}")
print(f"   ✓ Medidas disponibles: {len(df_canarias['MEDIDA_CORTA'].unique())}")

# ============================================
# PASO 4: PREPARACIÓN DE DATOS PARA VISUALIZACIÓN
# ============================================

print("\n[PASO 4] Preparando datos para visualización...")

# 4.1. Datos para Gráfico 1: Canarias agregado (ya preparado en df_canarias)
df_viz1 = df_canarias[['TIME_PERIOD#es', 'MEDIDA_CORTA', 'OBS_VALUE']].copy()

# 4.2. Datos para Gráfico 2: Provincias agregadas (ya preparado en df_provincias)
df_viz2 = df_provincias[['TIME_PERIOD#es', 'PROVINCIA', 'MEDIDA_CORTA', 'OBS_VALUE']].copy()

print(f"   ✓ Datos Gráfico 1 (Canarias): {len(df_viz1)} registros")
print(f"   ✓ Datos Gráfico 2 (Provincias): {len(df_viz2)} registros")

# ============================================
# PASO 5: VISUALIZACIÓN 1 - EVOLUCIÓN CANARIAS
# ============================================

print("\n[PASO 5] Generando visualizaciones...")
print("\n   → Gráfico 1: Evolución temporal Canarias...")

grafico1 = (ggplot(df_viz1, aes(x='TIME_PERIOD#es', y='OBS_VALUE', 
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
            ))

grafico1.save(f'{OUTPUT_DIR}/grafico1_evolucion_canarias.png', dpi=300)
print("      ✓ Guardado: grafico1_evolucion_canarias.png")

# ============================================
# PASO 6: VISUALIZACIÓN 2 - EVOLUCIÓN POR PROVINCIA
# ============================================

# print("\n   → Gráfico 2: Evolución temporal por provincia...")

# grafico2 = (ggplot(df_viz2, aes(x='TIME_PERIOD#es', y='OBS_VALUE', 
#                                  color='PROVINCIA', group='PROVINCIA')) +
#             geom_line(size=1.2) +
#             geom_point(size=2.5) +
#             facet_wrap('~MEDIDA_CORTA', scales='free_y', ncol=2) +
#             labs(
#                 title='Evolución de Ingresos por Provincia (2015-2023)',
#                 subtitle='Datos agregados oficiales por provincia (ES708 y ES709)',
#                 x='Año',
#                 y='Valor agregado (€)',
#                 color='Provincia'
#             ) +
#             scale_x_continuous(breaks=range(2015, 2024)) +
#             scale_color_manual(values=['#e74c3c', '#3498db']) +
#             theme_minimal() +
#             theme(
#                 figure_size=(16, 12),
#                 plot_title=element_text(size=14, weight='bold'),
#                 legend_position='bottom',
#                 strip_text=element_text(size=11, weight='bold')
#             ))

# grafico2.save('./images/test/grafico2_evolucion_provincias.png', dpi=300)
# print("      ✓ Guardado: grafico2_evolucion_provincias.png")

# ============================================
# PASO 7: ANÁLISIS Y ESTADÍSTICAS
# ============================================

# print("\n" + "="*60)
# print("ESTADÍSTICAS RESUMEN")
# print("="*60)

# print(f"\n Periodo analizado: {primer_año}-{ultimo_año}")
# print(f" Último año: {ultimo_año}")

# print(f"\n Crecimiento {primer_año}-{ultimo_año} en CANARIAS:")
# for medida in df_viz1['MEDIDA_CORTA'].unique():
#     datos_medida = df_viz1[df_viz1['MEDIDA_CORTA'] == medida].sort_values('TIME_PERIOD#es')
#     valor_inicial = datos_medida.iloc[0]['OBS_VALUE']
#     valor_final = datos_medida.iloc[-1]['OBS_VALUE']
#     crecimiento = ((valor_final - valor_inicial) / valor_inicial) * 100
#     print(f"   {medida:15s}: {crecimiento:+6.1f}%  ({valor_inicial:>10,.0f}€ → {valor_final:>10,.0f}€)")

# print(f"\n Crecimiento {primer_año}-{ultimo_año} por PROVINCIA:")
# for provincia in df_viz2['PROVINCIA'].unique():
#     print(f"\n   {provincia}:")
#     for medida in df_viz2['MEDIDA_CORTA'].unique():
#         datos = df_viz2[
#             (df_viz2['PROVINCIA'] == provincia) & 
#             (df_viz2['MEDIDA_CORTA'] == medida)
#         ].sort_values('TIME_PERIOD#es')
        
#         if len(datos) > 0:
#             valor_inicial = datos.iloc[0]['OBS_VALUE']
#             valor_final = datos.iloc[-1]['OBS_VALUE']
#             crecimiento = ((valor_final - valor_inicial) / valor_inicial) * 100
#             print(f"      {medida:15s}: {crecimiento:+6.1f}%")

# print("\n" + "="*60)
# print("✓ Pipeline completado exitosamente!")
# print("="*60)
# print("\nGráficos generados:")
# print("  1. Evolución temporal - Canarias (ES70)")
# print("  2. Evolución temporal - Provincias (ES708, ES709)")
# print("="*60)