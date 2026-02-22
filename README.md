# Análisis de Renta y Educación - Canarias (Dagster Analytics)

Este proyecto utiliza **Dagster** para orquestar un pipeline de datos que analiza la correlación entre el nivel de estudios y la renta media por hogar en las Islas Canarias, utilizando datos del **ISTAC**.

## Estructura del Proyecto

El repositorio está organizado para separar la lógica de negocio de los datos y la configuración de infraestructura:

- **`src/dagster_app/`**: Núcleo de la aplicación.
    - **`assets/`**: Código Python con la lógica de extracción, transformación y generación de gráficos (Plotnine). En esta rama, se le añadieron checks a cada etapa correspondiente en cada .py, que generaban una visualización diferente.
    - **`definitions.py`**: Definición de los activos y recursos de Dagster.
- **`data/`**: Datasets originales en formato Excel (`.xlsx`).
- **`images/dagster/`**: Resultados visuales generados automáticamente por el pipeline.
- **`docs/`**: En esta carpeta se encuentran los enunciados de las prácticas 2 y 3, y el informe correspondiente a la práctica 3.
- **`tests/`**: En esta carpeta se encuentran los archivos .py empleados para responder las primeras preguntas del enunciado. Se usaron los archivos definitions.py y test_checks.py.
- **`.github/workflows/`**: Automatizaciones de Integración y Entrega Continua.

---

## Gestión de Ramas (GitFlow)

El proyecto sigue una estrategia de ramificación para garantizar la estabilidad:

* **`main`**: Rama de producción. Contiene el código estable y los resultados finales (imágenes y README). Está libre de workflows de ejecución para evitar ruidos.
* **`develop`**: Rama de desarrollo. Aquí reside la lógica de **Integración Continua**. Todos los experimentos y actualizaciones se realizan aquí antes de pasar a la rama principal.
* **`practica-calidad-checks`**: Rama de calidad (DataOps). En esta rama se implementan los Asset Checks de Dagster para validar la integridad de los datos de renta y educación, asegurando que las visualizaciones cumplan con los estándares de diseño y veracidad antes de su materialización.

---

## Automatización (CI/CD con GitHub Actions)

He implementado un sistema de calidad y ejecución basado en eventos, localizado exclusivamente en la rama `develop`:

### 1. Integración Continua (CI) - *Automático*
Cada vez que se realiza un `push` a la rama `develop`, se dispara un workflow que:
- Configura el entorno Python 3.12.
- Realiza un **Linting** (flake8) para asegurar que no hay errores de sintaxis.
- Ejecuta `dagster definitions validate` para garantizar que el grafo de assets es íntegro y funcional.

### 2. Entrega Continua (CD) - *Manual (Aprobación)*
Para optimizar recursos, la materialización de los assets (ejecución del código) se realiza bajo demanda:
- Se activa mediante el botón **"Run workflow"** en la pestaña Actions.
- **Materializa** todos los assets de Dagster.
- **Genera y guarda** los gráficos resultantes como **Artifacts** descargables dentro de GitHub.

---

## Instalación y Ejecución Local

1.  **Clonar el repositorio:**
    ```bash
    git clone [https://github.com/LauraLasso/dagster-canarias-analytics.git](https://github.com/LauraLasso/dagster-canarias-analytics.git)
    cd dagster-canarias-analytics
    ```

2.  **Instalar dependencias:**
    ```bash
    pip install dagster dagster-webserver pandas plotnine openpyxl
    ```

3.  **Lanzar interfaz de Dagster:**
    ```bash
    dagster dev
    ```

---

## Notas de Implementación
- Se ha configurado un archivo `.gitignore` robusto para evitar la subida de cachés de Python (`__pycache__`) y archivos temporales de OneDrive que podrían bloquear el flujo de trabajo.
- Las rutas de archivos se gestionan de forma relativa mediante `pathlib` para asegurar la compatibilidad entre Windows (local) y Linux (GitHub Actions).

**Autor:** Laura Lasso  
**Asignatura:** Visualización de Datos  
**Máster:** Ciberseguridad e Inteligencia de Datos