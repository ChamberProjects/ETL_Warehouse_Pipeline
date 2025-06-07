# ETL_Warehouse_Pipeline

# Pipeline ETL con Modelo Dimensional Kimball para OLAP

Este proyecto implementa un pipeline ETL (Extract, Transform, Load) que procesa un conjunto de datos JSON comprimidos en un archivo ZIP, para transformarlos y cargarlos en una base de datos SQLite con un **modelo dimensional Kimball**, orientado a soportar análisis OLAP (Online Analytical Processing).

## Enfoque Kimball y OLAP

### ¿Qué es el modelo dimensional Kimball?

El modelo dimensional Kimball es una metodología para diseñar almacenes de datos (Data Warehouses) enfocados en facilitar consultas analíticas rápidas y eficientes. Este modelo organiza la información en **hechos** (fact tables) y **dimensiones** (dimensional tables), optimizando el acceso a los datos para análisis multidimensionales.

**Tabla de Hechos (Fact Table)**: Contiene medidas cuantitativas (por ejemplo, montos de transacciones, cantidades, totales) y referencias a las dimensiones.
**Tablas de Dimensiones (Dimension Tables)**: Contienen atributos descriptivos (por ejemplo, clientes, cuentas, fechas) usados para segmentar y filtrar los datos en análisis.

### ¿Por qué este pipeline ETL es Kimball?

1. **Separación clara de hechos y dimensiones**:
La tabla `fact_transactions` almacena las transacciones financieras, con métricas como cantidad, monto y totales.
Las tablas `dim_accounts`, `dim_customers` y `dim_dates` son dimensiones que describen cuentas, clientes y fechas respectivamente.
La tabla `account_customers` modela la relación entre clientes y cuentas (relación muchos a muchos).

2. **Generación de claves sustitutas**:
Para mantener integridad y facilitar el análisis, se crean IDs únicos generados internamente para cada dimensión (clientes, cuentas, fechas).
  
3. **Desnormalización controlada**:
Las dimensiones incluyen solo atributos relevantes para análisis, optimizando el rendimiento de las consultas OLAP.
  
4. **Preparación para análisis multidimensional**:
   Con esta estructura, es posible hacer consultas complejas para analizar, por ejemplo:
   Transacciones por cliente o cuenta.
   Evolución de montos a lo largo del tiempo.
   Comportamientos por productos o símbolos.

### ¿Cómo el pipeline soporta OLAP?

**Consultas rápidas y eficientes**: Al usar un esquema dimensional, las consultas de agregación, filtrado y segmentación se vuelven más intuitivas y performantes.
**Flexibilidad para análisis complejos**: La separación en dimensiones y hechos facilita cruzar diferentes perspectivas (clientes, fechas, productos) para obtener insights profundos.
**Facilidad de mantenimiento**: La estructura modular permite actualizar dimensiones y hechos de forma independiente, escalando el sistema según nuevas necesidades analíticas.



## Descripción del Pipeline ETL

### 1. Extracción (Extract)

Descomprime el archivo ZIP que contiene tres archivos JSON.
Lee y carga en memoria los datos de cuentas, clientes y transacciones.

### 2. Transformación (Transform)

**Dimensiones**:
Crea IDs únicos para cuentas, clientes y fechas.
Normaliza fechas para evitar duplicados y facilitar el filtrado.
Limpia y transforma formatos (fechas ISO, manejo de valores faltantes).

**Hechos**:
Mapea cada transacción con sus dimensiones asociadas.
Extrae medidas cuantitativas y atributos clave para análisis.

**Relaciones**:
Genera tabla puente para la relación N:M entre clientes y cuentas.

### 3. Carga (Load)

Crea tablas en SQLite siguiendo el modelo dimensional Kimball.
Inserta los datos transformados en sus respectivas tablas.
Muestra un resumen de filas cargadas para cada tabla.



## Estructura de las Tablas

| Tabla            | Descripción                                                  |
|------------------|--------------------------------------------------------------|
| `dim_accounts`   | Información descriptiva de las cuentas bancarias.             |
| `dim_customers`  | Datos personales y de identificación de los clientes.         |
| `account_customers` | Relación entre clientes y sus cuentas (muchos a muchos).    |
| `dim_dates`      | Fechas únicas normalizadas para análisis temporal.            |
| `fact_transactions` | Registros detallados de transacciones financieras.          |


## Uso

1. Colocar el archivo ZIP de datos en la ruta especificada en el script.
2. Ejecutar el script ETL para generar la base de datos SQLite con el modelo dimensional.
3. Utilizar la base de datos para realizar análisis OLAP mediante consultas SQL.


## Conclusión

Este pipeline ETL está diseñado para transformar datos transaccionales en un modelo dimensional Kimball, facilitando el análisis OLAP. Esto permite a los analistas y científicos de datos explorar y entender el comportamiento financiero desde múltiples perspectivas, optimizando la toma de decisiones basada en datos.


