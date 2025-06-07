# ETL_Warehouse_Pipeline

## Descripción del Pipeline ETL

La metodología Kimball fue seleccionada para este proyecto debido a su equilibrio entre simplicidad, rendimiento y facilidad de uso para análisis OLAP. A diferencia del enfoque One Big Table (OBT), que puede simplificar la estructura pero generar tablas enormes y poco manejables, Kimball organiza los datos en dimensiones y hechos, facilitando consultas rápidas y segmentadas. Frente al Data Vault, que es más complejo y enfocado en la integración y trazabilidad para entornos de data warehouse empresariales muy grandes, Kimball ofrece un diseño más accesible y fácil de implementar para proyectos de tamaño mediano y con requisitos analíticos claros. Por otro lado, el modelado normalizado, común en bases de datos transaccionales (OLTP), no es tan eficiente para consultas analíticas, ya que requiere múltiples joins complejos. En resumen, Kimball se eligió por su capacidad para optimizar el rendimiento en análisis multidimensional y su estructura intuitiva para usuarios finales, facilitando la exploración y generación de reportes de manera ágil.


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


