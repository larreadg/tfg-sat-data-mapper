# TFG SAT Data Mapper

Mapeador de datos de calidad de agua desde planillas Excel hacia una base de datos PostgreSQL.

Este proyecto forma parte de una **Tesis de Ingeniería Informática**, cuyo objetivo mayor es construir un **Sistema de Alerta Temprana** para riesgo de contaminación en agua subterránea.

## Objetivo

Estandarizar datos históricos que vienen en distintos formatos de planillas (2001, 2006, 2010-2013, 2018) y cargarlos en un esquema único de base de datos para análisis posterior.

## Flujo de datos

Excel  
-> `pozos`  
-> `muestreos`  
-> `mediciones` (usando `param_alias` -> `param_code` de `param_catalog`)

## Estructura principal

- `db/db.sql`: esquema completo de PostgreSQL, catálogos, aliases y vistas.
- `db/index.js`: helpers de acceso a datos (`guardarPozo`, `guardarMuestreo`, `guardarMedicion`, etc.).
- `p2001.js`, `p2006.js`, `p2010_2013.js`, `p2018.js`: scripts de importación por fuente/año.
- `data/`: planillas Excel de entrada.

## Requisitos

- Node.js (ESM)
- PostgreSQL
- Variables de entorno en `.env`:
  - `DATABASE_URL=postgres://usuario:password@host:puerto/base`

## Instalación

```bash
npm install
```

## Uso

1. Crear esquema en PostgreSQL ejecutando `db/db.sql`.
2. Ejecutar el importador deseado:

```bash
node p2001.js
node p2006.js
node p2010_2013.js
node p2018.js
```

## Vistas útiles

- `vw_muestreo_mediciones_estandar`: vista normalizada muestreo x medición.
- `vw_muestreos_parametros_wide`: vista pivoteada (1 fila por muestreo, parámetros como columnas).

## Contexto académico

Este repositorio corresponde a la etapa de integración y normalización de datos del trabajo final de grado.  
Los datos cargados serán base para desarrollar modelos y reglas de un sistema de alerta temprana de contaminación.
