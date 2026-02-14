CREATE TABLE param_catalog (
    param_code      text PRIMARY KEY,              -- clave técnica única (ej: nitratos)
    param_name      text NOT NULL,                 -- nombre formal
    unidad_std      text,                          -- unidad estándar (mg/L, uS/cm, etc.)
    categoria       text NOT NULL CHECK (
                        categoria IN (
                            'microbiologico',
                            'nutrientes',
                            'metales_pesados',
                            'iones_mayoritarios',
                            'fisicoquimico',
                            'organicos',
                            'otros'
                        )
                    ),
    descripcion     text,
    activo          boolean NOT NULL DEFAULT true,
    creado_en       timestamptz NOT NULL DEFAULT now()
);


CREATE TABLE param_alias (
    alias           text PRIMARY KEY,              -- nombre exacto en Excel
    param_code      text NOT NULL REFERENCES param_catalog(param_code) ON DELETE CASCADE,
    fuente          text,                          -- opcional (2001, 2006, etc.)
    creado_en       timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX param_alias_param_idx ON param_alias(param_code);


CREATE TABLE pozos (
    pozo_id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),

    pozo_code       text,               -- código de fuente
    fuente_codigo   text,               -- de qué archivo proviene

    distrito        text,
    localidad       text,

    x               double precision,
    y               double precision,
    elevacion_m     double precision,
    profundidad_m   double precision,

    creado_en       timestamptz NOT NULL DEFAULT now(),
    actualizado_en  timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX uq_pozo_fuente_codigo
ON pozos(fuente_codigo, pozo_code)
WHERE pozo_code IS NOT NULL;

CREATE INDEX idx_pozo_xy ON pozos(x,y);



CREATE TABLE muestreos (
    muestreo_id     uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    pozo_id         uuid NOT NULL REFERENCES pozos(pozo_id) ON DELETE CASCADE,

    fuente          text NOT NULL,
    fecha_muestreo  date,
    anio            integer,

    creado_en       timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT uq_muestreo UNIQUE (pozo_id, fuente, fecha_muestreo, anio)
);

CREATE INDEX idx_muestreo_pozo ON muestreos(pozo_id);
CREATE INDEX idx_muestreo_anio ON muestreos(anio);



CREATE TABLE mediciones (
    muestreo_id     uuid NOT NULL REFERENCES muestreos(muestreo_id) ON DELETE CASCADE,
    param_code      text NOT NULL REFERENCES param_catalog(param_code),

    valor           double precision,
    valor_texto     text,                  -- por si viene "ND" o "<0.01"

    unidad_original text,                  -- unidad tal cual vino en Excel

    creado_en       timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (muestreo_id, param_code)
);

CREATE INDEX idx_mediciones_param ON mediciones(param_code);



CREATE TABLE evaluaciones_contaminacion (
    muestreo_id          uuid PRIMARY KEY REFERENCES muestreos(muestreo_id) ON DELETE CASCADE,

    -- baseline por reglas expert
    puntaje_contaminacion    integer,
    nivel_contaminacion      text CHECK (nivel_contaminacion IN ('Bajo','Medio','Alto')),
    version_reglas           text,

    -- salida red neuronal
    prob_contaminacion       double precision CHECK (
                                prob_contaminacion IS NULL
                                OR (prob_contaminacion >= 0 AND prob_contaminacion <= 1)
                             ),
    umbral_alerta            double precision CHECK (
                                umbral_alerta IS NULL
                                OR (umbral_alerta >= 0 AND umbral_alerta <= 1)
                             ),
    alerta                   boolean,

    version_modelo           text,
    evaluado_en              timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_eval_nivel ON evaluaciones_contaminacion(nivel_contaminacion);
CREATE INDEX idx_eval_alerta ON evaluaciones_contaminacion(alerta);


-- ============================================================
-- UMBRALES POR PARAMETRO (locales e internacionales)
-- ============================================================
ALTER TABLE param_catalog
ADD COLUMN IF NOT EXISTS umbral_local_min double precision,
ADD COLUMN IF NOT EXISTS umbral_local_max double precision,
ADD COLUMN IF NOT EXISTS fuente_umbral_local text,
ADD COLUMN IF NOT EXISTS nota_umbral_local text,
ADD COLUMN IF NOT EXISTS umbral_internacional_min double precision,
ADD COLUMN IF NOT EXISTS umbral_internacional_max double precision,
ADD COLUMN IF NOT EXISTS fuente_umbral_internacional text,
ADD COLUMN IF NOT EXISTS nota_umbral_internacional text;



CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.actualizado_en = now();
  RETURN NEW;
END;
$$;

CREATE TRIGGER trg_update_pozos
BEFORE UPDATE ON pozos
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

INSERT INTO param_catalog (param_code, param_name, unidad_std, categoria, descripcion)
VALUES
-- MICROBIOLOGICO
('coliformes_fecales', 'Coliformes Fecales', 'UFC/100 mL', 'microbiologico', 'Indicador microbiológico de contaminación fecal'),
('coliformes_totales', 'Coliformes Totales', 'UFC/100 mL', 'microbiologico', 'Indicador microbiológico general'),

-- NUTRIENTES
('nitratos', 'Nitratos (NO3-)', 'mg/L', 'nutrientes', 'Forma oxidada de nitrógeno'),
('nitritos', 'Nitritos (NO2-)', 'mg/L', 'nutrientes', 'Forma intermedia de nitrógeno'),
('amonio', 'Amonio (NH4+)', 'mg/L', 'nutrientes', 'Nitrógeno amoniacal'),
('nitrogeno_total', 'Nitrógeno Total', 'mg/L', 'nutrientes', 'Suma total de nitrógeno'),

-- FISICOQUIMICOS
('ph', 'pH', NULL, 'fisicoquimico', 'Potencial de hidrógeno'),
('conductividad', 'Conductividad Eléctrica', 'uS/cm', 'fisicoquimico', 'Indicador de sales disueltas'),
('temperatura', 'Temperatura', '°C', 'fisicoquimico', 'Temperatura del agua'),
('turbidez', 'Turbidez', 'NTU', 'fisicoquimico', 'Nivel de partículas suspendidas'),
('cloro_residual', 'Cloro Residual', 'mg/L', 'fisicoquimico', 'Cloro libre residual'),
('solidos_totales', 'Sólidos Totales', 'mg/L', 'fisicoquimico', 'Sólidos disueltos totales'),

-- ORGANICOS
('materia_organica', 'Materia Orgánica', 'mg/L', 'organicos', 'Materia orgánica presente'),

-- ALCALINIDAD / CARBONATOS
('alcalinidad_total', 'Alcalinidad Total', 'mg/L', 'iones_mayoritarios', 'Alcalinidad total'),
('alcalinidad_fenolftaleina', 'Alcalinidad Fenolftaleína', 'mg/L', 'iones_mayoritarios', 'Alcalinidad parcial'),
('bicarbonato', 'Bicarbonato (HCO3-)', 'mg/L', 'iones_mayoritarios', 'Ion bicarbonato'),
('carbonato', 'Carbonato (CO3-2)', 'mg/L', 'iones_mayoritarios', 'Ion carbonato'),

-- IONES MAYORITARIOS
('sulfato', 'Sulfato (SO4-2)', 'mg/L', 'iones_mayoritarios', 'Ion sulfato'),
('cloruro', 'Cloruro (Cl-)', 'mg/L', 'iones_mayoritarios', 'Ion cloruro'),
('dureza', 'Dureza Total', 'mg/L', 'iones_mayoritarios', 'Dureza del agua'),
('calcio', 'Calcio (Ca+2)', 'mg/L', 'iones_mayoritarios', 'Ion calcio'),
('magnesio', 'Magnesio (Mg+2)', 'mg/L', 'iones_mayoritarios', 'Ion magnesio'),
('sodio', 'Sodio (Na+)', 'mg/L', 'iones_mayoritarios', 'Ion sodio'),
('potasio', 'Potasio (K+)', 'mg/L', 'iones_mayoritarios', 'Ion potasio'),
('hierro_total', 'Hierro Total', 'mg/L', 'iones_mayoritarios', 'Hierro total disuelto'),
('fluoruro', 'Fluoruro (F-)', 'mg/L', 'iones_mayoritarios', 'Ion fluoruro'),

-- METALES PESADOS
('arsenico', 'Arsénico', 'mg/L', 'metales_pesados', 'Metal pesado tóxico'),
('mercurio', 'Mercurio', 'mg/L', 'metales_pesados', 'Metal pesado tóxico'),
('manganeso', 'Manganeso', 'mg/L', 'metales_pesados', 'Metal traza'),
('cobre', 'Cobre', 'mg/L', 'metales_pesados', 'Metal traza'),
('cromo_total', 'Cromo Total', 'mg/L', 'metales_pesados', 'Metal pesado')
ON CONFLICT (param_code) DO NOTHING;


-- ===== 2001 =====
INSERT INTO param_alias (alias, param_code, fuente) VALUES
('PH', 'ph', 'GA_calidad_2001'),
('CE_uScm', 'conductividad', 'GA_calidad_2001'),
('T_C', 'temperatura', 'GA_calidad_2001'),
('Turbidez_U', 'turbidez', 'GA_calidad_2001'),
('Cl_res_mg', 'cloro_residual', 'GA_calidad_2001'),
('Col_fecal', 'coliformes_fecales', 'GA_calidad_2001'),
('Col_total', 'coliformes_totales', 'GA_calidad_2001'),
('NITRATO (mg/l)', 'nitratos', 'GA_calidad_2001'),
('NITRITO (mg/l)', 'nitritos', 'GA_calidad_2001'),
('N-NH4+ (mg/l)', 'amonio', 'GA_calidad_2001'),
('SO4-2 (mg/l)', 'sulfato', 'GA_calidad_2001'),
('Cl-  (mg/l)', 'cloruro', 'GA_calidad_2001'),
('AlcT  (mg/l)', 'alcalinidad_total', 'GA_calidad_2001'),
('AlcF  (mg/l)', 'alcalinidad_fenolftaleina', 'GA_calidad_2001'),
('HCO3-  (mg/l)', 'bicarbonato', 'GA_calidad_2001'),
('Dureza  (mg/l)', 'dureza', 'GA_calidad_2001'),
('Ca+2  (mg/l)', 'calcio', 'GA_calidad_2001'),
('Mg+2  (mg/l)', 'magnesio', 'GA_calidad_2001'),
('Na+  (mg/l)', 'sodio', 'GA_calidad_2001'),
('K+  (mg/l)', 'potasio', 'GA_calidad_2001'),
('Fetotal (mg/l)', 'hierro_total', 'GA_calidad_2001'),
('ST mgl', 'solidos_totales', 'GA_calidad_2001'),
('Oxcons m', 'materia_organica', 'GA_calidad_2001')
ON CONFLICT (alias) DO NOTHING;

-- ===== 2006 =====
INSERT INTO param_alias (alias, param_code, fuente) VALUES
('pH', 'ph', 'GA_calidad_tesis_2006'),
('Conductividad (us/cm)', 'conductividad', 'GA_calidad_tesis_2006'),
('Temperatura (C) ', 'temperatura', 'GA_calidad_tesis_2006'),
('Colifor_ F (UFC/100 ml)', 'coliformes_fecales', 'GA_calidad_tesis_2006'),
('Colifor_ T (UFC/100 ml)', 'coliformes_totales', 'GA_calidad_tesis_2006'),
('Nitritos (mg/l)', 'nitritos', 'GA_calidad_tesis_2006'),
('Nitratos (mg/l)', 'nitratos', 'GA_calidad_tesis_2006'),
('N-Amoniacal (mg/l)', 'amonio', 'GA_calidad_tesis_2006'),
('N-Nitrito (mg/l)', 'nitritos', 'GA_calidad_tesis_2006'),
('N-Nitratos (mg/l)', 'nitratos', 'GA_calidad_tesis_2006'),
('N-NitrogAmoniacal (mg/l)', 'amonio', 'GA_calidad_tesis_2006'),
('N-Total (mg/l)', 'nitrogeno_total', 'GA_calidad_tesis_2006')
ON CONFLICT (alias) DO NOTHING;

-- ===== 2010-2013 =====
INSERT INTO param_alias (alias, param_code, fuente) VALUES
('NO3', 'nitratos', 'GA_calidad_2010_2013'),
('FeTotal', 'hierro_total', 'GA_calidad_2010_2013'),
('Colif_Feca', 'coliformes_fecales', 'GA_calidad_2010_2013'),
('Colif_Tot', 'coliformes_totales', 'GA_calidad_2010_2013')
ON CONFLICT (alias) DO NOTHING;

-- ===== 2018 =====
INSERT INTO param_alias (alias, param_code, fuente) VALUES
('pH', 'ph', 'GA_calidad_FP_2018'),
('Conductividad (uS/cm)', 'conductividad', 'GA_calidad_FP_2018'),
('N-Nitritos (mg/L)', 'nitritos', 'GA_calidad_FP_2018'),
('N-Nitratos (mg/L)', 'nitratos', 'GA_calidad_FP_2018'),
('N-Amoniacal (mg/L)', 'amonio', 'GA_calidad_FP_2018'),
('Alcalinidad Total (mg/L)', 'alcalinidad_total', 'GA_calidad_FP_2018'),
('Materia Organica (mg/L)', 'materia_organica', 'GA_calidad_FP_2018'),
('Bicarbonato (mg/L)', 'bicarbonato', 'GA_calidad_FP_2018'),
('Carbonato (mg/L)', 'carbonato', 'GA_calidad_FP_2018'),
('Sulfato  (mg/L)', 'sulfato', 'GA_calidad_FP_2018'),
('Magnesio  (mg/L)', 'magnesio', 'GA_calidad_FP_2018'),
('Calcio  (mg/L)', 'calcio', 'GA_calidad_FP_2018'),
('Sodio  (mg/L)', 'sodio', 'GA_calidad_FP_2018'),
('Potasio  (mg/L)', 'potasio', 'GA_calidad_FP_2018'),
('Cloruro  (mg/L)', 'cloruro', 'GA_calidad_FP_2018'),
('Arsenico  (mg/L)', 'arsenico', 'GA_calidad_FP_2018'),
('Mercurio  (mg/L)', 'mercurio', 'GA_calidad_FP_2018'),
('Manganeso  (mg/L)', 'manganeso', 'GA_calidad_FP_2018'),
('Cobre  (mg/L)', 'cobre', 'GA_calidad_FP_2018'),
('Cromo Total  (mg/L)', 'cromo_total', 'GA_calidad_FP_2018'),
('Coliformes Fecales  (UFC/100 mL)', 'coliformes_fecales', 'GA_calidad_FP_2018')
ON CONFLICT (alias) DO NOTHING;


-- ============================================================
-- UMBRALES LOCALES PARAGUAY + REFERENCIA INTERNACIONAL (OPS/OMS)
-- Fuente: Ley 1614 ERSSAN, Anexo III, pp. 51-53
-- Titulo: "LIMITES DE CALIDAD DE AGUA POTABLE – FRECUENCIA DE MUESTREOS MINIMOS"
-- ============================================================

-- Turbidez: admisible 5 UNT; recomendado <1 UNT
UPDATE param_catalog
SET
    umbral_local_max = 5,
    fuente_umbral_local = 'Ley 1614 ERSSAN, Anexo III, p.51',
    nota_umbral_local = 'Turbiedad (1): 95% del tiempo',
    umbral_internacional_max = 1,
    fuente_umbral_internacional = 'Guías OPS/OMS (referenciadas en Ley 1614 ERSSAN, Anexo III, p.51)',
    nota_umbral_internacional = 'De preferencia <1 UNT'
WHERE param_code = 'turbidez';

-- pH (pozos): 6.5 a 8.5
UPDATE param_catalog
SET
    umbral_local_min = 6.5,
    umbral_local_max = 8.5,
    fuente_umbral_local = 'Ley 1614 ERSSAN, Anexo III, p.51',
    nota_umbral_local = 'PH (Pozos) (3): 90% del tiempo',
    umbral_internacional_min = 6.5,
    umbral_internacional_max = 8.5,
    fuente_umbral_internacional = 'Guías OPS/OMS (referenciadas en Ley 1614 ERSSAN, Anexo III, p.51)',
    nota_umbral_internacional = 'Mismo rango en tabla'
WHERE param_code = 'ph';

-- Conductividad: admisible 1250 uS/cm; recomendado 400 uS/cm
UPDATE param_catalog
SET
    umbral_local_max = 1250,
    fuente_umbral_local = 'Ley 1614 ERSSAN, Anexo III, p.51',
    umbral_internacional_max = 400,
    fuente_umbral_internacional = 'Guías OPS/OMS (referenciadas en Ley 1614 ERSSAN, Anexo III, p.51)'
WHERE param_code = 'conductividad';

-- Calcio: admisible 100 mg/L; recomendado 100 mg/L
UPDATE param_catalog
SET
    umbral_local_max = 100,
    fuente_umbral_local = 'Ley 1614 ERSSAN, Anexo III, p.51',
    umbral_internacional_max = 100,
    fuente_umbral_internacional = 'Guías OPS/OMS (referenciadas en Ley 1614 ERSSAN, Anexo III, p.51)'
WHERE param_code = 'calcio';

-- Magnesio: admisible 50 mg/L; recomendado 30 mg/L
UPDATE param_catalog
SET
    umbral_local_max = 50,
    fuente_umbral_local = 'Ley 1614 ERSSAN, Anexo III, p.51',
    umbral_internacional_max = 30,
    fuente_umbral_internacional = 'Guías OPS/OMS (referenciadas en Ley 1614 ERSSAN, Anexo III, p.51)'
WHERE param_code = 'magnesio';

-- Potasio: admisible 12 mg/L; recomendado 10 mg/L
UPDATE param_catalog
SET
    umbral_local_max = 12,
    fuente_umbral_local = 'Ley 1614 ERSSAN, Anexo III, p.51',
    umbral_internacional_max = 10,
    fuente_umbral_internacional = 'Guías OPS/OMS (referenciadas en Ley 1614 ERSSAN, Anexo III, p.51)'
WHERE param_code = 'potasio';

-- Alcalinidad total (como CaCO3): admisible 250 mg/L; recomendado 120 mg/L
UPDATE param_catalog
SET
    umbral_local_max = 250,
    fuente_umbral_local = 'Ley 1614 ERSSAN, Anexo III, p.51',
    nota_umbral_local = 'Alcalinidad (M) en CaCO3',
    umbral_internacional_max = 120,
    fuente_umbral_internacional = 'Guías OPS/OMS (referenciadas en Ley 1614 ERSSAN, Anexo III, p.51)',
    nota_umbral_internacional = 'Alcalinidad (M) en CaCO3'
WHERE param_code = 'alcalinidad_total';

-- Cloro residual libre: admisible 2.0 mg/L; recomendado 0.20 a 0.50 mg/L
UPDATE param_catalog
SET
    umbral_local_max = 2.0,
    fuente_umbral_local = 'Ley 1614 ERSSAN, Anexo III, p.51',
    nota_umbral_local = 'Sujeto a calidad bacteriológica en punto de suministro',
    umbral_internacional_min = 0.20,
    umbral_internacional_max = 0.50,
    fuente_umbral_internacional = 'Guías OPS/OMS (referenciadas en Ley 1614 ERSSAN, Anexo III, p.51)'
WHERE param_code = 'cloro_residual';

-- Dureza total (como CaCO3): admisible 400 mg/L; recomendado 250 mg/L
UPDATE param_catalog
SET
    umbral_local_max = 400,
    fuente_umbral_local = 'Ley 1614 ERSSAN, Anexo III, p.51',
    nota_umbral_local = 'Dureza total en CaCO3',
    umbral_internacional_max = 250,
    fuente_umbral_internacional = 'Guías OPS/OMS (referenciadas en Ley 1614 ERSSAN, Anexo III, p.51)',
    nota_umbral_internacional = 'Dureza total en CaCO3'
WHERE param_code = 'dureza';

-- Solidos totales disueltos (STD): admisible 1000 mg/L; recomendado 1000 mg/L
UPDATE param_catalog
SET
    umbral_local_max = 1000,
    fuente_umbral_local = 'Ley 1614 ERSSAN, Anexo III, p.51',
    nota_umbral_local = 'STD',
    umbral_internacional_max = 1000,
    fuente_umbral_internacional = 'Guías OPS/OMS (referenciadas en Ley 1614 ERSSAN, Anexo III, p.51)',
    nota_umbral_internacional = 'STD'
WHERE param_code = 'solidos_totales';

-- Arsenico: admisible 0.5 mg/L; recomendado 0 mg/L (segun tabla)
UPDATE param_catalog
SET
    umbral_local_max = 0.5,
    fuente_umbral_local = 'Ley 1614 ERSSAN, Anexo III, p.52',
    umbral_internacional_max = 0,
    fuente_umbral_internacional = 'Guías OPS/OMS (referenciadas en Ley 1614 ERSSAN, Anexo III, p.52)'
WHERE param_code = 'arsenico';

-- Nitratos (NO3): admisible 45 mg/L; recomendado 0 mg/L (segun tabla)
UPDATE param_catalog
SET
    umbral_local_max = 45,
    fuente_umbral_local = 'Ley 1614 ERSSAN, Anexo III, p.52',
    nota_umbral_local = 'Con nota del MSPBS para casos excepcionales (1)',
    umbral_internacional_max = 0,
    fuente_umbral_internacional = 'Guías OPS/OMS (referenciadas en Ley 1614 ERSSAN, Anexo III, p.52)'
WHERE param_code = 'nitratos';

-- Coliformes fecales: admisible 0; recomendado 0
UPDATE param_catalog
SET
    umbral_local_max = 0,
    fuente_umbral_local = 'Ley 1614 ERSSAN, Anexo III, p.53',
    nota_umbral_local = 'Metodo membrana filtrante (UFC/100ml)',
    umbral_internacional_max = 0,
    fuente_umbral_internacional = 'Guías OPS/OMS (referenciadas en Ley 1614 ERSSAN, Anexo III, p.53)',
    nota_umbral_internacional = 'Metodo membrana filtrante (UFC/100ml)'
WHERE param_code = 'coliformes_fecales';

-- Coliformes totales: en tabla aparecen criterios 0 y 3 (UFC/100ml) segun porcentaje de cumplimiento
UPDATE param_catalog
SET
    umbral_local_max = 3,
    fuente_umbral_local = 'Ley 1614 ERSSAN, Anexo III, p.53',
    nota_umbral_local = 'Metodo membrana filtrante: 0 (98% muestras) y hasta 3 (95% muestras)',
    umbral_internacional_max = 0,
    fuente_umbral_internacional = 'Guías OPS/OMS (referenciadas en Ley 1614 ERSSAN, Anexo III, p.53)',
    nota_umbral_internacional = 'Valor recomendado 0'
WHERE param_code = 'coliformes_totales';


-- ============================================================
-- REVISION INTERNACIONAL (investigacion complementaria)
-- Fuentes base:
-- - UE: Directive (EU) 2020/2184, Annex I (Partes A/B/C)
-- - USA: EPA NPDWR / NSDWR
-- ============================================================

-- Arsénico: 10 ug/L = 0.01 mg/L
UPDATE param_catalog
SET
    umbral_internacional_max = 0.01,
    fuente_umbral_internacional = 'Directive (EU) 2020/2184, Annex I, Part B (Arsenic 10 ug/L)',
    nota_umbral_internacional = 'Valor paramétrico UE'
WHERE param_code = 'arsenico';

-- Cromo total: 25 ug/L = 0.025 mg/L
UPDATE param_catalog
SET
    umbral_internacional_max = 0.025,
    fuente_umbral_internacional = 'Directive (EU) 2020/2184, Annex I, Part B (Chromium 25 ug/L)',
    nota_umbral_internacional = 'Valor paramétrico UE'
WHERE param_code = 'cromo_total';

-- Cobre: 2.0 mg/L
UPDATE param_catalog
SET
    umbral_internacional_max = 2.0,
    fuente_umbral_internacional = 'Directive (EU) 2020/2184, Annex I, Part B (Copper 2.0 mg/L)',
    nota_umbral_internacional = 'Valor paramétrico UE'
WHERE param_code = 'cobre';

-- Mercurio: 1 ug/L = 0.001 mg/L
UPDATE param_catalog
SET
    umbral_internacional_max = 0.001,
    fuente_umbral_internacional = 'Directive (EU) 2020/2184, Annex I, Part B (Mercury 1.0 ug/L)',
    nota_umbral_internacional = 'Valor paramétrico UE'
WHERE param_code = 'mercurio';

-- Fluoruro: 1.5 mg/L
UPDATE param_catalog
SET
    umbral_internacional_max = 1.5,
    fuente_umbral_internacional = 'Directive (EU) 2020/2184, Annex I, Part B (Fluoride 1.5 mg/L)',
    nota_umbral_internacional = 'Valor paramétrico UE'
WHERE param_code = 'fluoruro';

-- Amonio: 0.50 mg/L
UPDATE param_catalog
SET
    umbral_internacional_max = 0.50,
    fuente_umbral_internacional = 'Directive (EU) 2020/2184, Annex I, Part C (Ammonium 0.50 mg/L)',
    nota_umbral_internacional = 'Parametro indicador UE'
WHERE param_code = 'amonio';

-- Cloruro: 250 mg/L
UPDATE param_catalog
SET
    umbral_internacional_max = 250,
    fuente_umbral_internacional = 'Directive (EU) 2020/2184, Annex I, Part C (Chloride 250 mg/L)',
    nota_umbral_internacional = 'Parametro indicador UE'
WHERE param_code = 'cloruro';

-- Conductividad: 2500 uS/cm
UPDATE param_catalog
SET
    umbral_internacional_max = 2500,
    fuente_umbral_internacional = 'Directive (EU) 2020/2184, Annex I, Part C (Conductivity 2500 uS/cm at 20C)',
    nota_umbral_internacional = 'Parametro indicador UE'
WHERE param_code = 'conductividad';

-- Hierro total: 200 ug/L = 0.2 mg/L
UPDATE param_catalog
SET
    umbral_internacional_max = 0.2,
    fuente_umbral_internacional = 'Directive (EU) 2020/2184, Annex I, Part C (Iron 200 ug/L)',
    nota_umbral_internacional = 'Parametro indicador UE'
WHERE param_code = 'hierro_total';

-- Manganeso: 50 ug/L = 0.05 mg/L
UPDATE param_catalog
SET
    umbral_internacional_max = 0.05,
    fuente_umbral_internacional = 'Directive (EU) 2020/2184, Annex I, Part C (Manganese 50 ug/L)',
    nota_umbral_internacional = 'Parametro indicador UE'
WHERE param_code = 'manganeso';

-- Nitratos (como NO3-): 50 mg/L
UPDATE param_catalog
SET
    umbral_internacional_max = 50,
    fuente_umbral_internacional = 'Directive (EU) 2020/2184, Annex I, Part B (Nitrate 50 mg/L)',
    nota_umbral_internacional = 'Valor paramétrico UE'
WHERE param_code = 'nitratos';

-- Nitritos (como NO2-): 0.50 mg/L
UPDATE param_catalog
SET
    umbral_internacional_max = 0.50,
    fuente_umbral_internacional = 'Directive (EU) 2020/2184, Annex I, Part B (Nitrite 0.50 mg/L)',
    nota_umbral_internacional = 'Valor paramétrico UE'
WHERE param_code = 'nitritos';

-- Sodio: 200 mg/L
UPDATE param_catalog
SET
    umbral_internacional_max = 200,
    fuente_umbral_internacional = 'Directive (EU) 2020/2184, Annex I, Part C (Sodium 200 mg/L)',
    nota_umbral_internacional = 'Parametro indicador UE'
WHERE param_code = 'sodio';

-- Sulfato: 250 mg/L
UPDATE param_catalog
SET
    umbral_internacional_max = 250,
    fuente_umbral_internacional = 'Directive (EU) 2020/2184, Annex I, Part C (Sulphate 250 mg/L)',
    nota_umbral_internacional = 'Parametro indicador UE'
WHERE param_code = 'sulfato';

-- pH: 6.5 a 9.5
UPDATE param_catalog
SET
    umbral_internacional_min = 6.5,
    umbral_internacional_max = 9.5,
    fuente_umbral_internacional = 'Directive (EU) 2020/2184, Annex I, Part C (Hydrogen ion concentration 6.5-9.5)',
    nota_umbral_internacional = 'Parametro indicador UE'
WHERE param_code = 'ph';

-- Turbidez: <= 1 NTU (consumidor)
UPDATE param_catalog
SET
    umbral_internacional_max = 1,
    fuente_umbral_internacional = 'Directive (EU) 2020/2184, Annex I, Part C (Turbidity <= 1 NTU)',
    nota_umbral_internacional = 'Aplicable en puntos de cumplimiento UE'
WHERE param_code = 'turbidez';

-- Coliformes fecales ~ E. coli: 0 /100 mL
UPDATE param_catalog
SET
    umbral_internacional_max = 0,
    fuente_umbral_internacional = 'Directive (EU) 2020/2184, Annex I, Part A (E. coli: 0 in 100 mL)',
    nota_umbral_internacional = 'Usado como proxy para coliformes fecales'
WHERE param_code = 'coliformes_fecales';

-- Coliformes totales: 0 /100 mL
UPDATE param_catalog
SET
    umbral_internacional_max = 0,
    fuente_umbral_internacional = 'Directive (EU) 2020/2184, Annex I, Part C (Coliform bacteria: 0 in 100 mL)',
    nota_umbral_internacional = 'Parametro indicador UE'
WHERE param_code = 'coliformes_totales';

-- Solidos totales disueltos (TDS): 500 mg/L (EPA Secondary Standard)
UPDATE param_catalog
SET
    umbral_internacional_max = 500,
    fuente_umbral_internacional = 'US EPA NSDWR (Total Dissolved Solids 500 mg/L)',
    nota_umbral_internacional = 'Guia estetica no obligatoria federal'
WHERE param_code = 'solidos_totales';


-- ============================================================
-- REGLA DE CONSISTENCIA:
-- si no existe umbral local en la ley, adoptar umbral internacional
-- ============================================================
UPDATE param_catalog
SET
    umbral_local_min = COALESCE(umbral_local_min, umbral_internacional_min),
    umbral_local_max = COALESCE(umbral_local_max, umbral_internacional_max),
    fuente_umbral_local = COALESCE(
        fuente_umbral_local,
        CASE
            WHEN (umbral_internacional_min IS NOT NULL OR umbral_internacional_max IS NOT NULL)
            THEN fuente_umbral_internacional
            ELSE fuente_umbral_local
        END
    ),
    nota_umbral_local = COALESCE(
        nota_umbral_local,
        CASE
            WHEN (umbral_local_min IS NULL AND umbral_local_max IS NULL)
                 AND (umbral_internacional_min IS NOT NULL OR umbral_internacional_max IS NOT NULL)
            THEN 'Umbral local adoptado desde referencia internacional por ausencia en Ley 1614 ERSSAN'
            ELSE nota_umbral_local
        END
    )
WHERE
    (umbral_local_min IS NULL OR umbral_local_max IS NULL)
    AND (umbral_internacional_min IS NOT NULL OR umbral_internacional_max IS NOT NULL);

-- Parametros sin valor numerico internacional consolidado:
-- se deja constancia de fuente/conclusion y se mantienen umbrales en NULL.
UPDATE param_catalog
SET
    fuente_umbral_internacional = COALESCE(
        fuente_umbral_internacional,
        'Directive (EU) 2020/2184 Annex I + US EPA NPDWR/NSDWR'
    ),
    nota_umbral_internacional = COALESCE(
        nota_umbral_internacional,
        'Sin valor paramétrico numérico internacional único/reconocido'
    )
WHERE param_code IN (
    'alcalinidad_fenolftaleina',
    'bicarbonato',
    'carbonato',
    'materia_organica',
    'nitrogeno_total',
    'temperatura'
);


-- ============================================================
-- VISTA ESTANDAR: MUESTREO X MEDICION (normalizada)
-- - Sin columnas por alias de Excel
-- - Parametros estandar desde param_catalog
-- ============================================================
CREATE OR REPLACE VIEW vw_muestreo_mediciones_estandar AS
SELECT
    m.muestreo_id,
    m.fuente,
    m.fecha_muestreo,
    m.anio,
    m.creado_en AS muestreo_creado_en,

    p.pozo_id,
    p.pozo_code,
    p.fuente_codigo,
    p.distrito,
    p.localidad,
    p.x,
    p.y,
    p.elevacion_m,
    p.profundidad_m,
    p.creado_en AS pozo_creado_en,
    p.actualizado_en AS pozo_actualizado_en,

    md.param_code,
    pc.param_name,
    pc.unidad_std,
    pc.categoria,
    pc.descripcion AS param_descripcion,
    pc.activo AS param_activo,

    md.valor,
    md.valor_texto,
    md.unidad_original,
    md.creado_en AS medicion_creado_en
FROM muestreos m
JOIN pozos p ON p.pozo_id = m.pozo_id
LEFT JOIN mediciones md ON md.muestreo_id = m.muestreo_id
LEFT JOIN param_catalog pc ON pc.param_code = md.param_code;


-- ============================================================
-- VISTA ESTANDAR: MUESTREO (fila) + PARAMETROS (columnas)
-- ============================================================
CREATE OR REPLACE VIEW vw_muestreos_parametros_wide AS
SELECT
    m.muestreo_id,
    m.fuente,
    m.fecha_muestreo,
    m.anio,
    p.pozo_id,
    p.pozo_code,
    p.fuente_codigo,
    p.distrito,
    p.localidad,
    p.x,
    p.y,
    p.elevacion_m,
    p.profundidad_m,

    max(CASE WHEN md.param_code = 'coliformes_fecales' THEN md.valor END) AS coliformes_fecales,
    max(CASE WHEN md.param_code = 'coliformes_totales' THEN md.valor END) AS coliformes_totales,
    max(CASE WHEN md.param_code = 'nitratos' THEN md.valor END) AS nitratos,
    max(CASE WHEN md.param_code = 'nitritos' THEN md.valor END) AS nitritos,
    max(CASE WHEN md.param_code = 'amonio' THEN md.valor END) AS amonio,
    max(CASE WHEN md.param_code = 'nitrogeno_total' THEN md.valor END) AS nitrogeno_total,
    max(CASE WHEN md.param_code = 'ph' THEN md.valor END) AS ph,
    max(CASE WHEN md.param_code = 'conductividad' THEN md.valor END) AS conductividad,
    max(CASE WHEN md.param_code = 'temperatura' THEN md.valor END) AS temperatura,
    max(CASE WHEN md.param_code = 'turbidez' THEN md.valor END) AS turbidez,
    max(CASE WHEN md.param_code = 'cloro_residual' THEN md.valor END) AS cloro_residual,
    max(CASE WHEN md.param_code = 'solidos_totales' THEN md.valor END) AS solidos_totales,
    max(CASE WHEN md.param_code = 'materia_organica' THEN md.valor END) AS materia_organica,
    max(CASE WHEN md.param_code = 'alcalinidad_total' THEN md.valor END) AS alcalinidad_total,
    max(CASE WHEN md.param_code = 'alcalinidad_fenolftaleina' THEN md.valor END) AS alcalinidad_fenolftaleina,
    max(CASE WHEN md.param_code = 'bicarbonato' THEN md.valor END) AS bicarbonato,
    max(CASE WHEN md.param_code = 'carbonato' THEN md.valor END) AS carbonato,
    max(CASE WHEN md.param_code = 'sulfato' THEN md.valor END) AS sulfato,
    max(CASE WHEN md.param_code = 'cloruro' THEN md.valor END) AS cloruro,
    max(CASE WHEN md.param_code = 'dureza' THEN md.valor END) AS dureza,
    max(CASE WHEN md.param_code = 'calcio' THEN md.valor END) AS calcio,
    max(CASE WHEN md.param_code = 'magnesio' THEN md.valor END) AS magnesio,
    max(CASE WHEN md.param_code = 'sodio' THEN md.valor END) AS sodio,
    max(CASE WHEN md.param_code = 'potasio' THEN md.valor END) AS potasio,
    max(CASE WHEN md.param_code = 'hierro_total' THEN md.valor END) AS hierro_total,
    max(CASE WHEN md.param_code = 'fluoruro' THEN md.valor END) AS fluoruro,
    max(CASE WHEN md.param_code = 'arsenico' THEN md.valor END) AS arsenico,
    max(CASE WHEN md.param_code = 'mercurio' THEN md.valor END) AS mercurio,
    max(CASE WHEN md.param_code = 'manganeso' THEN md.valor END) AS manganeso,
    max(CASE WHEN md.param_code = 'cobre' THEN md.valor END) AS cobre,
    max(CASE WHEN md.param_code = 'cromo_total' THEN md.valor END) AS cromo_total
FROM muestreos m
JOIN pozos p ON p.pozo_id = m.pozo_id
LEFT JOIN mediciones md ON md.muestreo_id = m.muestreo_id
GROUP BY
    m.muestreo_id,
    m.fuente,
    m.fecha_muestreo,
    m.anio,
    p.pozo_id,
    p.pozo_code,
    p.fuente_codigo,
    p.distrito,
    p.localidad,
    p.x,
    p.y,
    p.elevacion_m,
    p.profundidad_m;

