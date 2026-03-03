import fs from "node:fs";
import path from "node:path";
import XLSX from "xlsx";

import dayjs from "dayjs";
import customParseFormat from "dayjs/plugin/customParseFormat.js";
dayjs.extend(customParseFormat);

import {
  withTx,
  guardarPozo,
  guardarMuestreo,
  guardarMedicion,
  cargarParamAliasMap,
} from "./db/index.js";

const FUENTE = "GA_calidad_2001";

const META_COLS = new Set([
  "No",
  "Codigo",
  "Fecha_mues",
  "X-UTM",
  "Y-UTM",
  "Localidad",
  "Prof_M",
  "CONTAMINACION",
]);

function toNum(v) {
  if (v === null || v === undefined || v === "") return null;
  if (typeof v === "number") return v;
  const s = String(v).trim().replace(",", ".");
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

function parseDate(value) {
  if (!value) return null;

  if (value instanceof Date) {
    return dayjs(value).format("YYYY-MM-DD");
  }

  if (typeof value === "number") {
    const parsed = XLSX.SSF.parse_date_code(value);
    if (parsed?.y) {
      return dayjs(
        `${parsed.y}-${parsed.m}-${parsed.d}`,
        "YYYY-M-D",
        true
      ).format("YYYY-MM-DD");
    }
  }

  const formats = [
    "YYYY-MM-DD",
    "DD/MM/YYYY",
    "D/M/YYYY",
    "DD/MM/YY",
    "D/M/YY",
  ];

  for (const f of formats) {
    const d = dayjs(String(value).trim(), f, true);
    if (d.isValid()) return d.format("YYYY-MM-DD");
  }

  return null;
}

function parseHeader(header) {
  const raw = String(header ?? "").trim();
  if (!raw) return { alias: null, unidad: null };

  const m = raw.match(/^(.*?)\s*\((.*?)\)\s*$/);
  if (m) {
    return { alias: m[1].trim(), unidad: m[2].trim() };
  }

  return { alias: raw, unidad: null };
}

function parseCellValue(v) {
  if (v === null || v === undefined || v === "")
    return { valor: null, valor_texto: null };

  if (typeof v === "number")
    return { valor: v, valor_texto: null };

  const s = String(v).trim();
  const n = Number(s.replace(",", "."));

  if (Number.isFinite(n))
    return { valor: n, valor_texto: null };

  return { valor: null, valor_texto: s };
}

function parseContaminacion(v) {
  if (v === null || v === undefined) return null;
  const normalized = String(v).trim().toUpperCase();
  if (!normalized) return null;
  if (normalized === "SI") return true;
  if (normalized === "NO") return false;
  return null;
}

function resolveFile() {
  const p1 = path.resolve("data", "GA_calidad_2001.xlsx");
  const p2 = path.resolve("data", "GA_calidad_2001_.xlsx");

  if (fs.existsSync(p1)) return p1;
  if (fs.existsSync(p2)) return p2;

  throw new Error("No se encontró GA_calidad_2001.xlsx");
}

export async function run() {
  const file = resolveFile();
  console.log("Leyendo:", file);

  const wb = XLSX.readFile(file, { cellDates: true });
  const ws = wb.Sheets[wb.SheetNames[0]];
  const rows = XLSX.utils.sheet_to_json(ws, { defval: null });

  console.log("Filas:", rows.length);

  const aliasMap = await cargarParamAliasMap({ fuente: FUENTE });

  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];

    try {
      await withTx(async (client) => {
        // ---------- POZO ----------
        const pozo = await guardarPozo(
          {
            pozo_code: r["Codigo"]?.trim(),
            fuente_codigo: FUENTE,
            localidad: r["Localidad"],
            x: toNum(r["X-UTM"]),
            y: toNum(r["Y-UTM"]),
            profundidad_m: toNum(r["Prof_M"]),
          },
          client
        );

        // ---------- MUESTREO ----------
        const fecha = parseDate(r["Fecha_mues"]);
        const anio = fecha ? Number(fecha.slice(0, 4)) : 2001;

        const muestreo = await guardarMuestreo(
          {
            pozo_id: pozo.pozo_id,
            fuente: FUENTE,
            fecha_muestreo: fecha,
            anio,
          },
          client
        );

        // ---------- EVALUACION CONTAMINACION ----------
        const alerta = parseContaminacion(r["CONTAMINACION"]);
        if (alerta !== null) {
          await client.query(
            `
            INSERT INTO evaluaciones_contaminacion (
              muestreo_id,
              contaminacion_observada,
              version_reglas
            )
            VALUES ($1, $2, $3)
            ON CONFLICT (muestreo_id) DO UPDATE
            SET
              contaminacion_observada = EXCLUDED.contaminacion_observada,
              version_reglas = EXCLUDED.version_reglas
            `,
            [muestreo.muestreo_id, alerta, "excel_2001_contaminacion_si_no_v1"]
          );
        }

        // ---------- MEDICIONES ----------
        for (const [col, val] of Object.entries(r)) {
          if (META_COLS.has(col)) continue;

          const rawCol = String(col ?? "").trim();
          const { alias, unidad } = parseHeader(rawCol);
          if (!alias) continue;

          // Primero intenta match exacto del encabezado completo, luego alias sin unidad.
          const mapping = aliasMap.get(rawCol) ?? aliasMap.get(alias);
          if (!mapping) continue;

          const { valor, valor_texto } = parseCellValue(val);
          if (valor === null && valor_texto === null) continue;

          await guardarMedicion(
            {
              muestreo_id: muestreo.muestreo_id,
              param_code: mapping.param_code,
              valor,
              valor_texto,
              unidad_original: unidad,
            },
            client
          );
        }
      });
    } catch (err) {
      console.warn(`Fila ${i + 2} falló:`, err.message);
    }
  }

  console.log("Importación 2001 finalizada");
}

// ejecución directa
if (process.argv[1].endsWith("p2001.js")) {
  run().catch((e) => {
    console.error(e);
    process.exit(1);
  });
}
