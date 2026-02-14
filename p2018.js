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

const FUENTE = "GA_calidad_FP_2018";
const PARAM_FALLBACK_BY_ALIAS = new Map([["pH", "ph"]]);

const META_COLS = new Set([
  "codigo_pozo",
  "nombre_lugar",
  "x",
  "y",
  "ele",
  "Fecha_muestreo",
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

  if (value instanceof Date) return dayjs(value).format("YYYY-MM-DD");

  if (typeof value === "number") {
    const parsed = XLSX.SSF.parse_date_code(value);
    if (parsed?.y) {
      return dayjs(`${parsed.y}-${parsed.m}-${parsed.d}`, "YYYY-M-D", true).format(
        "YYYY-MM-DD"
      );
    }
  }

  const formats = [
    "YYYY-MM-DD",
    "DD/MM/YYYY",
    "D/M/YYYY",
    "DD-MM-YYYY",
    "D-M-YYYY",
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
  if (m) return { alias: m[1].trim(), unidad: m[2].trim() };

  return { alias: raw, unidad: null };
}

function parseCellValue(v) {
  if (v === null || v === undefined || v === "") {
    return { valor: null, valor_texto: null };
  }

  if (typeof v === "number") return { valor: v, valor_texto: null };

  const s = String(v).trim();
  const n = Number(s.replace(",", "."));
  if (Number.isFinite(n)) return { valor: n, valor_texto: null };

  return { valor: null, valor_texto: s };
}

function resolveFile() {
  const p1 = path.resolve("data", "GA_calidad_FP_2018.xlsx");
  const p2 = path.resolve("data", "GA_calidad_FP_2018_.xlsx");

  if (fs.existsSync(p1)) return p1;
  if (fs.existsSync(p2)) return p2;

  throw new Error("No se encontró GA_calidad_FP_2018.xlsx");
}

export async function run() {
  const file = resolveFile();
  console.log("Leyendo:", file);

  const wb = XLSX.readFile(file, { cellDates: true });
  const ws = wb.Sheets[wb.SheetNames[0]];
  const rows = XLSX.utils.sheet_to_json(ws, { defval: null });
  console.log("Filas:", rows.length);

  const aliasMap = await cargarParamAliasMap({ fuente: FUENTE });

  let processed = 0;
  let failed = 0;
  const unknownCols = new Set();

  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];

    try {
      await withTx(async (client) => {
        const pozo = await guardarPozo(
          {
            pozo_code: r["codigo_pozo"] !== null && r["codigo_pozo"] !== undefined
              ? String(r["codigo_pozo"]).trim()
              : null,
            fuente_codigo: FUENTE,
            distrito: null,
            localidad: r["nombre_lugar"] ?? null,
            x: toNum(r["x"]),
            y: toNum(r["y"]),
            elevacion_m: toNum(r["ele"]),
            profundidad_m: null,
          },
          client,
          { tolerancia_m: 1 }
        );

        const fecha = parseDate(r["Fecha_muestreo"]);
        const anio = fecha ? Number(fecha.slice(0, 4)) : 2018;
        const fechaMuestreo = fecha ?? `${anio}-01-01`;

        const muestreo = await guardarMuestreo(
          {
            pozo_id: pozo.pozo_id,
            fuente: FUENTE,
            fecha_muestreo: fechaMuestreo,
            anio,
          },
          client
        );

        for (const [col, val] of Object.entries(r)) {
          if (META_COLS.has(col)) continue;

          const rawColExact = String(col ?? "");
          const rawCol = rawColExact.trim();
          const { alias, unidad } = parseHeader(rawColExact);
          if (!alias) continue;

          const mapping =
            aliasMap.get(rawColExact) ?? aliasMap.get(rawCol) ?? aliasMap.get(alias);
          const fallbackParamCode = PARAM_FALLBACK_BY_ALIAS.get(rawCol) ?? PARAM_FALLBACK_BY_ALIAS.get(alias);
          const finalMapping = mapping ?? (fallbackParamCode ? { param_code: fallbackParamCode } : null);

          if (!finalMapping) {
            unknownCols.add(rawCol);
            continue;
          }

          const { valor, valor_texto } = parseCellValue(val);
          if (valor === null && valor_texto === null) continue;

          await guardarMedicion(
            {
              muestreo_id: muestreo.muestreo_id,
              param_code: finalMapping.param_code,
              valor,
              valor_texto,
              unidad_original: unidad,
            },
            client
          );
        }
      });

      processed++;
    } catch (err) {
      failed++;
      console.warn(`Fila ${i + 2} falló:`, err.message);
    }
  }

  console.log("Importación 2018 finalizada");
  console.log("Filas procesadas:", processed);
  console.log("Filas con error:", failed);
  if (unknownCols.size) {
    console.log("Columnas sin alias:", [...unknownCols].sort());
  }
}

if (process.argv[1].endsWith("p2018.js")) {
  run().catch((e) => {
    console.error(e);
    process.exit(1);
  });
}
