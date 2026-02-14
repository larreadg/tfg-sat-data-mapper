import fs from "node:fs";
import path from "node:path";
import XLSX from "xlsx";

import {
  withTx,
  guardarPozo,
  guardarMuestreo,
  guardarMedicion,
  cargarParamAliasMap,
} from "./db/index.js";

const FUENTE = "GA_calidad_tesis_2006";

const META_COLS = new Set([
  "No",
  "Distrito",
  "X",
  "Y",
  "Año",
  "Contaminación?",
]);

function toNum(v) {
  if (v === null || v === undefined || v === "") return null;
  if (typeof v === "number") return v;
  const s = String(v).trim().replace(",", ".");
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
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
  if (v === null || v === undefined || v === "") {
    return { valor: null, valor_texto: null };
  }

  if (typeof v === "number") {
    return { valor: v, valor_texto: null };
  }

  const s = String(v).trim();
  const n = Number(s.replace(",", "."));
  if (Number.isFinite(n)) {
    return { valor: n, valor_texto: null };
  }

  return { valor: null, valor_texto: s };
}

function resolveFile() {
  const p1 = path.resolve("data", "GA_calidad_tesis_2006.xlsx");
  const p2 = path.resolve("data", "GA_calidad_tesis_2006_.xlsx");

  if (fs.existsSync(p1)) return p1;
  if (fs.existsSync(p2)) return p2;

  throw new Error("No se encontró GA_calidad_tesis_2006.xlsx");
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
            pozo_code: null,
            fuente_codigo: FUENTE,
            distrito: r["Distrito"] ?? null,
            localidad: null,
            x: toNum(r["X"]),
            y: toNum(r["Y"]),
            profundidad_m: null,
          },
          client,
          { tolerancia_m: 1 }
        );

        const anioNum = toNum(r["Año"]);
        const anio = Number.isFinite(anioNum) ? Math.trunc(anioNum) : 2006;
        const fechaMuestreo = `${anio}-01-01`;
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
          if (!mapping) {
            unknownCols.add(rawCol);
            continue;
          }

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

      processed++;
    } catch (err) {
      failed++;
      console.warn(`Fila ${i + 2} falló:`, err.message);
    }
  }

  console.log("Importación 2006 finalizada");
  console.log("Filas procesadas:", processed);
  console.log("Filas con error:", failed);
  if (unknownCols.size) {
    console.log("Columnas sin alias:", [...unknownCols].sort());
  }
}

if (process.argv[1].endsWith("p2006.js")) {
  run().catch((e) => {
    console.error(e);
    process.exit(1);
  });
}
