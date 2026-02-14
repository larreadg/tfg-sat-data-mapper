// db/index.js
import pg from "pg";
import dotenv from "dotenv";
dotenv.config();

const { Pool } = pg;

export const pool = new Pool({ connectionString: process.env.DATABASE_URL });

pool.on("connect", () => console.log("PostgreSQL conectado"));

export async function query(text, params = []) {
    return pool.query(text, params);
}

export async function withTx(fn) {
    const client = await pool.connect();
    try {
        await client.query("BEGIN");
        const result = await fn(client);
        await client.query("COMMIT");
        return result;
    } catch (e) {
        await client.query("ROLLBACK");
        throw e;
    } finally {
        client.release();
    }
}

// ----------------- PARAMETROS (alias -> code) -----------------

/**
 * Devuelve un Map(alias -> {param_code, unidad_std, param_name})
 * Si hay filas con fuente = '2001', esas pisan a las genericas (fuente NULL).
 */
export async function cargarParamAliasMap({ fuente }, client = null) {
    const c = client ?? pool;
    const { rows } = await c.query(
        `
    SELECT pa.alias, pa.param_code, pc.unidad_std, pc.param_name
    FROM param_alias pa
    JOIN param_catalog pc ON pc.param_code = pa.param_code
    WHERE pa.fuente IS NULL OR pa.fuente = $1
    ORDER BY (pa.fuente = $1) DESC
    `,
        [fuente]
    );

    const map = new Map();
    for (const r of rows) {
        // Primer insert gana; como ordenamos, primero viene el específico de fuente.
        if (!map.has(r.alias)) {
            map.set(r.alias, {
                param_code: r.param_code,
                unidad_std: r.unidad_std,
                param_name: r.param_name,
            });
        }
    }
    return map;
}

// ----------------- POZOS -----------------

export async function obtenerPozoPorFuenteCodigo({ fuente_codigo, pozo_code }, client = null) {
    const c = client ?? pool;
    const { rows } = await c.query(
        `SELECT * FROM pozos WHERE fuente_codigo = $1 AND pozo_code = $2 LIMIT 1`,
        [fuente_codigo, pozo_code]
    );
    return rows[0] ?? null;
}

export async function obtenerPozoPorCoords({ x, y, tolerancia_m = 1 }, client = null) {
    const c = client ?? pool;
    const { rows } = await c.query(
        `
    SELECT *,
      sqrt(power(x - $1, 2) + power(y - $2, 2)) AS distancia
    FROM pozos
    WHERE x IS NOT NULL AND y IS NOT NULL
      AND x BETWEEN $1 - $3 AND $1 + $3
      AND y BETWEEN $2 - $3 AND $2 + $3
    ORDER BY distancia ASC
    LIMIT 1
    `,
        [x, y, tolerancia_m]
    );
    if (!rows.length) return null;
    if (rows[0].distancia <= tolerancia_m) return rows[0];
    return null;
}

/**
 * UPSERT pozo:
 * - Primero por (fuente_codigo, pozo_code)
 * - Si no, por coords (tolerancia)
 * - Si no, INSERT
 */
export async function guardarPozo(input, client = null, opts = {}) {
    const c = client ?? pool;

    const fuente_codigo = input.fuente_codigo ?? null;
    const pozo_code = input.pozo_code ?? null;
    const x = input.x ?? null;
    const y = input.y ?? null;

    // 1) por fuente+codigo
    if (fuente_codigo && pozo_code) {
        const existing = await obtenerPozoPorFuenteCodigo({ fuente_codigo, pozo_code }, c);
        if (existing) {
            const { rows } = await c.query(
                `
                    UPDATE pozos
                    SET distrito      = COALESCE($2, distrito),
                        localidad     = COALESCE($3, localidad),
                        x             = COALESCE($4, x),
                        y             = COALESCE($5, y),
                        elevacion_m   = COALESCE($6, elevacion_m),
                        profundidad_m = COALESCE($7, profundidad_m)
                    WHERE pozo_id = $1
                    RETURNING *
                    `,
                [
                    existing.pozo_id,            // $1 uuid
                    input.distrito ?? null,      // $2 text
                    input.localidad ?? null,     // $3 text
                    input.x ?? null,             // $4 double
                    input.y ?? null,             // $5 double
                    input.elevacion_m ?? null,   // $6 double
                    input.profundidad_m ?? null, // $7 double
                ]
            );
            return rows[0];
        }
    }

    // 2) por coords
    const tol = opts.tolerancia_m ?? 1;
    if (typeof x === "number" && typeof y === "number") {
        const byCoords = await obtenerPozoPorCoords({ x, y, tolerancia_m: tol }, c);
        if (byCoords) {
            // Si viene pozo_code y el registro no tiene, lo asignamos conservando fuente_codigo
            const { rows } = await c.query(
            `
            UPDATE pozos
            SET pozo_code     = COALESCE(pozos.pozo_code, $2),
                fuente_codigo = COALESCE(pozos.fuente_codigo, $3),
                distrito      = COALESCE($4, distrito),
                localidad     = COALESCE($5, localidad),
                elevacion_m   = COALESCE($6, elevacion_m),
                profundidad_m = COALESCE($7, profundidad_m)
            WHERE pozo_id = $1
            RETURNING *
            `,
            [
                byCoords.pozo_id,            // $1
                pozo_code ?? null,           // $2
                fuente_codigo ?? null,       // $3
                input.distrito ?? null,      // $4
                input.localidad ?? null,     // $5
                input.elevacion_m ?? null,   // $6
                input.profundidad_m ?? null, // $7
            ]
            );
            return rows[0];
        }
    }

    // 3) insert
    const { rows } = await c.query(
        `
    INSERT INTO pozos
      (pozo_code, fuente_codigo, distrito, localidad, x, y, elevacion_m, profundidad_m)
    VALUES
      ($1,$2,$3,$4,$5,$6,$7,$8)
    RETURNING *
    `,
        [
            pozo_code,
            fuente_codigo,
            input.distrito ?? null,
            input.localidad ?? null,
            x,
            y,
            input.elevacion_m ?? null,
            input.profundidad_m ?? null,
        ]
    );
    return rows[0];
}

// ----------------- MUESTREOS -----------------

export async function guardarMuestreo(input, client = null) {
    const c = client ?? pool;

    if (!input.pozo_id) throw new Error("guardarMuestreo: pozo_id requerido");

    const { rows: found } = await c.query(
        `
    SELECT * FROM muestreos
    WHERE pozo_id = $1
      AND fuente = $2
      AND fecha_muestreo IS NOT DISTINCT FROM $3
      AND anio IS NOT DISTINCT FROM $4
    LIMIT 1
    `,
        [input.pozo_id, input.fuente, input.fecha_muestreo ?? null, input.anio ?? null]
    );
    if (found[0]) return found[0];

    const { rows } = await c.query(
        `
    INSERT INTO muestreos (pozo_id, fuente, fecha_muestreo, anio)
    VALUES ($1,$2,$3,$4)
    RETURNING *
    `,
        [input.pozo_id, input.fuente, input.fecha_muestreo ?? null, input.anio ?? null]
    );
    return rows[0];
}

// ----------------- MEDICIONES -----------------

export async function guardarMedicion(input, client = null) {
    const c = client ?? pool;

    if (!input.muestreo_id || !input.param_code) {
        throw new Error("guardarMedicion: muestreo_id y param_code requeridos");
    }

    const { rows: found } = await c.query(
        `SELECT * FROM mediciones WHERE muestreo_id = $1 AND param_code = $2 LIMIT 1`,
        [input.muestreo_id, input.param_code]
    );

    if (found[0]) {
        const { rows } = await c.query(
            `
      UPDATE mediciones
      SET valor = COALESCE($3, valor),
          valor_texto = COALESCE($4, valor_texto),
          unidad_original = COALESCE($5, unidad_original)
      WHERE muestreo_id = $1 AND param_code = $2
      RETURNING *
      `,
            [
                input.muestreo_id,
                input.param_code,
                input.valor ?? null,
                input.valor_texto ?? null,
                input.unidad_original ?? null,
            ]
        );
        return rows[0];
    }

    const { rows } = await c.query(
        `
    INSERT INTO mediciones (muestreo_id, param_code, valor, valor_texto, unidad_original)
    VALUES ($1,$2,$3,$4,$5)
    RETURNING *
    `,
        [
            input.muestreo_id,
            input.param_code,
            input.valor ?? null,
            input.valor_texto ?? null,
            input.unidad_original ?? null,
        ]
    );
    return rows[0];
}
