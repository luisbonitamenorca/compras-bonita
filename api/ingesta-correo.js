// api/ingesta-correo.js · Ingesta automática de facturas desde el buzón IMAP
// ============================================================================
// v2 — DESCUBRIR y PROCESAR son dos fases separadas.
//
// El problema de la v1: la fila en compras_correo sólo nacía cuando el procesado
// salía bien. Lo que no se procesaba no dejaba rastro, y con una ventana de 30
// días y un tope de 10 correos por ejecución, un atasco podía tragarse facturas
// de forma definitiva y silenciosa.
//
// v2:
//   Fase 1 (DESCUBRIR) — barata: recorre TODAS las carpetas, lee sólo los sobres
//   y registra cada mensaje en compras_correo con estado DESCUBIERTO. No descarga
//   nada. El inventario del buzón queda en la base de datos, es permanente, y
//   nada puede caducar sin que se vea.
//
//   Fase 2 (PROCESAR) — cara: coge los DESCUBIERTO más antiguos, descarga el
//   mensaje, sube los adjuntos y sólo entonces marca el correo como PROCESADO.
//   Si algo falla, el correo queda en ERROR y vuelve a la cola en la siguiente
//   pasada. Los adjuntos rechazados por tipo se registran como DESCARTADO con el
//   motivo, en vez de desaparecer.
//
// NO modifica el buzón: no marca como leído, no mueve ni borra nada.
//
// Variables de entorno en Vercel:
//   IMAP_HOST, IMAP_PORT (993), IMAP_USER, IMAP_PASS
//   INGESTA_CARPETAS_EXCLUIR  (opcional, lista separada por comas)
// ============================================================================

import { ImapFlow } from "imapflow";
import { simpleParser } from "mailparser";

const SB_URL = "https://qjfraquadsvtfwolfbkb.supabase.co";
const SB_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InFqZnJhcXVhZHN2dGZ3b2xmYmtiIiwicm9sZSI6ImFub24iLCJpYXQiOjE3ODAwNTYxNzIsImV4cCI6MjA5NTYzMjE3Mn0.3XidwXSbZPWKdlQD7vPOnqc96oY7sEVq7Bc74KF3okk";
const BUCKET = "documentos";

const DIAS_ATRAS   = 60;  // ventana de DESCUBRIMIENTO (el procesado ya no depende de ella)
const MAX_INTENTOS = 3;   // reintentos antes de dejar un correo en ERROR definitivo

// El límite real no es un número de correos, es el tiempo: unos pesan 200 KB y
// otros 15 MB. Un tope fijo o se queda corto en los días flojos o revienta por
// timeout en los días de cierre de mes. Se procesa mientras quede presupuesto.
//
// 60 s es lo que permite cualquier plan de Vercel. Si el proyecto está en Pro,
// pon INGESTA_SEGUNDOS_MAX=300 en las variables de entorno y esto se multiplica
// por cinco sin tocar el código.
const SEGUNDOS_MAX  = parseInt(process.env.INGESTA_SEGUNDOS_MAX || "60", 10);
const MARGEN_MS     = Math.min(15000, SEGUNDOS_MAX * 1000 * 0.25); // colchón para cerrar limpio
const PRESUPUESTO_MS = SEGUNDOS_MAX * 1000 - MARGEN_MS;
const TECHO_CORREOS = 200;                         // freno de seguridad, no objetivo

export const config = { maxDuration: SEGUNDOS_MAX };

// Carpetas que no se inventarían. Enviados y borradores no son facturas recibidas;
// la papelera se excluye porque borrar es una decisión humana que hay que respetar.
// Los nombres largos ("elementos enviados") van enteros porque no son un segmento
// de ruta sino el nombre completo de la carpeta: así los llama el servidor real.
const EXCLUIR_POR_DEFECTO = ["trash", "papelera", "deleted", "drafts", "borradores",
                             "sent", "enviados", "junk", "spam", "correo no deseado",
                             "elementos enviados", "elementos eliminados",
                             "elementos borrados", "elementos guardados",
                             "sent items", "deleted items", "bandeja de salida",
                             "outbox", "archive", "archivo"];

const MIMES_OK = ["application/pdf", "image/jpeg", "image/png", "image/webp", "image/heic"];
const EXT_OK   = ["pdf", "jpg", "jpeg", "png", "webp", "heic"];

/* ---------------------------------------------------------------- Supabase */

async function sb(path, opts = {}) {
  const r = await fetch(`${SB_URL}/rest/v1/${path}`, {
    ...opts,
    headers: {
      apikey: SB_KEY,
      Authorization: `Bearer ${SB_KEY}`,
      "Content-Type": "application/json",
      Prefer: opts.prefer || "return=representation",
      ...(opts.headers || {}),
    },
  });
  if (!r.ok) {
    const t = await r.text();
    throw new Error(`Supabase ${r.status}: ${t.slice(0, 200)}`);
  }
  const txt = await r.text();
  return txt ? JSON.parse(txt) : [];
}

async function sbUploadBuffer(buffer, nombre, mime) {
  const ext = (String(nombre || "").split(".").pop() || "bin").toLowerCase().replace(/[^a-z0-9]/g, "") || "bin";
  const path = `correo_${Date.now()}_${Math.random().toString(36).slice(2, 8)}.${ext}`;
  const r = await fetch(`${SB_URL}/storage/v1/object/${BUCKET}/${path}`, {
    method: "POST",
    headers: {
      apikey: SB_KEY,
      Authorization: `Bearer ${SB_KEY}`,
      "Content-Type": mime || "application/octet-stream",
    },
    body: buffer,
  });
  if (!r.ok) {
    const t = await r.text();
    throw new Error(`Storage ${r.status}: ${t.slice(0, 200)}`);
  }
  return { url: `${SB_URL}/storage/v1/object/public/${BUCKET}/${path}`, path };
}

/* ------------------------------------------------------------------ ayudas */

function esAdjuntoValido(a) {
  const mime = (a.contentType || "").toLowerCase();
  if (MIMES_OK.includes(mime)) return true;
  const ext = (String(a.filename || "").split(".").pop() || "").toLowerCase();
  return EXT_OK.includes(ext);
}

// Enlaces que huelen a descarga de factura. Plataformas como nuestrafacturamail
// no adjuntan el PDF: mandan un enlace. Sin esto la factura se pierde entera.
function enlacesDescarga(parsed) {
  const texto = `${parsed.html || ""} ${parsed.text || ""}`;
  const urls = texto.match(/https?:\/\/[^\s"'<>)\]]+/gi) || [];
  const interesantes = urls.filter((u) => /factur|descarg|download|invoice|documento|adjunto|\.pdf/i.test(u));
  return [...new Set(interesantes.length ? interesantes : urls)].slice(0, 5);
}

// El separador de jerarquía cambia según el servidor: unos usan "/" y otros ".".
// Comparamos segmento a segmento para que "INBOX.Sent" y "INBOX/Sent" se traten igual.
function carpetaExcluida(ruta, excluir) {
  const r = String(ruta || "").toLowerCase();
  if (!r) return false;
  const segmentos = r.split(/[/.]/).filter(Boolean);
  return excluir.some((e) => r === e || segmentos.includes(e) || segmentos.some((s) => s.startsWith(e + " ")));
}

/* ----------------------------------------------- FASE 1 · descubrir sobres */

async function descubrir(client, resumen, excluir) {
  const desde = new Date(Date.now() - DIAS_ATRAS * 24 * 3600 * 1000);
  let carpetas = [];
  try {
    carpetas = (await client.list()).map((b) => b.path);
  } catch (_) {
    carpetas = ["INBOX"];
  }
  carpetas = carpetas.filter((p) => !carpetaExcluida(p, excluir));
  resumen.carpetas = carpetas;

  for (const carpeta of carpetas) {
    let lock;
    try {
      lock = await client.getMailboxLock(carpeta);
    } catch (e) {
      resumen.errores.push(`carpeta ${carpeta}: ${e.message}`.slice(0, 200));
      continue;
    }
    try {
      const uids = await client.search({ since: desde }, { uid: true });
      if (!uids || !uids.length) continue;

      const lote = [];
      for await (const msg of client.fetch(uids, { envelope: true, uid: true }, { uid: true })) {
        const env = msg.envelope || {};
        const mid = (env.messageId ||
          `sin-mid|${carpeta}|${env.from?.[0]?.address || ""}|${env.subject || ""}|${env.date || ""}`).trim();
        lote.push({
          message_id: mid,
          remitente: env.from?.[0]?.address || null,
          asunto: env.subject || null,
          fecha_correo: env.date ? new Date(env.date).toISOString() : null,
          carpeta,
          uid: msg.uid,
          estado: "DESCUBIERTO",
        });
        resumen.revisados++;
      }

      // Upsert en bloques. ignore-duplicates hace que reencontrar un mensaje ya
      // conocido no cueste nada ni pise su estado. Esto sustituye a la consulta
      // "¿cuáles ya vi?" de la v1, que construía un filtro in.() a mano y podía
      // reventar la ejecución entera con un Message-ID que llevara una coma.
      for (let i = 0; i < lote.length; i += 100) {
        const trozo = lote.slice(i, i + 100);
        const nuevos = await sb("compras_correo?on_conflict=message_id", {
          method: "POST",
          prefer: "resolution=ignore-duplicates,return=representation",
          body: JSON.stringify(trozo),
        });
        resumen.descubiertos += nuevos.length;
      }
    } catch (e) {
      resumen.errores.push(`carpeta ${carpeta}: ${e.message}`.slice(0, 200));
    } finally {
      lock.release();
    }
  }
}

/* --------------------------------------- FASE 2 · procesar los descubiertos */

// Devuelve el mensaje parseado. Si el UID ya no vale (alguien movió el correo,
// cosa habitual en un buzón compartido), lo busca por Message-ID en la carpeta.
async function descargarMensaje(client, correo) {
  try {
    const { content } = await client.download(correo.uid, undefined, { uid: true });
    return await simpleParser(content);
  } catch (e) {
    const uids = await client.search({ header: { "message-id": correo.message_id } }, { uid: true });
    if (!uids || !uids.length) throw new Error(`no encontrado por UID ni por Message-ID: ${e.message}`);
    const { content } = await client.download(uids[0], undefined, { uid: true });
    return await simpleParser(content);
  }
}

async function procesarCorreo(client, correo, resumen) {
  const parsed = await descargarMensaje(client, correo);
  const todos  = parsed.attachments || [];
  const buenos = todos.filter(esAdjuntoValido);
  const malos  = todos.filter((a) => !esAdjuntoValido(a));

  // Las subidas son espera de red, no cálculo: en serie, un correo con 8 adjuntos
  // tarda 8 veces lo que uno. En paralelo tarda casi lo mismo que el más lento.
  const subidas = await Promise.all(buenos.map(async (a) => {
    const nombre = a.filename || "documento.pdf";
    const subida = await sbUploadBuffer(a.content, nombre, a.contentType);
    return { nombre, subida, mime: a.contentType || null };
  }));

  const filas = [];
  for (const s of subidas) {
    filas.push({
      correo_id: correo.id,
      nombre_archivo: s.nombre,
      mime: s.mime,
      url: s.subida.url,
      storage_path: s.subida.path,
      estado: "PENDIENTE",
      error: null,          // mismas claves que las filas de descarte: ver nota abajo
    });
  }
  // Los rechazados se registran igualmente, con el motivo. Un zip o un XML de
  // Facturae que no sabemos leer tiene que verse, no evaporarse.
  for (const a of malos) {
    filas.push({
      correo_id: correo.id,
      nombre_archivo: a.filename || "(sin nombre)",
      mime: a.contentType || null,
      url: null,
      storage_path: null,
      estado: "DESCARTADO",
      error: `tipo de adjunto no admitido: ${a.contentType || "desconocido"}`,
    });
  }
  // OJO: en un insert por lotes PostgREST exige que TODOS los objetos tengan
  // exactamente las mismas claves, o responde PGRST102 "All object keys must
  // match" y se pierde el correo entero. Por eso las filas buenas llevan
  // error:null explícito aunque no lo necesiten.
  if (filas.length) {
    await sb("compras_correo_adjunto", { method: "POST", body: JSON.stringify(filas) });
  }
  resumen.adjuntos   += buenos.length;
  resumen.rechazados += malos.length;

  const enlaces = buenos.length ? null : enlacesDescarga(parsed);
  if (!buenos.length) resumen.sin_adjuntos++;

  // PROCESADO sólo cuando todo lo anterior ha salido bien. Si reventó a mitad,
  // el correo sigue reclamable y se reintenta: nunca se da por bueno a medias.
  await sb(`compras_correo?id=eq.${correo.id}`, {
    method: "PATCH",
    body: JSON.stringify({
      estado: "PROCESADO",
      num_adjuntos: buenos.length,
      adjuntos_detectados: todos.length,
      enlaces: enlaces && enlaces.length ? enlaces : null,
      procesado_at: new Date().toISOString(),
      error: null,
    }),
  });
}

async function procesar(client, resumen, arranque) {
  // Reencolar lo que quedó a medias: ERROR con reintentos disponibles y
  // PROCESANDO abandonado (una ejecución que murió por timeout de Vercel).
  const hace1h = new Date(Date.now() - 3600 * 1000).toISOString();
  await sb(`compras_correo?estado=eq.ERROR&intentos=lt.${MAX_INTENTOS}`, {
    method: "PATCH", body: JSON.stringify({ estado: "DESCUBIERTO" }),
  }).catch(() => {});
  await sb(`compras_correo?estado=eq.PROCESANDO&or=(procesado_at.lt.${hace1h},procesado_at.is.null)`, {
    method: "PATCH", body: JSON.stringify({ estado: "DESCUBIERTO" }),
  }).catch(() => {});

  const pend = await sb(
    "compras_correo?estado=eq.DESCUBIERTO&select=id,message_id,uid,carpeta,asunto,intentos" +
    `&order=fecha_correo.asc&limit=${TECHO_CORREOS}`
  );
  resumen.lote = pend.length;
  if (!pend.length) return;

  // Agrupar por carpeta para abrir cada buzón una sola vez.
  const porCarpeta = new Map();
  for (const c of pend) {
    const k = c.carpeta || "INBOX";
    if (!porCarpeta.has(k)) porCarpeta.set(k, []);
    porCarpeta.get(k).push(c);
  }

  for (const [carpeta, correos] of porCarpeta) {
    let lock;
    try {
      lock = await client.getMailboxLock(carpeta);
    } catch (e) {
      resumen.errores.push(`abrir ${carpeta}: ${e.message}`.slice(0, 200));
      continue;
    }
    try {
      for (const correo of correos) {
        // Se para antes de que Vercel corte la función. Lo que no dé tiempo sigue
        // en DESCUBIERTO y lo coge la siguiente pasada: nada se queda a medias.
        if (Date.now() - arranque > PRESUPUESTO_MS) { resumen.corte_por_tiempo = true; break; }

        // Reclamo optimista: si otra ejecución lo cogió antes, la respuesta viene vacía.
        const claim = await sb(`compras_correo?id=eq.${correo.id}&estado=eq.DESCUBIERTO`, {
          method: "PATCH",
          body: JSON.stringify({ estado: "PROCESANDO", procesado_at: new Date().toISOString() }),
        });
        if (!claim.length) continue;

        try {
          await procesarCorreo(client, correo, resumen);
          resumen.procesados++;
        } catch (e) {
          await sb(`compras_correo?id=eq.${correo.id}`, {
            method: "PATCH",
            body: JSON.stringify({
              estado: "ERROR",
              intentos: (correo.intentos || 0) + 1,
              error: String(e.message || e).slice(0, 500),
            }),
          }).catch(() => {});
          resumen.errores.push(`${correo.asunto || correo.id}: ${e.message}`.slice(0, 200));
        }
      }
    } finally {
      lock.release();
    }
  }
}

/* ---------------------------------------------------------------- handler */

export default async function handler(req, res) {
  const host = process.env.IMAP_HOST;
  const user = process.env.IMAP_USER;
  const pass = process.env.IMAP_PASS;
  const port = parseInt(process.env.IMAP_PORT || "993", 10);

  if (!host || !user || !pass) {
    return res.status(500).json({
      error: "Faltan variables de entorno IMAP_HOST / IMAP_USER / IMAP_PASS en Vercel",
    });
  }

  const excluir = (process.env.INGESTA_CARPETAS_EXCLUIR || "")
    .split(",").map((s) => s.trim().toLowerCase()).filter(Boolean);
  const excluirFinal = excluir.length ? excluir : EXCLUIR_POR_DEFECTO;

  const resumen = {
    revisados: 0, descubiertos: 0, procesados: 0, adjuntos: 0,
    rechazados: 0, sin_adjuntos: 0, lote: 0,
    corte_por_tiempo: false, segundos: 0,
    carpetas: [], errores: [],
  };
  const arranque = Date.now();

  const client = new ImapFlow({
    host, port,
    secure: port === 993,
    auth: { user, pass },
    logger: false,
  });

  try {
    await client.connect();
    await descubrir(client, resumen, excluirFinal);
    await procesar(client, resumen, arranque);
    await client.logout();
    resumen.segundos = Math.round((Date.now() - arranque) / 100) / 10;

    // Cuánto queda por leer: el dato que hacía falta para saber si vamos al día.
    let pendientes = null;
    try {
      const r = await fetch(`${SB_URL}/rest/v1/compras_correo?estado=eq.DESCUBIERTO&select=id`, {
        headers: {
          apikey: SB_KEY, Authorization: `Bearer ${SB_KEY}`,
          Prefer: "count=exact", Range: "0-0",
        },
      });
      pendientes = parseInt((r.headers.get("content-range") || "/0").split("/")[1], 10);
    } catch (_) {}

    return res.status(200).json({ ok: true, pendientes_ahora: pendientes, ...resumen });
  } catch (e) {
    try { await client.logout(); } catch (_) {}
    const partes = [];
    if (e.authenticationFailed) partes.push("AUTENTICACIÓN RECHAZADA: revisa IMAP_USER e IMAP_PASS");
    if (e.message) partes.push(e.message);
    if (e.responseText) partes.push("Respuesta del servidor: " + e.responseText);
    if (e.code) partes.push("Código: " + e.code);
    if (e.serverResponseCode) partes.push("Server code: " + e.serverResponseCode);
    return res.status(500).json({
      ok: false,
      error: partes.join(" · ") || String(e),
      host_usado: host, puerto_usado: port, usuario_usado: user,
      ...resumen,
    });
  }
}
