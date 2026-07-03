// api/ingesta-correo.js · Ingesta automática de facturas desde el buzón IMAP
//
// Se conecta a facturas@bonitamenorca.com por IMAP, busca correos nuevos
// (que no estén ya registrados en compras_correo por su Message-ID),
// descarga los adjuntos PDF/imagen al bucket "documentos" de Supabase
// y los deja como PENDIENTE en compras_correo_adjunto.
// El OCR NO se hace aquí: lo hace la app al abrirse, con su pipeline habitual
// (pautas, alias, duplicados). Así hay una sola lógica de procesado.
//
// NO modifica el buzón: no marca como leído, no mueve ni borra nada.
//
// Variables de entorno necesarias en Vercel (Settings → Environment Variables):
//   IMAP_HOST  → servidor IMAP (ej. mail.bonitamenorca.com)
//   IMAP_PORT  → normalmente 993 (SSL). Si es 143 se usa STARTTLS.
//   IMAP_USER  → facturas@bonitamenorca.com
//   IMAP_PASS  → contraseña del buzón

import { ImapFlow } from "imapflow";
import { simpleParser } from "mailparser";

export const config = { maxDuration: 60 };

const SB_URL = "https://qjfraquadsvtfwolfbkb.supabase.co";
const SB_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InFqZnJhcXVhZHN2dGZ3b2xmYmtiIiwicm9sZSI6ImFub24iLCJpYXQiOjE3ODAwNTYxNzIsImV4cCI6MjA5NTYzMjE3Mn0.3XidwXSbZPWKdlQD7vPOnqc96oY7sEVq7Bc74KF3okk";
const BUCKET = "documentos";

const DIAS_ATRAS = 30;      // ventana de búsqueda en el buzón
const MAX_POR_EJECUCION = 10; // correos nuevos procesados por llamada (evita timeouts)

// tipos de adjunto que nos interesan
const MIMES_OK = ["application/pdf", "image/jpeg", "image/png", "image/webp", "image/heic"];
const EXT_OK = ["pdf", "jpg", "jpeg", "png", "webp", "heic"];

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

function esAdjuntoValido(a) {
  const mime = (a.contentType || "").toLowerCase();
  if (MIMES_OK.includes(mime)) return true;
  const ext = (String(a.filename || "").split(".").pop() || "").toLowerCase();
  return EXT_OK.includes(ext);
}

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

  const resumen = { revisados: 0, nuevos: 0, adjuntos: 0, sin_adjuntos: 0, errores: [] };

  const client = new ImapFlow({
    host,
    port,
    secure: port === 993, // 993 = SSL directo; 143 = STARTTLS automático
    auth: { user, pass },
    logger: false,
  });

  try {
    await client.connect();
    const lock = await client.getMailboxLock("INBOX");
    try {
      // 1) correos de los últimos N días
      const desde = new Date(Date.now() - DIAS_ATRAS * 24 * 3600 * 1000);
      const uids = await client.search({ since: desde }, { uid: true });
      if (!uids || !uids.length) {
        return res.status(200).json({ ok: true, mensaje: "Buzón sin correos en la ventana", ...resumen });
      }

      // 2) envelopes (barato) para conocer los Message-ID
      const sobres = [];
      for await (const msg of client.fetch(uids, { envelope: true, uid: true }, { uid: true })) {
        const env = msg.envelope || {};
        const mid = env.messageId || `sin-mid|${(env.from?.[0]?.address || "")}|${env.subject || ""}|${env.date || ""}`;
        sobres.push({
          uid: msg.uid,
          messageId: mid.trim(),
          remitente: env.from?.[0]?.address || null,
          asunto: env.subject || null,
          fecha: env.date ? new Date(env.date).toISOString() : null,
        });
      }
      resumen.revisados = sobres.length;

      // 3) ¿cuáles ya están registrados? (consulta en lotes para no pasarnos de URL)
      const yaVistos = new Set();
      for (let i = 0; i < sobres.length; i += 50) {
        const lote = sobres.slice(i, i + 50).map((s) => `"${s.messageId.replace(/"/g, "")}"`);
        const rows = await sb(`compras_correo?select=message_id&message_id=in.(${encodeURIComponent(lote.join(","))})`);
        rows.forEach((r) => yaVistos.add(r.message_id));
      }

      const nuevos = sobres.filter((s) => !yaVistos.has(s.messageId)).slice(0, MAX_POR_EJECUCION);

      // 4) descargar y registrar cada correo nuevo
      for (const s of nuevos) {
        try {
          const { content } = await client.download(s.uid, undefined, { uid: true });
          const parsed = await simpleParser(content);
          const adjuntos = (parsed.attachments || []).filter(esAdjuntoValido);

          // registrar el correo (unique message_id: si otra ejecución llegó antes, se ignora)
          const filaCorreo = await sb(`compras_correo?on_conflict=message_id`, {
            method: "POST",
            prefer: "resolution=ignore-duplicates,return=representation",
            body: JSON.stringify({
              message_id: s.messageId,
              remitente: s.remitente,
              asunto: s.asunto,
              fecha_correo: s.fecha,
              num_adjuntos: adjuntos.length,
            }),
          });
          if (!filaCorreo.length) continue; // otra ejecución lo registró primero
          const correoId = filaCorreo[0].id;
          resumen.nuevos++;

          if (!adjuntos.length) { resumen.sin_adjuntos++; continue; }

          for (const a of adjuntos) {
            const nombre = a.filename || "documento.pdf";
            const subida = await sbUploadBuffer(a.content, nombre, a.contentType);
            await sb("compras_correo_adjunto", {
              method: "POST",
              body: JSON.stringify({
                correo_id: correoId,
                nombre_archivo: nombre,
                mime: a.contentType || null,
                url: subida.url,
                storage_path: subida.path,
                estado: "PENDIENTE",
              }),
            });
            resumen.adjuntos++;
          }
        } catch (e) {
          resumen.errores.push(`${s.asunto || s.uid}: ${e.message}`.slice(0, 200));
        }
      }
    } finally {
      lock.release();
    }
    await client.logout();
    return res.status(200).json({ ok: true, ...resumen });
  } catch (e) {
    try { await client.logout(); } catch (_) {}
    // detalle ampliado del error para diagnóstico
    const partes = [];
    if (e.authenticationFailed) partes.push("AUTENTICACIÓN RECHAZADA: revisa IMAP_USER e IMAP_PASS");
    if (e.message) partes.push(e.message);
    if (e.responseText) partes.push("Respuesta del servidor: " + e.responseText);
    if (e.code) partes.push("Código: " + e.code);
    if (e.serverResponseCode) partes.push("Server code: " + e.serverResponseCode);
    return res.status(500).json({
      ok: false,
      error: partes.join(" · ") || String(e),
      host_usado: host,
      puerto_usado: port,
      usuario_usado: user,
      ...resumen,
    });
  }
}
