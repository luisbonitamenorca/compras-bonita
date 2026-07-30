// api/anthropic.js  ·  Proxy serverless para la API de Anthropic
// La API key NUNCA va en el frontend: se lee de la variable de entorno
// ANTHROPIC_API_KEY que configuras en Vercel (Settings → Environment Variables).

// Cabeceras de control de ritmo que Anthropic devuelve en cada respuesta.
// Sin reenviarlas, el frontend no puede saber cuánta cuota le queda y no tiene
// más remedio que dormir un tiempo fijo a ciegas.
const CABECERAS_LIMITE = [
  "anthropic-ratelimit-requests-limit",
  "anthropic-ratelimit-requests-remaining",
  "anthropic-ratelimit-requests-reset",
  "anthropic-ratelimit-input-tokens-limit",
  "anthropic-ratelimit-input-tokens-remaining",
  "anthropic-ratelimit-input-tokens-reset",
  "anthropic-ratelimit-output-tokens-limit",
  "anthropic-ratelimit-output-tokens-remaining",
  "anthropic-ratelimit-output-tokens-reset",
  "anthropic-ratelimit-tokens-limit",
  "anthropic-ratelimit-tokens-remaining",
  "anthropic-ratelimit-tokens-reset",
  "retry-after"
];

export default async function handler(req, res) {
  if (req.method !== "POST") {
    return res.status(405).json({ error: { message: "Method not allowed" } });
  }

  const key = process.env.ANTHROPIC_API_KEY;
  if (!key) {
    return res.status(500).json({
      error: { message: "Falta ANTHROPIC_API_KEY en las variables de entorno de Vercel" }
    });
  }

  try {
    const upstream = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-api-key": key,
        "anthropic-version": "2023-06-01"
      },
      body: JSON.stringify(req.body)
    });

    // Reenviar las cabeceras de ritmo tal cual llegan.
    for (const h of CABECERAS_LIMITE) {
      const v = upstream.headers.get(h);
      if (v !== null) res.setHeader(h, v);
    }
    // Sin esto el navegador no deja leerlas desde JavaScript aunque se envíen.
    res.setHeader("access-control-expose-headers", CABECERAS_LIMITE.join(", "));

    const data = await upstream.json();
    return res.status(upstream.status).json(data);
  } catch (e) {
    return res.status(500).json({ error: { message: String(e) } });
  }
}
