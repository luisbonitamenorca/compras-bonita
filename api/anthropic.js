// api/anthropic.js  ·  Proxy serverless para la API de Anthropic
// La API key NUNCA va en el frontend: se lee de la variable de entorno
// ANTHROPIC_API_KEY que configuras en Vercel (Settings → Environment Variables).

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
    const data = await upstream.json();
    return res.status(upstream.status).json(data);
  } catch (e) {
    return res.status(500).json({ error: { message: String(e) } });
  }
}
