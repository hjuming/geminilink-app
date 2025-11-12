/**
 * GeminiLink API Worker
 * ----------------------
 * 支援：健康檢查 / D1 查詢 / Gemini API / R2 上傳
 */

export interface Env {
  DB: D1Database; // D1 資料庫
  FILES: R2Bucket; // R2 儲存桶
  GEMINI_API_KEY: string; // 你的 Gemini API 金鑰（在 Cloudflare Dashboard 設定環境變數）
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);
    const { pathname } = url;

    // CORS 設定（方便前端呼叫）
    const corsHeaders = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
    };
    if (request.method === "OPTIONS") return new Response(null, { headers: corsHeaders });

    try {
      // ✅ 根路徑：API 狀態
      if (pathname === "/") {
        return new Response("GeminiLink API ready 🚀", { headers: corsHeaders });
      }

      // ✅ 健康檢查
      if (pathname === "/health") {
        return Response.json(
          { status: "ok", service: "GeminiLink API", time: new Date().toISOString() },
          { headers: corsHeaders }
        );
      }

      // ✅ 範例：呼叫 Gemini API（POST /ai，body: { prompt: "..." }）
      if (pathname === "/ai" && request.method === "POST") {
        const { prompt } = await request.json();

        const geminiRes = await fetch("https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "x-goog-api-key": env.GEMINI_API_KEY,
          },
          body: JSON.stringify({
            contents: [{ parts: [{ text: prompt }] }],
          }),
        });

        const data = await geminiRes.json();
        const output = data?.candidates?.[0]?.content?.parts?.[0]?.text || "(no response)";
        return Response.json({ output }, { headers: corsHeaders });
      }

      // ✅ 範例：讀取 D1 資料庫
      if (pathname === "/products") {
        const result = await env.DB.prepare("SELECT id, name, price FROM products LIMIT 5").all();
        return Response.json(result, { headers: corsHeaders });
      }

      // ✅ 範例：上傳檔案到 R2
      if (pathname === "/upload" && request.method === "POST") {
        const { filename, content } = await request.json();
        await env.FILES.put(filename, content);
        return Response.json({ message: `Uploaded ${filename} successfully.` }, { headers: corsHeaders });
      }

      // ❌ 未匹配的路由
      return new Response("Not found", { status: 404, headers: corsHeaders });
    } catch (err: any) {
      console.error(err);
      return Response.json(
        { error: err.message || "Internal Server Error" },
        { status: 500, headers: corsHeaders }
      );
    }
  },
} satisfies ExportedHandler<Env>;
