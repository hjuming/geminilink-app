/*
 * 檔案: src/index.ts
 * 版本: V12 (Hono 路由 + 認證)
 * 備註: 這是您的核心後端 API 伺服器。
 * - 使用 Hono 進行路由管理。
 * - 新增 /api/auth/register 和 /api/auth/login API。
 * - 保留 v11 的全自動批次匯入功能。
 * - 修正了所有 CSV 解析、Gemini 模型名稱 (gemini-2.5-flash) 和 D1 綁定問題。
 */

import { Hono } from 'hono';
import { GoogleGenerativeAI } from '@google/generative-ai';
import { parse } from 'csv-parse/browser/esm/sync'; // v3 修正版
import * as bcrypt from 'bcryptjs'; // v12 新增

/**
 * 環境變數 (來自 wrangler.toml 和 Cloudflare Secrets)
 */
export interface Env {
	DB: D1Database;
	FILES: R2Bucket;
	GEMINI_API_KEY: string;
	REGISTRATION_KEY: string; // v12 新增：用於保護註冊 API
}

// --- 設定 ---
const BATCH_SIZE = 3; // 每個批次處理 3 筆 (因為圖片處理耗時)
const CSV_FILE_NAME = 'product_inventory_master_v2.csv'; // R2 上的 CSV 檔案
const BCRYPT_SALT_ROUNDS = 10; // 密碼加密強度

// ===========================================
// === 1. 初始化 Hono App (您的 API 路由器) ===
// ===========================================
const app = new Hono<{ Bindings: Env }>();

// ===========================================
// === 2. API 路由 (v12 新增：認證) ===
// ===========================================

/**
 * POST /api/auth/register
 * 註冊您的第一個 admin 帳號。
 * 需要正確的 REGISTRATION_KEY 才能成功。
 */
app.post('/api/auth/register', async (c) => {
	const body = await c.req.json();
	const { email, password, key } = body;

	if (!email || !password || !key) {
		return c.json({ error: '缺少 email, password, 或 key' }, 400);
	}

	// 驗證 Registration Key
	if (key !== c.env.REGISTRATION_KEY) {
		return c.json({ error: '無效的註冊安全碼' }, 403); // 403 Forbidden
	}

	// 檢查帳號是否已存在
	try {
		const existingUser = await c.env.DB.prepare('SELECT user_id FROM Users WHERE email = ?').bind(email).first();
		if (existingUser) {
			return c.json({ error: '此 email 已被註冊' }, 409); // 409 Conflict
		}

		// 將密碼加密
		const passwordHash = await bcrypt.hash(password, BCRYPT_SALT_ROUNDS);

		// 寫入 D1 資料庫 (建立為 admin)
		await c.env.DB.prepare(
			`INSERT INTO Users (email, password_hash, role, supplier_id) 
       VALUES (?, ?, 'admin', NULL)`, // Admin 帳號不綁定特定供應商
		).bind(email, passwordHash).run();

		return c.json({ message: 'Admin 帳號建立成功' });
	} catch (e: any) {
		return c.json({ error: '資料庫錯誤', message: e.message }, 500);
	}
});

/**
 * POST /api/auth/login
 * 登入以取得權限 (未來用於前端介面)
 * (注意：一個完整的系統會回傳 JWT (Token)，目前我們先回傳成功訊息)
 */
app.post('/api/auth/login', async (c) => {
	const body = await c.req.json();
	const { email, password } = body;

	if (!email || !password) {
		return c.json({ error: '缺少 email 或 password' }, 400);
	}

	// 1. 尋找使用者
	const user = await c.env.DB.prepare(
    'SELECT user_id, email, password_hash, role FROM Users WHERE email = ?'
  ).bind(email).first<{ user_id: number; email: string; password_hash: string; role: string }>();

	if (!user) {
		return c.json({ error: '帳號或密碼錯誤' }, 401); // 401 Unauthorized
	}

	// 2. 驗證密碼
	const isPasswordValid = await bcrypt.compare(password, user.password_hash);

	if (!isPasswordValid) {
		return c.json({ error: '帳號或密碼錯誤' }, 401);
	}

	// 登入成功
	// (未來：在這裡產生一個 JWT Token)
	return c.json({
		message: '登入成功',
		user: {
			userId: user.user_id,
			email: user.email,
			role: user.role,
		},
		// token: "your-future-jwt-token-here"
	});
});


// ===========================================
// === 3. API 路由 (v11 保留：全自動匯入) ===
// ===========================================

/**
 * GET /admin/start-full-import
 * 啟動器：您唯一需要手動呼叫的網址
 */
app.get('/admin/start-full-import', (c) => {
	try {
		// 立即回傳訊息給使用者，告知任務已開始
		// 並在背景 (c.executionCtx.waitUntil) 觸發第一個批次
		c.executionCtx.waitUntil(
			// 我們呼叫自己的 POST 處理器，從批次 1 開始
			fetch(new URL('/admin/process-batch?batch=1', c.req.url), {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
			}),
		);

		// 立即回傳給您
		return c.json(
			{
				message: '✅ 自動匯入已啟動！ (v12)',
				details: '系統正在背景處理所有批次。您可以關閉此頁面。',
				check_r2: '請前往 R2 儀表板 (geminilink-files) 檢查圖片是否陸續上傳。',
				check_d1: '請前往 D1 儀表板 (geminilink_db) 檢查資料是否陸續寫入。',
			},
			202, // 202 Accepted: 請求已接受，正在處理
		);
	} catch (e: any) {
		return c.json({ error: '啟動失敗', message: e.message }, 500);
	}
});

/**
 * POST /admin/process-batch
 * 處理器：這個 API 會被「啟動器」或「它自己」在背景呼叫
 */
app.post('/admin/process-batch', async (c) => {
	// 執行批次匯入邏輯 (在背景執行)
	// 我們將這個耗時的任務交給 c.executionCtx.waitUntil，
	// 這樣即使呼叫端斷線，它也能繼續執行
	c.executionCtx.waitUntil(handleBatchImport(c.req, c.env, c.executionCtx));

	// 立即回傳，表示「已收到處理請求」
	return c.json({ message: '批次處理請求已接收' }, 202);
});

// ===========================================
// === 4. 核心匯入邏輯 (來自 v11) ===
// ===========================================

/**
 * 核心處理函式 (v11)
 * 這會在背景執行 (由 ctx.waitUntil 觸發)
 */
async function handleBatchImport(request: Request, env: Env, ctx: ExecutionContext) {
	const url = new URL(request.url);
	
	try {
		console.log(`[handleBatchImport] 開始處理... ${url.search}`);
		const startTime = Date.now();

		// 1. 初始化服務 (從 env 取得)
		const genAI = new GoogleGenerativeAI(env.GEMINI_API_KEY);
		// v9 修正：使用您指定的 'gemini-2.5-flash' 模型
		const model = genAI.getGenerativeModel({ model: 'gemini-2.5-flash' });
		const DB = env.DB;
		const R2_BUCKET = env.FILES;

		// 2. 取得批次編號 (例如 ?batch=1)
		const batchNumber = parseInt(url.searchParams.get('batch') || '1', 10);
		const offset = (batchNumber - 1) * BATCH_SIZE;

		// 3. 從 R2 讀取 CSV 檔案
		const csvObject = await R2_BUCKET.get(CSV_FILE_NAME);
		if (csvObject === null) {
			console.error(`R2 儲存桶中找不到檔案: ${CSV_FILE_NAME}`);
			return; // 在背景中靜默失敗
		}
		const csvText = await csvObject.text();

		// 4. 解析 CSV
		const allProducts: any[] = parse(csvText, {
			columns: true,
			skip_empty_lines: true,
			bom: true, // v4 修正：移除 Excel CSV 的 UTF-8 BOM
		});

		const totalProducts = allProducts.length;
		const productsToProcess = allProducts.slice(offset, offset + BATCH_SIZE);

		// 如果沒有更多商品，回報完成
		if (productsToProcess.length === 0) {
			console.log('🎉 全部匯入完成！');
			return; // 結束遞迴
		}

		// 5. 處理這個批次的 3 筆商品
		console.log(`[批次 ${batchNumber}] 正在處理 ${productsToProcess.length} 筆商品 (SKU: ${productsToProcess.map(p => p['商品貨號']).join(', ')})...`);
		const importLog: string[] = [];
		let dbStatements: D1PreparedStatement[] = [];

		for (const row of productsToProcess) {
			const sku = row['商品貨號'];
			const supplierId = row['供應商'] || 'WEDO';
			if (!sku) continue; 

			// 5a. 呼叫 AI 產生「主要客群」
			const prompt = getAudiencePrompt_v7(row);
			let audienceTags: string[] = ['other'];
			try {
				const result = await model.generateContent(prompt);
				const response = result.response.text().trim();
				const cleanedResponse = response.replace(/```json/g, '').replace(/```/g, '').trim();
				const parsedResponse = JSON.parse(cleanedResponse);
				audienceTags = Array.isArray(parsedResponse) ? parsedResponse.filter(Boolean) : ['other'];
			} catch (aiError: any) {
				importLog.push(`SKU ${sku} AI 失敗: ${aiError.message}. 使用預設值 ['other']`);
			}

			// 5b. 準備 SQL 批次 (靜態資料)
			// v11：呼叫更新的 getProductSqlStatements_v11
			const productStatements = getProductSqlStatements_v11(row, sku, supplierId, audienceTags, DB);
			dbStatements.push(...productStatements);
			importLog.push(`SKU ${sku} -> 客群: [${audienceTags.join(', ')}] -> 已準備匯入 D1`);

			// 5c. 【新功能】處理圖片：下載並上傳至 R2
			const imageUrls = parseImageUrls(row['商品圖檔']);
			let imageIndex = 0;
			for (const imageUrl of imageUrls) {
				const isPrimary = imageIndex === 0 ? 1 : 0;
				const r2Key = `${supplierId}/${sku}/image-${imageIndex + 1}.jpg`;

				try {
					// 執行下載和上傳 (非同步，但不 block 迴圈)
					// 我們也必須將這個任務交給 waitUntil，確保它在背景完成
					ctx.waitUntil(fetchAndUploadImage(imageUrl, r2Key, R2_BUCKET));

					dbStatements.push(
						DB.prepare(`INSERT OR IGNORE INTO ProductImages (sku, r2_key, is_primary) VALUES (?, ?, ?)`).bind(
							sku,
							r2Key,
							isPrimary,
						),
					);
					importLog.push(`  └ 圖片 ${imageIndex + 1} -> (開始上傳至 R2: ${r2Key})`);
				} catch (imgError: any) {
					importLog.push(`  └ 圖片 ${imageIndex + 1} (${imageUrl}) 處理失敗: ${imgError.message}`);
				}
				imageIndex++;
			} 
		}

		// 6. 執行 D1 批次匯入
		if (dbStatements.length > 0) {
			await DB.batch(dbStatements);
		} else {
			importLog.push('警告：這個批次沒有產生任何 SQL 語句。');
		}

		const endTime = Date.now();
		const nextBatch = batchNumber + 1;
		const remaining = totalProducts - (offset + productsToProcess.length);

		console.log(`[批次 ${batchNumber}] ✅ 完成。耗時 ${endTime - startTime}ms。`);
		console.log(importLog.join('\n'));
		
		// 7. 【關鍵】檢查是否還有剩餘，並觸發下一個批次
		if (remaining > 0) {
			console.log(`[批次 ${batchNumber}] 偵測到還有 ${remaining} 筆，正在觸發下一批次 (batch=${nextBatch})...`);
			
			// 構造下一個批次的 URL
			const nextUrl = new URL(request.url);
      // 確保我們呼叫的是絕對路徑
      nextUrl.pathname = '/admin/process-batch';
			nextUrl.searchParams.set('batch', nextBatch.toString());
			
			// 在背景中呼叫自己
			ctx.waitUntil(
				fetch(nextUrl.toString(), {
					method: 'POST',
					headers: { 'Content-Type': 'application/json' },
				})
			);
		} else {
			console.log(`[批次 ${batchNumber}] 🎉 全部 ${totalProducts} 筆商品匯入完成！`);
		}
		
	} catch (e: any) {
		console.error(`[批次處理失敗] ${e.message}`, e.stack);
	}
}


// ===========================================
// === 5. 輔助函式 (Helpers) ===
// ===========================================

/**
 * AI 提示模板 (v7 規則更新版)
 */
function getAudiencePrompt_v7(product: any): string {
	const description = (product['商品介紹'] || '').substring(0, 300);
	return `
		你是一個資料庫ETL專家。
		請根據以下商品資料，判斷其主要適用物種 (Audience)。

		產品名稱: ${product['產品名稱']}
		類別: ${product['類別']}
		商品介紹: ${description}

		你的回答必須是一個 JSON 陣列，只能包含 "Dog", "Cat", "Humans", "other" 這幾個值。
		
		**重要規則:**
		1.  **"SPA礦泉浴", "香薰浴鹽", "深海泥洗護"** 這類美容/SPA產品，請根據商品介紹判斷是給寵物 (Dog/Cat) 還是人類 (Humans) 使用。如果介紹中提到 "狗狗" 或 "貓咪"，請分類為 ["Dog", "Cat"] (如果貓狗通用) 或 ["Dog"] (如果只給狗)。
		2.  **"包包", "鑰匙圈", "配件"** 這類商品應分類為 ["Humans"]。
		3.  "迷你犬", "狗狗", "BokBok for Dog" = ["Dog"]
		4.  "貓咪", "貓罐", "BokBok for Cat" = ["Cat"]
		5.  如果商品介紹明顯提到貓狗通用 = ["Dog", "Cat"]
		6.  如果都無法判斷 = ["other"]

		範例:
		- 產品名稱 "耐咬史迪克-XS（迷你犬）": ["Dog"]
		- 產品名稱 "毛孩快跑-橘鮮蝦貓罐": ["Cat"]
		- 產品名稱 "SPA礦泉浴", 介紹 "讓狗狗的毛髮...": ["Dog"]
		- 產品名稱 "寵物造型鑰匙圈": ["Humans"]
	`;
}

/**
 * 輔助函式：解析 '商品圖檔' 欄位中的多個 URL
 */
function parseImageUrls(cellContent: string): string[] {
	if (!cellContent) return [];
	const urlRegex = /\((https:\/\/[^)]+)\)/g;
	const matches = cellContent.matchAll(urlRegex);
	return Array.from(matches, (match) => match[1]);
}

/**
 * 輔助函式：從 URL 下載圖片並上傳到 R2
 */
async function fetchAndUploadImage(url: string, r2Key: string, bucket: R2Bucket) {
	try {
		const response = await fetch(url);
		if (!response.ok) {
			throw new Error(`下載失敗: ${response.status} ${response.statusText}`);
		}
		const imageBuffer = await response.arrayBuffer();
		const contentType = response.headers.get('Content-Type') || 'image/jpeg';
		await bucket.put(r2Key, imageBuffer, {
			httpMetadata: { contentType },
		});
	} catch (error: any) {
		console.error(`圖片處理失敗 (URL: ${url}, R2Key: ${r2Key}): ${error.message}`);
	}
}

/**
 * 輔助函式：準備 D1 商品資料 (v11 版)
 */
function getProductSqlStatements_v11(
	row: any,
	sku: string,
	supplierId: string,
	audienceTags: string[],
	db: D1Database,
): D1PreparedStatement[] {
	const statements: D1PreparedStatement[] = [];

	// 1. 寫入 'Products' 主檔
	statements.push(
		db
			.prepare(
				`INSERT OR IGNORE INTO Products (
					sku, supplier_id, name, name_en, barcode, brand_name, 
					description, ingredients, size_dimensions, weight_g, 
					origin, msrp, case_pack, is_public, is_active_product
				) 
			 	 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)` // 欄位已更新
			)
			.bind(
				sku,
				supplierId,
				row['產品名稱'] || '',
				row['英文品名'] || '', // 【新】英文品名
				row['國際條碼'] || null,
				row['品牌名稱'] || '',
				row['商品介紹'] || '',
				row['成份/材質'] || '',
				row['商品尺寸'] || '',
				parseFloat(row['重量g']) || 0,
				row['產地'] || '',
				parseInt(String(row['建議售價']).replace('$', '')) || 0,
				row['箱入數'] || '',
				row['現貨商品'] === '是' ? 1 : 0 // 【新】現貨商品 (1=是, 0=否)
			),
	);

	// 2. 寫入 'ProductInventory' 庫存
	statements.push(
		db
			.prepare(
				`INSERT OR IGNORE INTO ProductInventory (sku, available_good, available_defective, last_synced_at) 
			 VALUES (?, ?, ?, datetime('now'))`,
			)
			.bind(
				sku,
				parseInt(row['庫存_正品_可用']) || 0,
				parseInt(row['庫存_次品_可用']) || 0,
			),
	);

	// 3. 寫入 'ProductTags' 標籤
	if (row['類別']) {
		statements.push(db.prepare(`INSERT OR IGNORE INTO ProductTags (sku, tag) VALUES (?, ?)`).bind(sku, row['類別']));
	}

	// 4. 寫入 'ProductAudience' (AI 產生的)
	for (const tag of audienceTags) {
		if (tag) {
			statements.push(db.prepare(`INSERT OR IGNORE INTO ProductAudience (sku, audience_tag) VALUES (?, ?)`).bind(sku, tag));
		}
	}

	return statements;
}

// ===========================================
// === 6. Hono 最終啟動點 ===
// ===========================================
export default app;
