/*
 * 檔案: src/index.ts
 * 版本: V13 (Hono + 即時監控 UI)
 * 備註: 這是您的核心後端 API 伺服器。
 * - 移除了 v12 失敗的 "全自動" 背景迴圈 (ctx.waitUntil)。
 * - 恢復 v9 的 `GET /api/admin/batch-import` API，它會處理一個批次並 "回傳 JSON 報告"。
 * - 新增 `GET /admin/importer` API，它會回傳一個 "HTML 頁面"。
 * - 這個 HTML 頁面上的 JavaScript 將在 "使用者瀏覽器" 中執行迴圈，
 * 提供即時、可監控的匯入進度。
 */

import { Hono } from 'hono';
import { html } from 'hono/html'; // v13 新增：用於回傳 HTML 頁面
import { GoogleGenerativeAI } from '@google/generative-ai';
import { parse } from 'csv-parse/browser/esm/sync'; // v3 修正版
import * as bcrypt from 'bcryptjs';

/**
 * 環境變數 (來自 wrangler.toml 和 Cloudflare Secrets)
 */
export interface Env {
	DB: D1Database;
	FILES: R2Bucket;
	GEMINI_API_KEY: string;
	REGISTRATION_KEY: string;
}

// --- 設定 ---
const BATCH_SIZE = 3; // 每個批次處理 3 筆 (因為圖片處理耗時)
const CSV_FILE_NAME = 'product_inventory_master_v2.csv'; // R2 上的 CSV 檔案
const BCRYPT_SALT_ROUNDS = 10;

// ===========================================
// === 1. 初始化 Hono App (您的 API 路由器) ===
// ===========================================
const app = new Hono<{ Bindings: Env }>();

// ===========================================
// === 2. API 路由 (v12 保留：認證) ===
// ===========================================

/**
 * POST /api/auth/register
 * 註冊您的第一個 admin 帳號。
 */
app.post('/api/auth/register', async (c) => {
	const body = await c.req.json();
	const { email, password, key } = body;

	if (!email || !password || !key) {
		return c.json({ error: '缺少 email, password, 或 key' }, 400);
	}

	// 驗證 Registration Key
	if (key !== c.env.REGISTRATION_KEY) {
		return c.json({ error: '無效的註冊安全碼' }, 403);
	}

	try {
		const existingUser = await c.env.DB.prepare('SELECT user_id FROM Users WHERE email = ?').bind(email).first();
		if (existingUser) {
			return c.json({ error: '此 email 已被註冊' }, 409);
		}
		const passwordHash = await bcrypt.hash(password, BCRYPT_SALT_ROUNDS);
		await c.env.DB.prepare(
			`INSERT INTO Users (email, password_hash, role, supplier_id) 
       VALUES (?, ?, 'admin', NULL)`,
		)
			.bind(email, passwordHash)
			.run();

		return c.json({ message: 'Admin 帳號建立成功' });
	} catch (e: any) {
		return c.json({ error: '資料庫錯誤', message: e.message }, 500);
	}
});

/**
 * POST /api/auth/login
 * 登入以取得權限 (未來用於前端介面)
 */
app.post('/api/auth/login', async (c) => {
	const body = await c.req.json();
	const { email, password } = body;

	if (!email || !password) {
		return c.json({ error: '缺少 email 或 password' }, 400);
	}

	const user = await c.env.DB.prepare(
    'SELECT user_id, email, password_hash, role FROM Users WHERE email = ?'
  ).bind(email).first<{ user_id: number; email: string; password_hash: string; role: string }>();

	if (!user) {
		return c.json({ error: '帳號或密碼錯誤' }, 401);
	}

	const isPasswordValid = await bcrypt.compare(password, user.password_hash);
	if (!isPasswordValid) {
		return c.json({ error: '帳號或密碼錯誤' }, 401);
	}

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
// === 3. API 路由 (v13 修正：匯入工具) ===
// ===========================================

/**
 * GET /api/admin/batch-import
 * 處理器 API (v9 恢復)
 * * 處理一個批次 (例如 batch=1)，然後 "回傳 JSON 報告"。
 * 這個 API 會被 /admin/importer 頁面上的 JavaScript 呼叫。
 */
app.get('/api/admin/batch-import', async (c) => {
	const env = c.env;
	const ctx = c.executionCtx;
	const url = new URL(c.req.url);

	try {
		const startTime = Date.now();

		// 1. 初始化服務
		const genAI = new GoogleGenerativeAI(env.GEMINI_API_KEY);
		const model = genAI.getGenerativeModel({ model: 'gemini-2.5-flash' }); // v9
		const DB = env.DB;
		const R2_BUCKET = env.FILES;

		// 2. 取得批次編號
		const batchNumber = parseInt(url.searchParams.get('batch') || '1', 10);
		const offset = (batchNumber - 1) * BATCH_SIZE;

		// 3. 從 R2 讀取 CSV
		const csvObject = await R2_BUCKET.get(CSV_FILE_NAME);
		if (csvObject === null) {
			return c.json({ error: `R2 儲存桶中找不到檔案: ${CSV_FILE_NAME}` }, 404);
		}
		const csvText = await csvObject.text();

		// 4. 解析 CSV
		const allProducts: any[] = parse(csvText, {
			columns: true,
			skip_empty_lines: true,
			bom: true, // v4
		});

		const totalProducts = allProducts.length;
		const productsToProcess = allProducts.slice(offset, offset + BATCH_SIZE);

		if (productsToProcess.length === 0) {
			return c.json({
				message: '🎉 全部匯入完成！',
				processed: 0,
				remaining: 0,
				totalProducts: totalProducts,
			});
		}

		// 5. 處理這個批次的商品
		const importLog: string[] = [];
		let dbStatements: D1PreparedStatement[] = [];

		for (const row of productsToProcess) {
			const sku = row['商品貨號'];
			const supplierId = row['供應商'] || 'WEDO';
			if (!sku) continue;

			// 5a. 呼叫 AI
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

			// 5b. 準備 SQL (v11)
			const productStatements = getProductSqlStatements_v11(row, sku, supplierId, audienceTags, DB);
			dbStatements.push(...productStatements);
			importLog.push(`SKU ${sku} -> 客群: [${audienceTags.join(', ')}] -> 已準備匯入 D1`);

			// 5c. 處理圖片
			const imageUrls = parseImageUrls(row['商品圖檔']);
			let imageIndex = 0;
			for (const imageUrl of imageUrls) {
				const isPrimary = imageIndex === 0 ? 1 : 0;
				const r2Key = `${supplierId}/${sku}/image-${imageIndex + 1}.jpg`;
				try {
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

		// 6. 執行 D1 批次
		if (dbStatements.length > 0) {
			await DB.batch(dbStatements);
		} else {
			importLog.push('警告：這個批次沒有產生任何 SQL 語句。');
		}

		const endTime = Date.now();
		const nextBatch = batchNumber + 1;
		const remaining = totalProducts - (offset + productsToProcess.length);

		// 7. 回傳 JSON 報告
		return c.json({
			message: `✅ 批次 ${batchNumber} 完成。`,
			processed: productsToProcess.length,
			remaining: remaining,
			totalProducts: totalProducts,
			nextBatch: remaining > 0 ? nextBatch : null,
			duration: `${(endTime - startTime) / 1000} 秒`,
			logs: importLog,
		});
	} catch (e: any) {
		return c.json({ error: '批次匯入失敗', message: e.message, stack: e.stack }, 500);
	}
});

/**
 * GET /admin/importer
 * 匯入工具 UI (v13 新增)
 * * 回傳一個 HTML 頁面，頁面上的 JavaScript 會自動執行批次匯入
 * 並在畫面上顯示即時日誌。
 */
app.get('/admin/importer', (c) => {
	// 我們使用 Hono 的 'html' 輔助工具來回傳 HTML 內容
	return c.html(html`
		<!DOCTYPE html>
		<html lang="zh-Hant">
			<head>
				<meta charset="UTF-8" />
				<meta name="viewport" content="width=device-width, initial-scale=1.0" />
				<title>雙核星鏈 - 即時匯入工具</title>
				<style>
					body {
						font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
						margin: 0;
						padding: 2rem;
						background-color: #f4f7f6;
						color: #333;
					}
					#root {
						max-width: 800px;
						margin: 0 auto;
						padding: 2rem;
						background-color: #ffffff;
						border-radius: 8px;
						box-shadow: 0 4px 12px rgba(0, 0, 0, 0.05);
					}
					h1 {
						color: #111;
						border-bottom: 2px solid #eee;
						padding-bottom: 10px;
					}
					button {
						font-size: 1rem;
						padding: 10px 15px;
						color: #fff;
						background-color: #007bff;
						border: none;
						border-radius: 5px;
						cursor: pointer;
						transition: background-color 0.2s;
					}
					button:disabled {
						background-color: #ccc;
						cursor: not-allowed;
					}
					button:hover:not(:disabled) {
						background-color: #0056b3;
					}
					#logs {
						font-family: 'SF Mono', 'Consolas', 'Menlo', monospace;
						font-size: 0.85rem;
						background-color: #2b2b2b;
						color: #f8f8f2;
						padding: 1rem;
						border-radius: 5px;
						margin-top: 1.5rem;
						max-height: 400px;
						overflow-y: auto;
						white-space: pre-wrap;
						word-wrap: break-word;
					}
					.log-entry {
						padding: 2px 0;
						border-bottom: 1px solid #444;
					}
					.log-entry.batch-start {
						color: #50e3c2; /* 亮青色 */
						font-weight: bold;
						margin-top: 10px;
					}
					.log-entry.error {
						color: #ff4d4d; /* 紅色 */
					}
					.log-entry.success {
						color: #7ed321; /* 綠色 */
						font-size: 1.1rem;
						font-weight: bold;
					}
					#status {
						font-size: 1.1rem;
						font-weight: 500;
						margin-top: 1rem;
					}
				</style>
			</head>
			<body>
				<div id="root">
					<h1>雙核星鏈 (GeminiLink) - 即時匯入工具 (v13)</h1>
					<p>點擊按鈕開始將 R2 (geminilink-files) 中的 CSV 檔案匯入 D1 (geminilink_db)。</p>
					<p>匯入將在您的瀏覽器中自動分批執行，請保持此頁面開啟直到完成。</p>
					<button id="start-button">開始全自動匯入</button>
					
					<div id="status">狀態：待命中...</div>
					<div id="logs">
						<div class="log-entry">等待開始...</div>
					</div>
				</div>

				<script>
					const startButton = document.getElementById('start-button');
					const logsContainer = document.getElementById('logs');
					const statusElement = document.getElementById('status');
					let totalProducts = 0;

					startButton.addEventListener('click', () => {
						startButton.disabled = true;
						startButton.textContent = '匯入中...';
						addLog('初始化...', 'batch-start');
						runBatch(1); // 從批次 1 開始
					});

					async function runBatch(batchNumber) {
						if (!batchNumber) {
							addLog('🎉 全部匯入完成！', 'success');
							statusElement.textContent = \`狀態：全部 \${totalProducts} 筆商品已完成匯入！\`;
							startButton.disabled = false;
							startButton.textContent = '重新開始';
							return;
						}

						statusElement.textContent = \`狀態：正在處理批次 \${batchNumber}...\`;
						addLog(\`--- 開始處理批次 \${batchNumber} --- \`, 'batch-start');

						try {
							// 呼叫我們自己的 v13 API
							const response = await fetch(\`/api/admin/batch-import?batch=\${batchNumber}\`);
							if (!response.ok) {
								const errData = await response.json().catch(() => ({}));
								throw new Error(\`HTTP 錯誤！狀態: \${response.status} - \${errData.message || response.statusText}\`);
							}
							
							const data = await response.json();

							if (data.error) {
								throw new Error(data.message);
							}
							
							// 顯示 AI 和圖片處理日誌
							if (data.logs && Array.isArray(data.logs)) {
								data.logs.forEach(log => addLog(log));
							}

							totalProducts = data.totalProducts || totalProducts;
							const processedCount = (totalProducts - (data.remaining || 0));
							statusElement.textContent = \`狀態：批次 \${batchNumber} 完成。 (\${processedCount} / \${totalProducts})\`;
							
							// 遞迴呼叫下一個批次
							setTimeout(() => {
								runBatch(data.nextBatch);
							}, 500); // 批次之間延遲 0.5 秒

						} catch (error) {
							addLog(\`批次 \${batchNumber} 失敗: \${error.message}\`, 'error');
							statusElement.textContent = \`狀態：批次 \${batchNumber} 失敗。請檢查日誌並重試。\`;
							startButton.disabled = false;
							startButton.textContent = '重試';
						}
					}

					function addLog(message, type = '') {
						// 第一次清除 "等待開始"
						if (logsContainer.children.length === 1 && logsContainer.children[0].textContent === '等待開始...') {
							logsContainer.innerHTML = '';
						}
						const entry = document.createElement('div');
						entry.className = \`log-entry \${type}\`;
						entry.textContent = message;
						logsContainer.appendChild(entry);
						// 自動捲動到底部
						logsContainer.scrollTop = logsContainer.scrollHeight;
					}
				</script>
			</body>
		</html>
	`);
});

// ===========================================
// === 5. 輔助函式 (Helpers) (來自 v11) ===
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
				row['英文品名'] || '', // 【v11 新增】英文品名
				row['國際條碼'] || null,
				row['品牌名稱'] || '',
				row['商品介紹'] || '',
				row['成份/材質'] || '',
				row['商品尺寸'] || '',
				parseFloat(row['重量g']) || 0,
				row['產地'] || '',
				parseInt(String(row['建議售價']).replace('$', '')) || 0,
				row['箱入數'] || '',
				row['現貨商品'] === '是' ? 1 : 0 // 【v11 新增】現貨商品
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
