/*
 * 檔案: src/index.ts
 * 版本: V15 (D1+R2+AI 最終修正)
 * 備註: 這是您的核心後端 API 伺服器。
 * - [D1 修正] 新增 "ensureSupplierExists" 邏輯，自動建立不存在的供應商。
 * - [R2 修正] 使用 `await fetchAndUploadImage`，確保圖片上傳成功並正確回報錯誤。
 * - [AI 修正] 使用您確認過的 `gemini-2.5-flash` 模型。
 */

import { Hono } from 'hono';
import { html } from 'hono/html';
import { GoogleGenerativeAI } from '@google/generative-ai';
import { parse } from 'csv-parse/browser/esm/sync';
import * as bcrypt from 'bcryptjs';

export interface Env {
	DB: D1Database;
	FILES: R2Bucket;
	GEMINI_API_KEY: string;
	REGISTRATION_KEY: string;
}

// --- 設定 ---
const BATCH_SIZE = 3; 
const CSV_FILE_NAME = 'product_inventory_master_v2.csv'; // R2 上的 CSV 檔案
const BCRYPT_SALT_ROUNDS = 10;
// -------------

const app = new Hono<{ Bindings: Env }>();

// ===========================================
// === 2. API 路由 (v12 保留：認證) ===
// ===========================================

app.post('/api/auth/register', async (c) => {
	const body = await c.req.json();
	const { email, password, key } = body;
	if (!email || !password || !key) {
		return c.json({ error: '缺少 email, password, 或 key' }, 400);
	}
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
		user: { userId: user.user_id, email: user.email, role: user.role },
	});
});

// ===========================================
// === 3. API 路由 (v15 修正：匯入工具) ===
// ===========================================

app.get('/api/admin/batch-import', async (c) => {
	const env = c.env;
	const url = new URL(c.req.url);

	try {
		const startTime = Date.now();
		const genAI = new GoogleGenerativeAI(env.GEMINI_API_KEY);
		
        // v15 修正：使用您確認過的 `gemini-2.5-flash`
		const model = genAI.getGenerativeModel({ model: 'gemini-2.5-flash' }); 

		const DB = env.DB;
		const R2_BUCKET = env.FILES;
		const batchNumber = parseInt(url.searchParams.get('batch') || '1', 10);
		const offset = (batchNumber - 1) * BATCH_SIZE;

		const csvObject = await R2_BUCKET.get(CSV_FILE_NAME);
		if (csvObject === null) {
			return c.json({ error: `R2 儲存桶中找不到檔案: ${CSV_FILE_NAME}` }, 404);
		}
		const csvText = await csvObject.text();
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

		const importLog: string[] = [];
		let dbStatements: D1PreparedStatement[] = [];

		for (const row of productsToProcess) {
			const sku = row['商品貨號'];
            // v14 邏輯：從 CSV 讀取供應商，如果為空才預設為 'WEDO'
			const supplierId = row['供應商'] || 'WEDO';
			if (!sku) continue;

			// --- v14 修正：D1 FOREIGN KEY 錯誤 ---
			// 1. 在處理商品前，先確保供應商存在於 D1
			// ------------------------------------
			try {
                // 這個函式會檢查 'BokBok' (或 'WEDO') 是否存在，不存在則建立
				await ensureSupplierExists(DB, supplierId);
			} catch (supplierError: any) {
				importLog.push(`🔴 SKU ${sku} 失敗：無法建立供應商 "${supplierId}": ${supplierError.message}`);
				continue; // 跳過這個商品
			}
			// ------------------------------------
			
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
			importLog.push(`SKU ${sku} -> 供應商: [${supplierId}] -> 客群: [${audienceTags.join(', ')}] -> 已準備 D1`);

			// --- v14 修正：R2 圖片上傳 (使用 await) ---
			// 5c. 處理圖片 (現在會等待上傳完成)
			// ---------------------------------------
			const imageUrls = parseImageUrls(row['商品圖檔']);
			let imageIndex = 0;
			for (const imageUrl of imageUrls) {
				const isPrimary = imageIndex === 0 ? 1 : 0;
				const r2Key = `${supplierId}/${sku}/image-${imageIndex + 1}.jpg`;
				try {
					// v14 修正：直接 await
					await fetchAndUploadImage(imageUrl, r2Key, R2_BUCKET);
			
					// 只有在上傳成功後，才將 SQL 加入批次
					dbStatements.push(
						DB.prepare(`INSERT OR IGNORE INTO ProductImages (sku, r2_key, is_primary) VALUES (?, ?, ?)`).bind(
							sku,
							r2Key,
							isPrimary,
						),
					);
					importLog.push(`  └ 圖片 ${imageIndex + 1} -> 已上傳至 R2: ${r2Key}`);
			
				} catch (imgError: any) {
					// v14 修正：現在可以捕捉到 Airtable 過期網址的錯誤
					importLog.push(`  └ 🔴 圖片 ${imageIndex + 1} (${imageUrl.substring(0, 30)}...) 處理失敗: ${imgError.message}`);
				}
				imageIndex++;
			}
			// ---------------------------------------
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
 * 匯入工具 UI (v13 保留)
 */
app.get('/admin/importer', (c) => {
	// v15 修正：更新 UI 標題
	return c.html(html`
		<!DOCTYPE html>
		<html lang="zh-Hant">
			<head>
				<meta charset="UTF-8" />
				<meta name="viewport" content="width=device-width, initial-scale=1.0" />
				<title>雙核星鏈 - 即時匯入工具 (v15)</title>
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
					<h1>雙核星鏈 (GeminiLink) - 即時匯入工具 (v15)</h1>
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
							// 呼叫我們自己的 v15 API
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
								data.logs.forEach(log => {
									const isError = log.includes('失敗') || log.includes('🔴');
									addLog(log, isError ? 'error' : '');
								});
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
						if (logsContainer.children.length === 1 && logsContainer.children[0].textContent === '等待開始...') {
							logsContainer.innerHTML = '';
						}
						const entry = document.createElement('div');
						entry.className = \`log-entry \${type}\`;
						entry.textContent = message;
						logsContainer.appendChild(entry);
						logsContainer.scrollTop = logsContainer.scrollHeight;
					}
				</script>
			</body>
		</html>
	`);
});

// ===========================================
// === 5. 輔助函式 (Helpers) (v14 新增/修改) ===
// ===========================================

/**
 * v14 新增：確保供應商存在
 */
async function ensureSupplierExists(db: D1Database, supplierId: string) {
	const supplier = await db.prepare('SELECT supplier_id FROM Suppliers WHERE supplier_id = ?').bind(supplierId).first();
	if (supplier) {
		return;
	}
	const tempEmail = `${supplierId.toLowerCase().replace(/\s+/g, '')}@geminilink.auto`;
	await db.prepare('INSERT INTO Suppliers (supplier_id, name, email) VALUES (?, ?, ?)')
		.bind(supplierId, supplierId, tempEmail)
		.run();
	console.warn(`自動建立了新供應商: ${supplierId}`);
}


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
	// v14 修正：移除內部 try...catch，讓錯誤可以被上層捕捉
	const response = await fetch(url);
	if (!response.ok) {
		throw new Error(`下載失敗: ${response.status} ${response.statusText}`);
	}
	const imageBuffer = await response.arrayBuffer();
	const contentType = response.headers.get('Content-Type') || 'image/jpeg';
	
	await bucket.put(r2Key, imageBuffer, {
		httpMetadata: { contentType },
	});
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
			 	 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)`
			)
			.bind(
				sku,
				supplierId,
				row['產品名稱'] || '',
				row['英文品名'] || '', 
				row['國際條碼'] || null,
				row['品牌名稱'] || '',
				row['商品介紹'] || '',
				row['成份/材質'] || '',
				row['商品尺寸'] || '',
				parseFloat(row['重量g']) || 0,
				row['產地'] || '',
				parseInt(String(row['建議售價']).replace('$', '')) || 0,
				row['箱入數'] || '',
				row['現貨商品'] === '是' ? 1 : 0
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
