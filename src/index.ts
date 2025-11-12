/*
 * 檔案: src/index.ts
 * 版本: V18 (動態表格 + 手動供應商)
 * 備註: 這是您的核心後端 API 伺服器。
 * - [API 新增] /api/admin/airtable-tables API，用於抓取 Base 中的所有表格。
 * - [API 升級] /api/admin/batch-import 現在需要 "table_id" 和 "supplier_id" 兩個參數。
 * - [API 升級] 匯入邏輯不再猜測供應商，而是使用您傳入的 "supplier_id"。
 * - [UI 升級] /admin/importer 頁面現在會動態載入「下拉選單」並新增「供應商 ID」輸入框。
 */

import { Hono } from 'hono';
import { html } from 'hono/html';
import { GoogleGenerativeAI } from '@google/generative-ai';
import * as bcrypt from 'bcryptjs';
import Airtable from 'airtable';

// v17: 我們不再需要 AIRTABLE_TABLE_NAME
export interface Env {
	DB: D1Database;
	FILES: R2Bucket;
	GEMINI_API_KEY: string;
	REGISTRATION_KEY: string;
	AIRTABLE_API_KEY: string;
	AIRTABLE_BASE_ID: string;
}

// --- 設定 ---
const BATCH_SIZE = 3; // 每次處理 3 筆
const BCRYPT_SALT_ROUNDS = 10;
// -------------

const app = new Hono<{ Bindings: Env }>();

// ===========================================
// === 2. API 路由 (v12 保留：認證) ===
// ===========================================
// (此區塊程式碼與 v17 相同，保持不變)
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
// === 3. API 路由 (v18 升級：匯入) ===
// ===========================================

/**
 * [v18 新增] API 1: 取得 Airtable Base 中的所有表格
 * 用於填充 UI 上的下拉選單
 */
app.get('/api/admin/airtable-tables', async (c) => {
	const env = c.env;
	try {
		// 這是 Airtable 的 Metadata API
		const url = `https://api.airtable.com/v0/meta/bases/${env.AIRTABLE_BASE_ID}/tables`;
		const response = await fetch(url, {
			headers: {
				Authorization: `Bearer ${env.AIRTABLE_API_KEY}`,
			},
		});

		if (!response.ok) {
			const errText = await response.text();
			throw new Error(`Airtable Metadata API 錯誤: ${response.status} ${errText}`);
		}

		const data: any = await response.json();
		
		// 我們只需要 id 和 name
		const tables = data.tables.map((table: any) => ({
			id: table.id,
			name: table.name,
		}));

		return c.json(tables);
	} catch (e: any) {
		return c.json({ error: '無法取得 Airtable 表格列表', message: e.message }, 500);
	}
});


/**
 * [v18 升級] API 2: 批次匯入
 * 現在需要 "table_id" 和 "supplier_id"
 */
app.get('/api/admin/batch-import', async (c) => {
	const env = c.env;
	const url = new URL(c.req.url);

	try {
		const startTime = Date.now();
		// 1. 初始化服務
		const genAI = new GoogleGenerativeAI(env.GEMINI_API_KEY);
		const model = genAI.getGenerativeModel({ model: 'gemini-2.5-flash' });
		const DB = env.DB;
		const R2_BUCKET = env.FILES;
		Airtable.configure({ apiKey: env.AIRTABLE_API_KEY });
		const base = Airtable.base(env.AIRTABLE_BASE_ID);
		
		// 2. v18 升級：取得 table_id, offset, 和 supplier_id
		const offset = url.searchParams.get('offset') || undefined;
		const tableId = url.searchParams.get('table_id');
		const supplierId = url.searchParams.get('supplier_id'); // v18 新增

		if (!tableId || !supplierId) {
			return c.json({ error: 'Airtable Table ID (table_id) 和 供應商 ID (supplier_id) 都是必要參數' }, 400);
		}

		// 3. 使用動態 tableId 抓取資料
		const records = await base(tableId)
			.select({
				pageSize: BATCH_SIZE,
				offset: offset,
			})
			.firstPage();

		const productsToProcess = records.map((record) => record.fields);
		const newOffset = records.offset;

		if (productsToProcess.length === 0) {
			return c.json({
				message: '🎉 全部匯入完成！',
				processed: 0,
				remaining: 0,
				nextOffset: null,
			});
		}

		// 5. 處理這個批次的 3 筆商品
		const importLog: string[] = [];
		let dbStatements: D1PreparedStatement[] = [];

		for (const row of productsToProcess) {
			const sku = row['商品貨號'] as string;
			
			// v18 升級：不再猜測供應商，直接使用傳入的 supplierId
			if (!sku) continue;

			// 5a. 確保供應商存在
			try {
				await ensureSupplierExists(DB, supplierId);
			} catch (supplierError: any) {
				importLog.push(`🔴 SKU ${sku} 失敗：無法建立供應商 "${supplierId}": ${supplierError.message}`);
				continue;
			}
			
			// 5b. 呼叫 AI
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

			// 5c. 準備 SQL
			const productStatements = getProductSqlStatements_v16(row, sku, supplierId, audienceTags, DB);
			dbStatements.push(...productStatements);
			importLog.push(`SKU ${sku} -> 供應商: [${supplierId}] -> 客群: [${audienceTags.join(', ')}] -> 已準備 D1`);

			// 5d. 處理圖片
			const images = (row['商品圖檔'] as any[]) || [];
			let imageIndex = 0;
			for (const image of images) {
				const imageUrl = image.url;
				if (!imageUrl) continue;

				const isPrimary = imageIndex === 0 ? 1 : 0;
				const r2Key = `${supplierId}/${sku}/image-${imageIndex + 1}.jpg`; 
				try {
					await fetchAndUploadImage(imageUrl, r2Key, R2_BUCKET);
					dbStatements.push(
						DB.prepare(`INSERT OR IGNORE INTO ProductImages (sku, r2_key, is_primary) VALUES (?, ?, ?)`).bind(
							sku,
							r2Key,
							isPrimary,
						),
					);
					importLog.push(`  └ 圖片 ${imageIndex + 1} -> 已上傳至 R2: ${r2Key}`);
				} catch (imgError: any) {
					importLog.push(`  └ 🔴 圖片 ${imageIndex + 1} (${imageUrl.substring(0, 30)}...) 處理失敗: ${imgError.message}`);
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

		// 7. 回傳 JSON 報告
		return c.json({
			message: `✅ 批次 (Table: ${tableId}, Offset: ${offset || 'start'}) 完成。`,
			processed: productsToProcess.length,
			nextOffset: newOffset || null, 
			duration: `${(endTime - startTime) / 1000} 秒`,
			logs: importLog,
		});
	} catch (e: any) {
		return c.json({ error: '批次匯入失敗', message: e.message, stack: e.stack }, 500);
	}
});

/**
 * GET /admin/importer
 * 匯入工具 UI (v18 升級)
 */
app.get('/admin/importer', (c) => {
	// v18 升級：UI 標題和 JavaScript 邏輯已更新
	return c.html(html`
		<!DOCTYPE html>
		<html lang="zh-Hant">
			<head>
				<meta charset="UTF-8" />
				<meta name="viewport" content="width=device-width, initial-scale=1.0" />
				<title>雙核星鏈 - Airtable 匯入工具 (v18)</title>
				<style>
					/* ... (v17 的 CSS 樣式保持不變) ... */
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
					/* v18 升級：表單樣式 */
					#importer-form {
						display: grid;
						grid-template-columns: 1fr;
						gap: 10px;
						margin-bottom: 1rem;
						padding: 1rem;
						background-color: #fafafa;
						border-radius: 5px;
					}
					@media (min-width: 600px) {
						#importer-form {
							grid-template-columns: 1fr 1fr auto;
						}
					}
					.form-group {
						display: flex;
						flex-direction: column;
					}
					.form-group label {
						font-size: 0.85rem;
						font-weight: 500;
						margin-bottom: 4px;
						color: #555;
					}
					.form-group select, .form-group input {
						font-size: 1rem;
						padding: 10px;
						border: 1px solid #ccc;
						border-radius: 5px;
					}
					#start-button {
						align-self: end; /* 對齊到 grid 的底部 */
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
					<h1>雙核星鏈 (GeminiLink) - Airtable 匯入工具 (v18)</h1>
					<p>系統已自動抓取您 Airtable Base 中的所有表格。請選擇要匯入的表格，並手動指定一個供應商 ID。</p>
					
					<!-- v18 升級：改為表單 -->
					<div id="importer-form">
						<div class="form-group">
							<label for="table-select">1. 選擇 Airtable 表格</label>
							<select id="table-select" disabled>
								<option value="">載入中...</option>
							</select>
						</div>
						<div class="form-group">
							<label for="supplier-id-input">2. 指定供應商 ID</label>
							<input type="text" id="supplier-id-input" placeholder="例如: WEDO (將用於 R2 資料夾)" />
						</div>
						<button id="start-button" disabled>載入表格中...</button>
					</div>
					
					<div id="status">狀態：待命中...</div>
					<div id="logs">
						<div class="log-entry">等待開始...</div>
					</div>
				</div>

				<script>
					const startButton = document.getElementById('start-button');
					const tableSelect = document.getElementById('table-select');
					const supplierIdInput = document.getElementById('supplier-id-input');
					const logsContainer = document.getElementById('logs');
					const statusElement = document.getElementById('status');
					let totalProcessed = 0;

					// v18 升級：頁面載入時，自動抓取表格列表
					window.addEventListener('load', async () => {
						try {
							const response = await fetch('/api/admin/airtable-tables');
							if (!response.ok) {
								throw new Error('無法抓取表格列表');
							}
							const tables = await response.json();
							
							tableSelect.innerHTML = '<option value="">-- 請選擇一個表格 --</option>'; // 清空 "載入中..."
							tables.forEach(table => {
								const option = document.createElement('option');
								option.value = table.id; // "tblRUJ..."
								option.textContent = table.name; // "WEDO商品"
								tableSelect.appendChild(option);
							});
							tableSelect.disabled = false;
							startButton.disabled = false;
							startButton.textContent = '開始全自動匯入';

						} catch (error) {
							addLog(\`🔴 嚴重錯誤：無法載入 Airtable 表格列表。 \${error.message}\`, 'error');
							statusElement.textContent = '狀態：初始化失敗。';
						}
					});

					startButton.addEventListener('click', () => {
						// v18 升級：從下拉選單和輸入框讀取
						const tableId = tableSelect.value;
						const supplierId = supplierIdInput.value;

						if (!tableId) {
							addLog('錯誤：請從下拉選單選擇一個表格。', 'error');
							return;
						}
						if (!supplierId) {
							addLog('錯誤：請輸入一個供應商 ID (例如 WEDO)。', 'error');
							return;
						}

						startButton.disabled = true;
						tableSelect.disabled = true;
						supplierIdInput.disabled = true;
						startButton.textContent = '匯入中...';
						addLog(\`初始化... 準備匯入表格: \${tableId} | 供應商: \${supplierId}\`, 'batch-start');
						totalProcessed = 0;
						runBatch(tableId, supplierId, null); // v18 升級：傳入 tableId 和 supplierId
					});

					async function runBatch(tableId, supplierId, offset) {
						// v18 升級：offset 為 null (最後一頁) 時結束
						if (offset === 'STOP') {
							addLog(\`🎉 全部匯入完成！總共處理 \${totalProcessed} 筆商品。\`, 'success');
							statusElement.textContent = \`狀態：全部 \${totalProcessed} 筆商品已完成匯入！\`;
							startButton.disabled = false;
							tableSelect.disabled = false;
							supplierIdInput.disabled = false;
							startButton.textContent = '重新開始';
							return;
						}

						const offsetString = offset || 'START';
						statusElement.textContent = \`狀態：正在處理 (Offset: \${offsetString})...\`;
						addLog(\`--- 開始處理 (Table: \${tableId}, Supplier: \${supplierId}, Offset: \${offsetString}) --- \`, 'batch-start');

						try {
							// v18 升級：API 路徑現在包含 table_id, supplier_id, 和 offset
							const apiUrl = new URL('/api/admin/batch-import', window.location.origin);
							apiUrl.searchParams.set('table_id', tableId);
							apiUrl.searchParams.set('supplier_id', supplierId);
							if (offset) {
								apiUrl.searchParams.set('offset', offset);
							}

							const response = await fetch(apiUrl.toString());
							
							if (!response.ok) {
								const errData = await response.json().catch(() => ({}));
								throw new Error(\`HTTP 錯誤！狀態: \${response.status} - \${errData.message || response.statusText}\`);
							}
							
							const data = await response.json();
							if (data.error) {
								throw new Error(data.message);
							}
							
							if (data.logs && Array.isArray(data.logs)) {
								data.logs.forEach(log => {
									const isError = log.includes('失敗') || log.includes('🔴');
									addLog(log, isError ? 'error' : '');
								});
							}

							totalProcessed += data.processed || 0;
							statusElement.textContent = \`狀態：批次完成。 (已處理 \${totalProcessed} 筆商品)\`;
							
							// v18 升級：遞迴呼叫下一個 offset
							const nextOffset = data.nextOffset || 'STOP'; // 如果 nextOffset 是 null，代表結束
							setTimeout(() => {
								runBatch(tableId, supplierId, nextOffset);
							}, 500); // 批次之間延遲 0.5 秒

						} catch (error) {
							addLog(\`批次 (Offset: \${offsetString}) 失敗: \${error.message}\`, 'error');
							statusElement.textContent = \`狀態：批次 (Offset: \${offsetString}) 失敗。請檢查日誌並重試。\`;
							startButton.disabled = false;
							tableSelect.disabled = false;
							supplierIdInput.disabled = false;
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
// === 5. 輔助函式 (Helpers) (v16 修改) ===
// ===========================================
// (此區塊程式碼與 v17 相同，保持不變)
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
	const description = (product['商品介紹'] as string || '').substring(0, 300);
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
 * 輔助函式：解析 '商品圖檔' 欄位
 */
function parseImageUrls(airtableImageField: any): string[] {
	if (!Array.isArray(airtableImageField)) {
		return [];
	}
	return airtableImageField.map((image: any) => image.url).filter(Boolean);
}

/**
 * 輔助函式：從 URL 下載圖片並上傳到 R2
 */
async function fetchAndUploadImage(url: string, r2Key: string, bucket: R2Bucket) {
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
 * 輔助函式：準備 D1 商品資料 (v16 版)
 */
function getProductSqlStatements_v16(
	row: any, // row 現在是 Airtable record.fields
	sku: string,
	supplierId: string,
	audienceTags: string[],
	db: D1Database,
): D1PreparedStatement[] {
	const statements: D1PreparedStatement[] = [];

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

	statements.push(
		db
			.prepare(
				`INSERT OR IGNORE INTO ProductInventory (sku, available_good, available_defective, last_synced_at) 
			 VALUES (?, ?, ?, datetime('now'))`,
			)
			.bind(
				sku,
				0, // v16: 預設為 0
				0  // v16: 預設為 0
			),
	);

	if (row['類別']) {
		statements.push(db.prepare(`INSERT OR IGNORE INTO ProductTags (sku, tag) VALUES (?, ?)`).bind(sku, row['類別']));
	}

	for (const tag of audienceTags) {
		if (tag) {
			statements.push(db.prepare(`INSERT OR IGGNORE INTO ProductAudience (sku, audience_tag) VALUES (?, ?)`).bind(sku, tag));
		}
	}

	return statements;
}

// ===========================================
// === 6. Hono 最終啟動點 ===
// ===========================================
export default app;
