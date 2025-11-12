import { GoogleGenerativeAI } from '@google/generative-ai';
// 修正 v3：使用 "browser/esm/sync" 路徑
// 這才是官方提供給 Worker/瀏覽器環境的正確版本
import { parse } from 'csv-parse/browser/esm/sync';

/**
 * 歡迎使用 雙核星鏈 (GeminiLink) API 伺服器 (v4 - 最終修正版)
 *
 * 環境變數 (來自 wrangler.toml 和 Cloudflare Secrets):
 * - env.DB: 我們的 D1 資料庫 (geminilink_db)
 * - env.FILES: 我們的 R2 儲存桶 (geminilink-files)
 * - env.GEMINI_API_KEY: 您的 Gemini API 金鑰 (來自 Cloudflare Secrets)
 */
export interface Env {
	DB: D1Database;
	FILES: R2Bucket; // 修正 v3：符合 wrangler.toml 的 "FILES" 綁定
	GEMINI_API_KEY: string;
}

// --- 設定 ---
const BATCH_SIZE = 3; // 降至 3 筆，因為圖片處理非常耗時
const CSV_FILE_NAME = 'product_inventory_master_v2.csv'; // 您上傳到 R2 的檔案名稱
// -------------

export default {
	async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
		const url = new URL(request.url);

		// 我們的主 API 端點
		if (url.pathname !== '/admin/batch-import') {
			return new Response(
				'歡迎使用 雙核星鏈 (GeminiLink) API。\n請訪問 /admin/batch-import?batch=1 來啟動匯入。',
				{ status: 404, headers: { 'Content-Type': 'text/plain; charset=utf-8' } },
			);
		}

		// --- 執行批次匯入邏輯 ---
		try {
			const startTime = Date.now();

			// 1. 初始化服務 (從 env 取得)
			const genAI = new GoogleGenerativeAI(env.GEMINI_API_KEY);
			const model = genAI.getGenerativeModel({ model: 'gemini-1.5-flash' });
			const DB = env.DB;
			const R2_BUCKET = env.FILES; // 修正 v3：使用 env.FILES

			// 2. 取得批次編號 (例如 ?batch=1)
			const batchNumber = parseInt(url.searchParams.get('batch') || '1', 10);
			const offset = (batchNumber - 1) * BATCH_SIZE;

			// 3. 從 R2 讀取 CSV 檔案
			const csvObject = await R2_BUCKET.get(CSV_FILE_NAME);
			if (csvObject === null) {
				return Response.json({ error: `R2 儲存桶中找不到檔案: ${CSV_FILE_NAME}` }, { status: 404 });
			}
			const csvText = await csvObject.text();

			// 4. 解析 CSV
			const allProducts: any[] = parse(csvText, {
				columns: true,
				skip_empty_lines: true,
				bom: true, // 修正 v4：移除 Excel CSV 的 UTF-8 BOM
			});

			const totalProducts = allProducts.length;
			const productsToProcess = allProducts.slice(offset, offset + BATCH_SIZE);

			// 如果沒有更多商品，回報完成
			if (productsToProcess.length === 0) {
				return Response.json({
					message: '🎉 全部匯入完成！',
					totalProducts: totalProducts,
				});
			}

			// 5. 處理這個批次的 3 筆商品
			const importLog: string[] = [];
			let dbStatements: D1PreparedStatement[] = [];

			for (const row of productsToProcess) {
				const sku = row['商品貨號'];
				const supplierId = row['供應商'] || 'WEDO'; // 預設為 WEDO
				if (!sku) continue; // 跳過空行

				// 5a. 呼叫 AI 產生「主要客群」
				const prompt = getAudiencePrompt(row);
				let audienceTags: string[] = ['其他']; // 預設值
				try {
					const result = await model.generateContent(prompt);
					const response = result.response.text().trim();
					const cleanedResponse = response.replace(/```json/g, '').replace(/```/g, '');
					const parsedResponse = JSON.parse(cleanedResponse);
					audienceTags = Array.isArray(parsedResponse) ? parsedResponse.filter(Boolean) : ['其他'];
				} catch (aiError: any) {
					importLog.push(`SKU ${sku} AI 失敗: ${aiError.message}. 使用預設值 ['其他']`);
				}

				// 5b. 準備 SQL 批次 (靜態資料)
				// 修正 v3：將 DB 實例傳遞給輔助函式
				const productStatements = getProductSqlStatements(row, sku, supplierId, audienceTags, DB);
				dbStatements.push(...productStatements);
				importLog.push(`SKU ${sku} -> 客群: [${audienceTags.join(', ')}] -> 已準備匯入 D1`);

				// 5c. 【新功能】處理圖片：下載並上傳至 R2
				const imageUrls = parseImageUrls(row['商品圖檔']);
				let imageIndex = 0;
				for (const imageUrl of imageUrls) {
					const isPrimary = imageIndex === 0 ? 1 : 0;
					// 檔名: image-1.jpg, image-2.jpg ... (Airtable 網址沒有副檔名, 我們預設為 .jpg)
					const r2Key = `${supplierId}/${sku}/image-${imageIndex + 1}.jpg`;

					try {
						// 執行下載和上傳 (非同步，但不 block 迴圈)
						// ctx.waitUntil 在背景執行，確保 Worker 不會提早終止
						ctx.waitUntil(fetchAndUploadImage(imageUrl, r2Key, R2_BUCKET));

						// 成功後，準備 SQL 寫入 ProductImages
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
				} // 圖片迴圈結束
			} // 商品迴圈結束

			// 6. 執行 D1 批次匯入 (包含所有商品資料 + 圖片資料)
			if (dbStatements.length > 0) { // 修正 v4：確保有 SQL 才執行
				await DB.batch(dbStatements);
			} else {
				importLog.push('警告：這個批次沒有產生任何 SQL 語句。');
			}

			const endTime = Date.now();
			const nextBatch = batchNumber + 1;
			const remaining = totalProducts - (offset + productsToProcess.length);

			// 7. 回傳 JSON 報告
			return Response.json({
				message: `✅ 批次 ${batchNumber} 完成。`,
				processed: productsToProcess.length,
				remaining: remaining,
				totalProducts: totalProducts,
				nextBatch: remaining > 0 ? nextBatch : null, // 提示下一個批次
				duration: `${(endTime - startTime) / 1000} 秒`,
				logs: importLog,
			});
		} catch (e: any) {
			console.error('批次匯入失敗:', e);
			return Response.json({ error: '批次匯入失敗', message: e.message, stack: e.stack }, { status: 500 });
		}
	},
};

/**
 * AI 提示模板
 * 根據商品資料產生「主要客群」
 */
function getAudiencePrompt(product: any): string {
	const description = (product['商品介紹'] || '').substring(0, 300);
	return `
		你是一個資料庫ETL專家。
		請根據以下商品資料，判斷其主要適用物種。
		
		產品名稱: ${product['產品名稱']}
		類別: ${product['類別']}
		商品介紹: ${description}

		你的回答必須是一個 JSON 陣列，只能包含 "狗", "貓", "人", "其他" 這幾個值。
		範例:
		- 如果是狗用品: ["狗"]
		- 如果是貓用品: ["貓"]
		- 如果是貓狗通用: ["狗", "貓"]
	`;
}

/**
 * 【新】輔助函式：解析 '商品圖檔' 欄位中的多個 URL
 */
function parseImageUrls(cellContent: string): string[] {
	if (!cellContent) return [];
	// Regex: 尋找所有被括號包住的 https 網址
	const urlRegex = /\((https:\/\/[^)]+)\)/g;
	const matches = cellContent.matchAll(urlRegex);
	// matches 是一個 iterator, [1] 是第一個捕獲組 (網址本身)
	return Array.from(matches, (match) => match[1]);
}

/**
 * 【新】輔D助函式：從 URL 下載圖片並上傳到 R2
 * 這是一個非同步函式，會在背景執行
 */
async function fetchAndUploadImage(url: string, r2Key: string, bucket: R2Bucket) {
	try {
		// 1. 下載圖片
		const response = await fetch(url);
		if (!response.ok) {
			throw new Error(`下載失敗: ${response.status} ${response.statusText}`);
		}
		const imageBuffer = await response.arrayBuffer();
		const contentType = response.headers.get('Content-Type') || 'image/jpeg';

		// 2. 上傳到 R2
		await bucket.put(r2Key, imageBuffer, {
			httpMetadata: { contentType },
		});
	} catch (error: any) {
		console.error(`圖片處理失敗 (URL: ${url}, R2Key: ${r2Key}): ${error.message}`);
		// 即使失敗也不拋出錯誤，以免中斷主流程
	}
}

/**
 * 輔助函式：準備 D1 商品資料 (不含圖片)
 * 修正 v3：傳入 DB 實例
 */
function getProductSqlStatements(
	row: any,
	sku: string,
	supplierId: string,
	audienceTags: string[],
	db: D1Database, // 修正：接收 D1 實例
): D1PreparedStatement[] {
	const statements: D1PreparedStatement[] = [];

	// 1. 寫入 'Products' 主檔
	statements.push(
		db
			.prepare(
				`INSERT OR IGNORE INTO Products (sku, supplier_id, name, barcode, brand_name, description, ingredients, size_dimensions, weight_g, origin, msrp, case_pack, is_public) 
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)`,
			)
			.bind(
				sku,
				supplierId,
				row['產品名稱'] || '',
				row['國際條碼'] || null, // 允許條碼為空
				row['品牌名稱'] || '',
				row['商品介紹'] || '',
				row['成份/材質'] || '',
				row['商品尺寸'] || '',
				parseFloat(row['重量g']) || 0,
				row['產地'] || '',
				parseInt(String(row['建議售價']).replace('$', '')) || 0, // 處理 '$' 符號
				row['箱入數'] || '',
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
		if (tag) { // 確保標籤不是 null 或空字串
			statements.push(db.prepare(`INSERT OR IGNORE INTO ProductAudience (sku, audience_tag) VALUES (?, ?)`).bind(sku, tag));
		}
	}

	return statements;
}
