# Gemini Live Assistant

透過 Google Gemini 即時 API (WebSocket) 打造的一款桌面即時助理，具備系統音訊監聽、螢幕截圖、以及半透明浮動視窗介面。

## 功能特色

- **系統音訊監聽**：使用 `pyaudiowpatch` (WASAPI Loopback) 擷取電腦輸出的所有音訊，即時傳送至 Gemini。
- **螢幕截圖**：每秒擷取一次螢幕畫面，以 JPEG 壓縮後傳送，讓 Gemini 能「看見」使用者當前的畫面內容。
- **即時轉錄**：Gemini 會將語音轉錄為文字，並在浮動視窗中顯示。
- **浮動玻璃視窗**：半透明、無邊框、置頂顯示的 UI 視窗，可拖曳移動。
- **繁體中文回應**：System instruction 設定為以繁體中文回應使用者。

## 系統需求

- **作業系統**：Windows (需支援 WASAPI Loopback)
- **Python**：3.10 或更新版本
- **Gemini API Key**：需具備 Gemini API 存取權限

## 安裝

### 1. 安裝相依套件

```bash
pip install -r requirements.txt
```

### 2. 設定 API Key

編輯 [`config.py`](config.py)，填入你的 Gemini API Key：

```python
GEMINI_API_KEY = "你的_API_KEY"
GEMINI_MODEL = "models/gemini-3.1-flash-live-preview"
```

> 注意：目前預設模型為 `gemini-3.1-flash-live-preview`，請依實際可用模型調整。

## 使用方式

執行主程式：

```bash
python main.py
```

啟動後會出現一個半透明的浮動視窗，放置於螢幕右下角。程式會自動：

1. 初始化音訊裝置（系統音訊 Loopback）
2. 建立 WebSocket 連線至 Gemini API
3. 開始監聽音訊並定時擷取螢幕畫面
4. 當 Gemini 偵測到語音內容時，會自動回應並顯示在視窗中

### 操作方式

- **拖曳標題列**：移動浮動視窗位置
- **點擊右下角 `✕`**：關閉程式

## 專案結構

```
meeting test/
├── main.py           # 主程式：音訊擷取、WebSocket 通訊、UI 視窗
├── config.py         # 設定檔：API Key 與模型名稱
├── requirements.txt  # Python 相依套件清單
└── README.md         # 專案說明文件
```

## 架構說明

### 執行緒模型

採用 [`threading.Thread`](main.py:82) + [`asyncio`](main.py:28) 的混合模型：

- **背景工作執行緒** ([`GeminiLiveWorker`](main.py:82))：負責所有非同步作業，包括音訊擷取、螢幕截圖、WebSocket 通訊。
- **主 UI 執行緒** ([`GlassWindow`](main.py:400))：PyQt6 事件迴圈，負責顯示轉錄文字與狀態資訊。
- 兩個執行緒之間透過 [`queue.Queue`](main.py:478) 傳遞文字與狀態訊息，透過 [`QTimer`](main.py:480) 輪詢更新 UI。

### 音訊流程

1. [`_get_loopback_device()`](main.py:103) 自動尋找 WASAPI Loopback 裝置
2. PyAudio 以原生取樣率讀取 PCM 音訊區塊（每 100ms）
3. [`_resample_to_16k_mono()`](main.py:114) 將音訊降採樣至 16kHz 單聲道
4. Base64 編碼後透過 WebSocket 傳送至 Gemini

### 螢幕截圖流程

1. [`capture_screen()`](main.py:66) 使用 `mss` 擷取完整螢幕
2. Pillow 縮圖至最大寬度 1024px，JPEG 品質 85
3. Base64 編碼後以 `realtimeInput.video` 形式發送

### 接收回應

- [`_receiver_loop()`](main.py:340) 持續讀取 WebSocket 訊息
- 支援 [`inputTranscription`](main.py:366)（使用者的語音轉錄）和 [`outputTranscription`](main.py:373)（Gemini 的回應轉錄）
- 也支援 [`modelTurn`](main.py:381) 的文字內容

### UI 視窗

[`GlassWindow`](main.py:400) 是一個自訂的 PyQt6 `QWidget`：

- **無邊框、置頂**：`FramelessWindowHint` + `WindowStaysOnTopHint`
- **半透明背景**：`WA_TranslucentBackground`，自訂 `paintEvent` 繪製圓角矩形
- **拖曳功能**：透過 `mousePressEvent`/`mouseMoveEvent`/`mouseReleaseEvent` 實現
- **安全注意**：避免使用 `rgba()` 樣式以防止 Windows DWM 崩潰

## 設定參數

| 參數 | 預設值 | 說明 |
|------|--------|------|
| [`TARGET_SAMPLE_RATE`](main.py:56) | 16000 | 輸出音訊取樣率 (Hz) |
| [`AUDIO_CHUNK_MS`](main.py:58) | 100 | 每個音訊區塊的時間長度 (ms) |
| [`MAX_WIDTH`](main.py:61) | 1024 | 螢幕截圖最大寬度 (px) |
| [`JPEG_QUALITY`](main.py:62) | 85 | 螢幕截圖 JPEG 壓縮品質 |
| [`SCREENSHOT_INTERVAL`](main.py:63) | 1.0 | 截圖間隔時間 (秒) |

## 注意事項

- 目前僅支援 **Windows WASAPI Loopback**，無法在 macOS 或 Linux 上執行。
- 若找不到 Loopback 裝置（例如未安裝聲音驅動或虛擬音效卡），程式會顯示錯誤訊息並結束。
- WebSocket 連線若中斷會自動顯示錯誤代碼。
- API Key 請妥善保管，勿上傳至公開版本控制系統。

## 疑難排解

**找不到 WASAPI Loopback 裝置**
- 確保系統正在播放音訊（Loopback 裝置需要至少一個音訊串流才能啟用）
- 檢查是否安裝了最新的聲音驅動程式

**連線失敗**
- 確認 `GEMINI_API_KEY` 是否正確
- 確認網路環境能否連線至 `generativelanguage.googleapis.com`

**視窗無法顯示**
- 確保螢幕解析度至少為 520x400
- 檢查是否有其他全螢幕應用程式阻擋
