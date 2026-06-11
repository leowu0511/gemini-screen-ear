# -*- coding: utf-8 -*-
"""
main_full.py - 完整版：音訊 + 截圖 + UI
使用 threading.Thread + queue + QTimer 輪詢，避免 QThread + asyncio 衝突
"""
import sys
import os
import logging
import queue
import threading
import io
import faulthandler
import traceback

if sys.platform == "win32":
    os.system("")
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("gemini_full.log", encoding="utf-8", mode="w"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# 黑盒子：原生層崩潰（segfault/abort）時，把所有執行緒的 C/Python traceback 寫進
# crash.log（append、行緩衝），用來抓那種沒有 Python traceback、沒有 WER 紀錄的
# 間歇性原生崩潰（PyAudio/mss 等）。檔案 handle 需在程序生命週期內保持開啟。
_CRASH_LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "crash.log")
_crash_log_fp = open(_CRASH_LOG_PATH, "a", buffering=1, encoding="utf-8")
_crash_log_fp.write(f"\n==== process start {__import__('datetime').datetime.now()} pid={os.getpid()} ====\n")
faulthandler.enable(file=_crash_log_fp, all_threads=True)


def _thread_excepthook(args):
    """記錄背景執行緒中未被攔截的例外（threading.Thread 預設只印到 stderr）。"""
    logger.error(
        "Uncaught exception in thread %s:\n%s",
        getattr(args.thread, "name", "?"),
        "".join(traceback.format_exception(args.exc_type, args.exc_value, args.exc_traceback)),
    )


threading.excepthook = _thread_excepthook

import asyncio
import base64
import json
import struct
import time
import tempfile
import concurrent.futures

import pyaudiowpatch as pyaudio
import websockets
import mss
from PIL import Image

from PyQt6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QTextEdit, QLabel
)
from PyQt6.QtCore import (
    Qt, QTimer, QPoint
)
from PyQt6.QtGui import (
    QPainter, QColor, QFont, QCursor, QMouseEvent, QPainterPath
)

from config import GEMINI_API_KEY, GEMINI_MODEL

# 端點對齊官方 Live API 文件：使用 v1beta（contextWindowCompression 等欄位以 v1beta 為準）
WS_URL = (
    f"wss://generativelanguage.googleapis.com/ws/"
    f"google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent"
    f"?key={GEMINI_API_KEY}"
)

# 跨程序持久化 resumption handle：寫在系統暫存目錄（避免 OneDrive 同步頻繁觸發）。
# 看門狗在 c.py 崩潰後重啟，新程序讀回 handle 即可從上次 session 續接、保留脈絡。
SESSION_HANDLE_FILE = os.path.join(tempfile.gettempdir(), "gemini_live_session_handle.txt")
SESSION_HANDLE_MAX_AGE = 300.0  # 秒；超過此年紀的 handle 視為過期，改開全新 session

TARGET_SAMPLE_RATE = 48000
SAMPLE_WIDTH = 2
AUDIO_CHUNK_MS = 100

# Screenshot settings
MAX_WIDTH = 1024
JPEG_QUALITY = 85
SCREENSHOT_INTERVAL = 1.0


def capture_screen():
    """使用 mss 擷取螢幕，Pillow 壓縮為 JPEG"""
    with mss.MSS() as sct:
        monitor = sct.monitors[1]
        screenshot = sct.grab(monitor)
        img = Image.frombytes("RGB", screenshot.size, screenshot.bgra, "raw", "BGRX")
        w, h = img.size
        if w > MAX_WIDTH:
            new_w = MAX_WIDTH
            new_h = int(h * MAX_WIDTH / w)
            img = img.resize((new_w, new_h), Image.LANCZOS)
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=JPEG_QUALITY)
        return buf.getvalue()


class GeminiLiveWorker(threading.Thread):
    """背景工作執行緒：使用 threading.Thread 而非 QThread"""
    def __init__(self, text_queue, status_queue):
        super().__init__(daemon=True)
        self.text_queue = text_queue
        self.status_queue = status_queue
        self._running = False
        self._stop_event = threading.Event()
        # 啟動時先嘗試讀回上次（可能崩潰的）程序留下的 handle，達成跨程序續接
        self._resumption_handle = self._load_persisted_handle()
        self._handle_last_persist_t = 0.0  # 節流持久化寫入用
        self._session_established = False  # 本次連線是否已完成 setupComplete
        self._audio_executor = None        # 專用單執行緒 executor 跑 stream.read
        self._session_uptime = 0.0

    # ---- session resumption handle 跨程序持久化 ----
    def _load_persisted_handle(self):
        try:
            if os.path.exists(SESSION_HANDLE_FILE):
                age = time.time() - os.path.getmtime(SESSION_HANDLE_FILE)
                if age <= SESSION_HANDLE_MAX_AGE:
                    with open(SESSION_HANDLE_FILE, "r", encoding="utf-8") as f:
                        h = f.read().strip()
                    if h:
                        logger.info(f"讀回持久化 handle（{age:.0f}s 前），將續接上次 session")
                        return h
                else:
                    logger.info(f"持久化 handle 已過期（{age:.0f}s），改開全新 session")
        except Exception as e:
            logger.warning(f"讀取持久化 handle 失敗: {e}")
        return None

    def _persist_handle(self, handle):
        try:
            tmp = SESSION_HANDLE_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(handle)
            os.replace(tmp, SESSION_HANDLE_FILE)  # 原子置換
        except Exception as e:
            logger.debug(f"持久化 handle 寫入失敗: {e}")

    def _clear_persisted_handle(self):
        try:
            if os.path.exists(SESSION_HANDLE_FILE):
                os.remove(SESSION_HANDLE_FILE)
        except Exception:
            pass

    def run(self):
        self._running = True
        self._pa = None
        self._stream = None
        # 專用單執行緒 executor 跑 stream.read：保證任何時刻只有「一條」執行緒在讀串流。
        # 根除「兩條 executor 執行緒同時 stream.read 同一串流 → access violation」原生崩潰
        # （續接換 session 時，舊 read 還沒返回、新 _audio_loop 又發起 read 所致）。
        self._audio_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="audio-read"
        )
        try:
            asyncio.run(self._async_main())
        except Exception as e:
            logger.exception("Fatal error in worker")
            self._put_text(f"\n[Fatal Error] {type(e).__name__}: {e}\n")
        finally:
            # 先關音訊 executor 並等最後一次 read 結束（cancel 掉還在排隊的），
            # 確保沒有 read 還在跑，再 stop/close 串流，避免「邊讀邊關」崩潰。
            try:
                self._audio_executor.shutdown(wait=True, cancel_futures=True)
            except Exception:
                pass
            try:
                if self._stream is not None:
                    self._stream.stop_stream()
                    self._stream.close()
            except Exception:
                pass
            try:
                if self._pa is not None:
                    self._pa.terminate()
            except Exception:
                pass
            logger.info("Audio cleanup done.")

    def stop(self):
        self._running = False
        self._stop_event.set()

    def _get_loopback_device(self, pa):
        try:
            return pa.get_default_wasapi_loopback()
        except Exception:
            for i in range(pa.get_device_count()):
                dev = pa.get_device_info_by_index(i)
                host_api = pa.get_host_api_info_by_index(dev["hostApi"])
                if "WASAPI" in host_api["name"] and "[Loopback]" in dev["name"]:
                    return dev
            return None

    def _resample_to_16k_mono(self, audio_data, orig_rate, orig_channels):
        total_samples = len(audio_data) // (SAMPLE_WIDTH * orig_channels)
        if total_samples == 0:
            return b""
        fmt = f"<{total_samples * orig_channels}h"
        try:
            values = struct.unpack(fmt, audio_data)
        except struct.error:
            return b""
        if orig_channels > 1:
            mono = []
            for i in range(total_samples):
                frame_start = i * orig_channels
                frame_end = frame_start + orig_channels
                avg = sum(values[frame_start:frame_end]) // orig_channels
                mono.append(avg)
            values = tuple(mono)
        if orig_rate != TARGET_SAMPLE_RATE:
            if orig_rate % TARGET_SAMPLE_RATE == 0:
                step = orig_rate // TARGET_SAMPLE_RATE
                values = values[::step]
            else:
                ratio = orig_rate / TARGET_SAMPLE_RATE
                new_len = int(len(values) / ratio)
                resampled = []
                for i in range(new_len):
                    resampled.append(values[int(i * ratio)])
                values = tuple(resampled)
        clamped = tuple(max(-32768, min(32767, int(v))) for v in values)
        return struct.pack(f"<{len(clamped)}h", *clamped)

    def _put_status(self, status):
        try:
            self.status_queue.put_nowait(status)
        except queue.Full:
            pass

    def _put_text(self, text):
        try:
            self.text_queue.put_nowait(text)
        except queue.Full:
            pass

    async def _async_main(self):
        logger.info("Initializing audio...")
        self._put_status("正在初始化音訊...")
        pa = pyaudio.PyAudio()
        self._pa = pa  # run() 的 finally 會負責 terminate
        loopback_dev = self._get_loopback_device(pa)
        if loopback_dev is None:
            self._put_status("找不到 WASAPI Loopback 裝置！")
            self._put_text("\n[錯誤] 找不到 WASAPI Loopback 裝置！\n")
            return

        native_rate = int(loopback_dev["defaultSampleRate"])
        native_channels = loopback_dev["maxInputChannels"]
        device_index = loopback_dev["index"]
        chunk_frames = int(native_rate * AUDIO_CHUNK_MS / 1000)

        logger.info(f"Audio: {native_rate}Hz, {native_channels}ch, device={device_index}")
        self._put_status(f"音訊: {native_rate}Hz, {native_channels}ch")

        try:
            stream = pa.open(
                format=pyaudio.paInt16,
                channels=native_channels,
                rate=native_rate,
                input=True,
                input_device_index=device_index,
                frames_per_buffer=chunk_frames,
            )
            self._stream = stream  # run() 的 finally 會負責 stop/close
        except Exception as e:
            logger.error(f"Failed to open audio stream: {e}")
            self._put_status(f"無法開啟音訊串流: {e}")
            self._put_text(f"\n[錯誤] 無法開啟音訊串流: {e}\n")
            return

        # 重連迴圈：Gemini Live 單一 session 約 10 分鐘會被伺服器切斷，透過
        # sessionResumption handle 在斷線後無縫續接、保留對話脈絡；contextWindowCompression
        # 則避免脈絡累積超窗。若帶 handle 卻一連上就被踢（例如脈絡已壞的中毒 handle），
        # 則丟掉 handle 改開全新 session，避免死循環。
        MIN_HEALTHY_UPTIME = 5.0  # 秒；session 維持不到這個時間視為不健康
        reconnect_count = 0
        while self._running:
            self._session_established = False
            self._session_uptime = 0.0
            try:
                await self._run_session(stream, native_rate, native_channels, chunk_frames)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Session ended: {type(e).__name__}: {e}")

            if not self._running:
                break

            # 健康的 session：有完成 setup，且維持超過門檻時間。
            healthy = self._session_established and self._session_uptime >= MIN_HEALTHY_UPTIME

            # 不健康又帶著 handle → handle（或其對應脈絡）已壞，丟掉改開全新 session，
            # 避免拿同一個壞 handle 無限重連被 1007 連環踢。
            if not healthy and self._resumption_handle:
                logger.warning(
                    f"Unhealthy session (established={self._session_established}, "
                    f"uptime={self._session_uptime:.1f}s); dropping resumption handle, fresh session next"
                )
                self._resumption_handle = None
                self._clear_persisted_handle()  # 連持久化檔一起清掉，避免崩潰重啟又讀回壞 handle

            if healthy:
                reconnect_count = 0
                delay = 1.0
            else:
                reconnect_count += 1
                delay = min(2 ** reconnect_count, 30)

            resume_note = "續接" if self._resumption_handle else "重新連線"
            logger.info(f"{resume_note} in {delay:.0f}s")
            self._put_status(f"連線中斷，{delay:.0f}s 後{resume_note}...")

            slept = 0.0
            while self._running and slept < delay:
                await asyncio.sleep(0.2)
                slept += 0.2

        logger.info("Session loop ended.")

    async def _run_session(self, stream, native_rate, native_channels, chunk_frames):
        """單一 Gemini Live 連線的生命週期；斷線時 return，由外層 _async_main 重連。"""
        if self._resumption_handle:
            logger.info("Connecting to Gemini (resuming session)...")
            self._put_status("正在續接 session...")
        else:
            logger.info("Connecting to Gemini...")
            self._put_status("正在連線到 Gemini...")

        async with websockets.connect(WS_URL, max_size=None) as ws:
            logger.info("Connected!")

            session_resumption = {}
            if self._resumption_handle:
                session_resumption = {"handle": self._resumption_handle}

            setup_msg = {
                "setup": {
                    "model": GEMINI_MODEL,
                    "generationConfig": {
                        "responseModalities": ["AUDIO"],
                    },
                    "inputAudioTranscription": {},
                    "outputAudioTranscription": {},
                    # 空 dict = 啟用 session resumption（伺服器會回傳 handle）；
                    # 帶 handle = 續接先前 session。
                    "sessionResumption": session_resumption,
                    # 脈絡窗口壓縮：用 slidingWindow 自動裁掉最舊的脈絡，避免長時間
                    # 累積（音訊＋每秒截圖）超出模型脈絡窗口而在續接時被 1007
                    # "invalid argument" 踢掉。這是讓 session 可無限期續接的關鍵。
                    # 預設 triggerTokens = 模型脈絡窗口的 80%，targetTokens = 其一半。
                    "contextWindowCompression": {"slidingWindow": {}},
                    "systemInstruction": {
                        "parts": [{"text": "你是一個即時助理，正在監聽使用者電腦的音訊和螢幕畫面。當你聽到對話或內容時，請主動用繁體中文提供評論、摘要或見解。不需要等待明確的問題，直接回應你觀察到的內容。"}]
                    }
                }
            }
            await ws.send(json.dumps(setup_msg))
            logger.info("Setup sent, waiting for setupComplete...")

            while True:
                raw = await asyncio.wait_for(ws.recv(), timeout=10)
                if isinstance(raw, bytes):
                    data = json.loads(raw.decode("utf-8"))
                else:
                    data = json.loads(raw)
                logger.debug(f"Setup recv: {data.keys()}")
                if "setupComplete" in data:
                    logger.info("Setup complete!")
                    break

            self._session_established = True
            session_t0 = time.monotonic()
            if self._resumption_handle:
                self._put_status("已續接 session - 聆聽中")
            else:
                self._put_status("聆聽中 - 播放語音即可觸發回應")

            audio_task = asyncio.create_task(
                self._audio_loop(ws, stream, native_rate, native_channels, chunk_frames)
            )
            screenshot_task = asyncio.create_task(
                self._screenshot_loop(ws)
            )
            receiver_task = asyncio.create_task(
                self._receiver_loop(ws)
            )
            tasks = [audio_task, screenshot_task, receiver_task]

            # 跑到使用者要求停止，或連線中斷（任一 loop 因 ConnectionClosed 而結束）
            while self._running and not any(t.done() for t in tasks):
                await asyncio.sleep(0.3)

            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

            # 記錄本次 session 維持了多久（從 setupComplete 算起），供外層判斷是否為
            # 「一連上就被踢」的中毒 handle。
            self._session_uptime = time.monotonic() - session_t0

    async def _audio_loop(self, ws, stream, native_rate, native_channels, chunk_frames):
        loop = asyncio.get_event_loop()
        chunk_count = 0
        try:
            while self._running:
                # 用專用單執行緒 executor，序列化所有 stream.read，杜絕並行讀取崩潰
                audio_data = await loop.run_in_executor(
                    self._audio_executor, lambda: stream.read(chunk_frames, False)
                )
                pcm = self._resample_to_16k_mono(audio_data, native_rate, native_channels)
                if not pcm:
                    continue

                b64 = base64.b64encode(pcm).decode("ascii")
                msg = {
                    "realtimeInput": {
                        "audio": {
                            "data": b64,
                            "mimeType": f"audio/pcm;rate={TARGET_SAMPLE_RATE}"
                        }
                    }
                }
                await ws.send(json.dumps(msg))
                chunk_count += 1
                if chunk_count % 500 == 0:
                    logger.info(f"Sent {chunk_count} audio chunks")
                    self._put_status(f"已發送 {chunk_count} 個音訊區塊")

                await asyncio.sleep(AUDIO_CHUNK_MS / 1000.0 * 0.8)

        except asyncio.CancelledError:
            pass
        except websockets.exceptions.ConnectionClosedError:
            logger.warning("Audio loop: connection closed")
        except Exception as e:
            logger.error(f"Audio loop error: {e}")
            self._put_text(f"\n[音訊錯誤] {e}\n")

    async def _screenshot_loop(self, ws):
        """每秒截圖並發送"""
        loop = asyncio.get_event_loop()
        count = 0
        await asyncio.sleep(2.0)
        try:
            while self._running:
                jpeg_data = await loop.run_in_executor(None, capture_screen)
                b64 = base64.b64encode(jpeg_data).decode("ascii")
                msg = {
                    "realtimeInput": {
                        "video": {
                            "data": b64,
                            "mimeType": "image/jpeg"
                        }
                    }
                }
                await ws.send(json.dumps(msg))
                count += 1
                if count % 10 == 0:
                    logger.info(f"Sent {count} screenshots")
                    self._put_status(f"已發送 {count} 張截圖")
                await asyncio.sleep(SCREENSHOT_INTERVAL)
        except asyncio.CancelledError:
            pass
        except websockets.exceptions.ConnectionClosedError:
            logger.warning("Screenshot loop: connection closed")
        except Exception as e:
            logger.error(f"Screenshot loop error: {e}")

    async def _receiver_loop(self, ws):
        full_text = ""
        try:
            while self._running:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
                except asyncio.TimeoutError:
                    continue

                if isinstance(raw, bytes):
                    try:
                        data = json.loads(raw.decode("utf-8"))
                    except (UnicodeDecodeError, json.JSONDecodeError):
                        continue
                else:
                    try:
                        data = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                # session resumption：保存最新 handle，供斷線後續接
                sru = data.get("sessionResumptionUpdate")
                if sru:
                    if sru.get("resumable") and sru.get("newHandle"):
                        self._resumption_handle = sru["newHandle"]
                        logger.info("Resumption handle updated")
                        # 持久化到暫存檔（節流：最多每 ~8s 寫一次），供崩潰重啟後續接
                        now = time.monotonic()
                        if now - self._handle_last_persist_t >= 8.0:
                            self._persist_handle(self._resumption_handle)
                            self._handle_last_persist_t = now

                # goAway：伺服器預告即將切斷此連線（通常因 session 時長上限）
                ga = data.get("goAway")
                if ga:
                    logger.info(f"goAway received, timeLeft={ga.get('timeLeft')}")
                    self._put_status("伺服器即將輪替 session，準備續接...")

                server_content = data.get("serverContent")
                if server_content is None:
                    continue

                logger.debug(f"serverContent keys: {server_content.keys()}")

                it = server_content.get("inputTranscription")
                if it:
                    text = it.get("text", "")
                    if text:
                        logger.info(f"inputTranscription: {repr(text)}")
                        self._put_text(f"[你] {text}\n")

                ot = server_content.get("outputTranscription")
                if ot:
                    text = ot.get("text", "")
                    if text:
                        logger.info(f"outputTranscription: {repr(text)}")
                        self._put_text(text)
                        full_text += text

                mt = server_content.get("modelTurn")
                if mt:
                    for part in mt.get("parts", []):
                        t = part.get("text")
                        if t:
                            logger.info(f"modelTurn text: {repr(t)}")
                            self._put_text(t)
                            full_text += t

                if server_content.get("turnComplete", False):
                    logger.info("Turn complete")
                    self._put_text("\n")

        except asyncio.CancelledError:
            pass
        except websockets.exceptions.ConnectionClosedError:
            logger.warning("Receiver loop: connection closed")


class GlassWindow(QWidget):
    def __init__(self):
        super().__init__()
        self._drag_start = QPoint()
        self._is_dragging = False
        self._start_monotonic = time.monotonic()  # 運作時間起算點
        self._start_wall = time.strftime("%H:%M:%S")  # 啟動的牆鐘時間（顯示用）
        self._last_uptime_sec = -1
        self._init_ui()

    def _init_ui(self):
        self.setWindowFlags(
            Qt.WindowType.FramelessWindowHint |
            Qt.WindowType.WindowStaysOnTopHint |
            Qt.WindowType.Tool
        )
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        self.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents, False)
        self.setFixedSize(520, 400)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(15, 15, 15, 15)
        layout.setSpacing(8)

        title_bar = QLabel("🎙️ Gemini Live Assistant")
        title_bar.setFont(QFont("Microsoft JhengHei UI", 11, QFont.Weight.Bold))
        title_bar.setStyleSheet("color: #FFFFFF; padding: 4px;")
        title_bar.setCursor(QCursor(Qt.CursorShape.OpenHandCursor))
        title_bar.mousePressEvent = self._title_mouse_press
        title_bar.mouseMoveEvent = self._title_mouse_move
        title_bar.mouseReleaseEvent = self._title_mouse_release
        layout.addWidget(title_bar)

        self.status_label = QLabel("狀態: 初始化中...")
        self.status_label.setFont(QFont("Microsoft JhengHei UI", 9))
        self.status_label.setStyleSheet("color: #AAAAAA; padding: 2px;")
        layout.addWidget(self.status_label)

        # 運作時間顯示（自本程序啟動起算；看門狗重啟後會歸零，正好可看出重啟過）
        self.uptime_label = QLabel(f"運作時間: 00:00:00　(啟動 {self._start_wall})")
        self.uptime_label.setFont(QFont("Microsoft JhengHei UI", 9))
        self.uptime_label.setStyleSheet("color: #88C0FF; padding: 2px;")
        layout.addWidget(self.uptime_label)

        self.text_display = QTextEdit()
        self.text_display.setReadOnly(True)
        self.text_display.setFont(QFont("Microsoft JhengHei UI", 11))
        # 修復: 不使用 rgba()，避免 Windows DWM 崩潰
        self.text_display.setStyleSheet("""
            QTextEdit {
                background-color: rgb(30, 30, 50);
                color: #E0E0E0;
                border: 1px solid rgb(100, 100, 150);
                border-radius: 8px;
                padding: 10px;
                selection-background-color: rgb(100, 150, 255);
            }
            QScrollBar:vertical {
                background: transparent;
                width: 6px;
            }
            QScrollBar::handle:vertical {
                background: rgb(150, 150, 200);
                border-radius: 3px;
                min-height: 20px;
            }
            QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
                height: 0px;
            }
        """)
        layout.addWidget(self.text_display)

        close_btn = QLabel("✕")
        close_btn.setFont(QFont("Arial", 12))
        close_btn.setStyleSheet("color: #888888; padding: 4px 8px;")
        close_btn.setCursor(QCursor(Qt.CursorShape.PointingHandCursor))
        close_btn.mousePressEvent = lambda e: self._close_app()
        close_btn.setAlignment(Qt.AlignmentFlag.AlignRight)
        layout.addWidget(close_btn)

        self.setStyleSheet("""
            QWidget {
                background-color: rgb(20, 20, 40);
                border-radius: 12px;
            }
        """)

        self.text_queue = queue.Queue(maxsize=1000)
        self.status_queue = queue.Queue(maxsize=100)
        self.timer = QTimer(self)
        self.timer.timeout.connect(self._poll_queues)
        self.timer.start(100)

    def _poll_queues(self):
        while True:
            try:
                status = self.status_queue.get_nowait()
                self.status_label.setText(f"狀態: {status}")
            except queue.Empty:
                break
        while True:
            try:
                text = self.text_queue.get_nowait()
                self.append_text(text)
            except queue.Empty:
                break
        self._update_uptime()

    def _update_uptime(self):
        """每秒更新一次運作時間（HH:MM:SS）。由 100ms 的 _poll_queues 驅動，
        只在整數秒變動時才改 label，避免不必要的重繪。"""
        sec = int(time.monotonic() - self._start_monotonic)
        if sec == self._last_uptime_sec:
            return
        self._last_uptime_sec = sec
        h, rem = divmod(sec, 3600)
        m, s = divmod(rem, 60)
        self.uptime_label.setText(
            f"運作時間: {h:02d}:{m:02d}:{s:02d}　(啟動 {self._start_wall})"
        )
        # 每分鐘在 log 留一個運作時間心跳，這樣即使視窗突然閃退、事後也能從 log
        # 一眼看出「跑了多久才掛」。
        if sec > 0 and sec % 60 == 0:
            logger.info(f"[uptime] 已運作 {h:02d}:{m:02d}:{s:02d}")

    def _title_mouse_press(self, event: QMouseEvent):
        if event.button() == Qt.MouseButton.LeftButton:
            self._drag_start = event.globalPosition().toPoint() - self.frameGeometry().topLeft()
            self._is_dragging = True

    def _title_mouse_move(self, event: QMouseEvent):
        if self._is_dragging:
            self.move(event.globalPosition().toPoint() - self._drag_start)

    def _title_mouse_release(self, event: QMouseEvent):
        self._is_dragging = False

    def _close_app(self):
        QApplication.quit()

    def append_text(self, text: str):
        cursor = self.text_display.textCursor()
        cursor.movePosition(cursor.MoveOperation.End)
        self.text_display.setTextCursor(cursor)
        self.text_display.insertPlainText(text)
        self.text_display.ensureCursorVisible()

    def set_status(self, status: str):
        self.status_label.setText(f"狀態: {status}")

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        painter.setBrush(QColor(20, 20, 40, 255))
        painter.setPen(Qt.PenStyle.NoPen)
        rect = self.rect()
        path = QPainterPath()
        path.addRoundedRect(rect.x(), rect.y(), rect.width(), rect.height(), 12, 12)
        painter.drawPath(path)
        painter.end()


def main():
    app = QApplication(sys.argv)
    app.setStyle("Fusion")

    window = GlassWindow()
    window.show()

    screen = app.primaryScreen().geometry()
    window.move(
        screen.width() - window.width() - 20,
        screen.height() - window.height() - 60
    )

    worker = GeminiLiveWorker(window.text_queue, window.status_queue)

    def on_quit():
        worker.stop()
        # 給 worker 短暫時間自行收尾（關 websocket、PortAudio）。
        worker.join(timeout=3)
        # 不論 worker 是否還卡在阻塞的網路 I/O（例如 websockets.connect 握手逾時長達 10s），
        # 一律以 os._exit 立即結束程序：跳過 Python 解譯器的 finalization，杜絕 daemon 執行緒
        # 的 C 擴充（PortAudio）與解譯器關閉競爭而導致的 segfault（exit 139）。OS 會回收資源。
        os._exit(0)

    app.aboutToQuit.connect(on_quit)

    worker.start()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
