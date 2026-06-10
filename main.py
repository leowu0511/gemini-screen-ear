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

import asyncio
import base64
import json
import struct

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

WS_URL = (
    f"wss://generativelanguage.googleapis.com/ws/"
    f"google.ai.generativelanguage.v1alpha.GenerativeService.BidiGenerateContent"
    f"?key={GEMINI_API_KEY}"
)

TARGET_SAMPLE_RATE = 16000
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

    def run(self):
        self._running = True
        try:
            asyncio.run(self._async_main())
        except Exception as e:
            logger.exception("Fatal error in worker")
            self._put_text(f"\n[Fatal Error] {type(e).__name__}: {e}\n")

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
        loopback_dev = self._get_loopback_device(pa)
        if loopback_dev is None:
            self._put_status("找不到 WASAPI Loopback 裝置！")
            self._put_text("\n[錯誤] 找不到 WASAPI Loopback 裝置！\n")
            pa.terminate()
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
        except Exception as e:
            logger.error(f"Failed to open audio stream: {e}")
            self._put_status(f"無法開啟音訊串流: {e}")
            self._put_text(f"\n[錯誤] 無法開啟音訊串流: {e}\n")
            pa.terminate()
            return

        logger.info("Connecting to Gemini...")
        self._put_status("正在連線到 Gemini...")

        try:
            async with websockets.connect(WS_URL) as ws:
                logger.info("Connected!")
                self._put_status("已連線！")

                setup_msg = {
                    "setup": {
                        "model": GEMINI_MODEL,
                        "generationConfig": {
                            "responseModalities": ["AUDIO"],
                        },
                        "inputAudioTranscription": {},
                        "outputAudioTranscription": {},
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

                while self._running:
                    await asyncio.sleep(0.5)

                audio_task.cancel()
                screenshot_task.cancel()
                receiver_task.cancel()

                try:
                    await asyncio.wait_for(audio_task, timeout=3)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
                try:
                    await asyncio.wait_for(screenshot_task, timeout=3)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
                try:
                    await asyncio.wait_for(receiver_task, timeout=3)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass

        except websockets.exceptions.ConnectionClosedError as e:
            logger.error(f"Connection closed: code={e.code} reason={e.reason}")
            self._put_status(f"連線關閉: code={e.code}")
            self._put_text(f"\n[錯誤] 連線關閉: code={e.code}\n")
        except Exception as e:
            logger.exception("Error in async_main")
            self._put_status(f"錯誤: {type(e).__name__}: {e}")
            self._put_text(f"\n[錯誤] {type(e).__name__}: {e}\n")
        finally:
            stream.stop_stream()
            stream.close()
            pa.terminate()
            logger.info("Cleanup done.")

    async def _audio_loop(self, ws, stream, native_rate, native_channels, chunk_frames):
        loop = asyncio.get_event_loop()
        chunk_count = 0
        try:
            while self._running:
                audio_data = await loop.run_in_executor(
                    None, lambda: stream.read(chunk_frames, False)
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
        # daemon=True，主程序結束 worker 自動結束，不 join 避免 block UI thread

    app.aboutToQuit.connect(on_quit)

    worker.start()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
