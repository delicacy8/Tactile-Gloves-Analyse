
#!/usr/bin/env python3
"""Xense USB glove recorder. Python >=3.9; only xenseglovesdk is required.

Run: python xense_glove_recorder.py
Offline check: python xense_glove_recorder.py --demo --duration 3
"""
import argparse
import datetime as dt
import importlib
import json 
import platform
import queue
import statistics
import sys
import threading
import time
import uuid
import zipfile
import math
from pathlib import Path

SCRIPT_VERSION = "1.0.0"
DOCS = "https://docs.xenserobotics.com/docs/latest/sdk/xenseglovesdk/"


def utc_now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def write_json(path, data):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(path)


def field(obj, name, default=None):
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def device_info(info):
    return {key: field(info, key) for key in (
        "serial_number", "version", "rows", "cols", "hand", "frequency",
        "ble_frequency", "output_mode", "output_mode_name", "channel", "board_kind")}


def frame_key(frame):
    return (frame.get("timestamp_ms", frame.get("timestamp")), frame.get("sequence"))


class JsonlWriter:
    """Bounded queue: stop on overflow rather than silently discard samples."""
    def __init__(self, path):
        self.queue = queue.Queue(maxsize=2048)
        self.error = None
        self.count = 0
        self.file = path.open("x", encoding="utf-8", buffering=1024 * 1024)
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        try:
            last_flush = time.monotonic()
            while True:
                try:
                    item = self.queue.get(timeout=0.2)
                except queue.Empty:
                    self.file.flush()
                    continue
                if item is None:
                    break
                self.file.write(json.dumps(item, ensure_ascii=False, separators=(",", ":")) + "\n")
                self.count += 1
                if time.monotonic() - last_flush >= 1:
                    self.file.flush()
                    last_flush = time.monotonic()
        except Exception as exc:
            self.error = exc
        finally:
            try:
                self.file.close()
            except Exception as exc:
                self.error = self.error or exc

    def submit(self, frame):
        if self.error:
            raise RuntimeError("写入轨迹失败: " + str(self.error))
        try:
            self.queue.put_nowait(frame)
        except queue.Full:
            raise RuntimeError("磁盘写入跟不上采集，队列已满；停止以避免静默丢帧")

    def close(self):
        while self.thread.is_alive():
            try:
                self.queue.put(None, timeout=0.2)
                break
            except queue.Full:
                pass
        self.thread.join()
        if self.error:
            raise RuntimeError("轨迹写入不完整: " + str(self.error))


def read_jsonl(path):
    with path.open(encoding="utf-8") as source:
        for line in source:
            yield json.loads(line)


def summarize(path, requested_hz):
    """Audit what was actually written, streaming through the JSONL file."""
    count = 0
    intervals = []
    missing = resets = duplicate_saved = zero_frames = saturated = 0
    valid_min = valid_max = None
    previous = None
    first_host = last_host = None
    modes = set()
    for frame in read_jsonl(path):
        count += 1
        modes.add(str(frame.get("mode")))
        vals = [v for v in frame["values"] if v is not None]
        n = len(vals)
        valid_min = n if valid_min is None else min(n, valid_min)
        valid_max = n if valid_max is None else max(n, valid_max)
        zero_frames += bool(vals) and all(v == 0 for v in vals)
        saturated += any(v == 254 for v in vals)
        current_host = frame["_capture"]["elapsed_s"]
        if first_host is None:
            first_host = current_host
        last_host = current_host
        if previous is not None:
            duplicate_saved += frame_key(frame) == frame_key(previous)
            sd = (frame["sequence"] - previous["sequence"]) % 65536
            td = (frame["timestamp_ms"] - previous["timestamp_ms"]) % 4294967296
            if sd >= 32768 or td >= 2147483648 or (sd == 0 and td != 0):
                resets += 1
            else:
                if sd > 1:
                    missing += sd - 1
                if td > 0:
                    intervals.append(td)
        previous = frame
    ordered = sorted(intervals)
    interval_stats = None
    if ordered:
        interval_stats = {
            "min": ordered[0], "median": statistics.median(ordered),
            "p95": ordered[max(0, math.ceil(0.95 * len(ordered)) - 1)], "max": ordered[-1]}
    hz = len(intervals) * 1000 / sum(intervals) if intervals else None
    warnings = []
    if count == 0:
        warnings.append("没有保存任何帧。")
    if missing:
        warnings.append("保存序号有缺口；不能仅凭此区分设备、传输、SDK最新帧覆盖或应用漏采。")
    if resets:
        warnings.append("发现疑似设备重启或时间/序号不连续；应分段分析。")
    if hz is not None and hz < requested_hz * 0.9:
        warnings.append("实际保存频率低于请求频率的90%；这只是采集质控提示，不是硬件合格阈值。")
    if saturated:
        warnings.append("存在ADC达到254的帧，需要检查是否接近输出上限。")
    return {
        "saved_frames": count, "requested_hz": requested_hz,
        "saved_hz_from_device_time": hz,
        "saved_span_host_monotonic_s": None if count < 2 else last_host - first_host,
        "device_interval_ms": interval_stats,
        "missing_sequence_estimate": missing,
        "sequence_or_time_discontinuities": resets,
        "duplicate_saved_frames": duplicate_saved,
        "valid_points_min": valid_min, "valid_points_max": valid_max,
        "all_zero_frames": zero_frames, "frames_with_adc_254": saturated,
        "modes": sorted(modes), "warnings": warnings,
        "notes": ["ADC不是牛顿；null不是零接触。",
                  "保存帧率不是传感器带宽或机械响应时间。",
                  "仅保存压力帧附带的gyro，不保证独立IMU流完整。",
                  "序号和设备时间正常回绕已处理；极长中断或重启的缺帧数无法精确恢复。"]}


def command_reader(commands):
    while True:
        try:
            text = input()
        except EOFError:
            commands.put(("stop", "stdin_closed", time.monotonic(), time.time()))
            return
        now, wall = time.monotonic(), time.time()
        if text.strip().lower() in ("", "q", "quit", "stop"):
            commands.put(("stop", "keyboard", now, wall))
            return
        commands.put(("marker", text.strip(), now, wall))


def capture(glove, args, folder):
    commands = queue.Queue()
    start = time.monotonic()
    last_new = start
    # Exclude the SDK's cached pre-start frame from the recorded motion.
    last_key = None
    if not args.demo:
        try:
            last_key = frame_key(glove.read_frame(timeout=0).to_dict())
        except TimeoutError:
            pass
    writer = JsonlWriter(folder / "frames.jsonl")
    if not args.demo:
        threading.Thread(target=command_reader, args=(commands,), daemon=True).start()
    duplicates = timeouts = accepted = 0
    reason, error = "unknown", None
    events = (folder / "events.jsonl").open("x", encoding="utf-8", buffering=1)

    def event(kind, label, mono=None, wall=None):
        events.write(json.dumps({"event": kind, "label": label,
            "elapsed_s": (time.monotonic() if mono is None else mono) - start,
            "host_unix_s": time.time() if wall is None else wall,
            "last_enqueued_frame_index": accepted - 1}, ensure_ascii=False) + "\n")

    event("start", "recording_started")
    print("\n正在录制，可以开始运动。结束：按回车或 Ctrl+C。备注：输入文字并回车。", flush=True)
    try:
        while True:
            if writer.error:
                raise RuntimeError(str(writer.error))
            stop = False
            while not commands.empty():
                kind, label, mono, wall = commands.get_nowait()
                event(kind, label, mono, wall)
                if kind == "stop":
                    reason, stop = label, True
                    break
                print("已标记: " + label, flush=True)
            if stop:
                break
            now = time.monotonic()
            if args.duration and now - start >= args.duration:
                reason = "duration_reached"
                break
            if now - last_new > args.stale_seconds:
                raise RuntimeError("超过 %.1f 秒没有新帧（设备停止更新或连接中断）" % args.stale_seconds)
            try:
                frame = glove.read_frame(timeout=0.2, data_cmd=args.mode).to_dict()
            except TimeoutError:
                timeouts += 1
                continue
            mono, wall = time.monotonic(), time.time()
            key = frame_key(frame)
            if key == last_key:
                duplicates += 1
                time.sleep(0.001)
                continue
            if frame.get("rows") != 63 or frame.get("cols") != 39 or len(frame.get("values", [])) != 2457:
                raise ValueError("帧矩阵尺寸异常；预期63×39，停止采集")
            if not isinstance(frame.get("sequence"), int) or not isinstance(frame.get("timestamp_ms"), int):
                raise ValueError("缺少有效设备时间或序号，无法可靠审计轨迹")
            if frame.get("mode") != args.mode:
                raise ValueError("收到不匹配的输出模式: " + str(frame.get("mode")))
            frame["_capture"] = {"frame_index": accepted, "elapsed_s": mono - start,
                                 "read_host_unix_s": wall}
            writer.submit(frame)
            accepted += 1
            last_new, last_key = mono, key
    except KeyboardInterrupt:
        reason = "ctrl_c"
    except Exception as exc:
        reason, error = "error", str(exc)
    finally:
        event("end", reason)
        events.close()
        try:
            writer.close()
        except Exception as exc:
            reason, error = "write_error", str(exc)
    return {"stop_reason": reason, "error": error, "elapsed_s": time.monotonic() - start,
            "enqueued_frames": accepted, "written_frames": writer.count,
            "duplicate_poll_reads": duplicates, "read_timeouts": timeouts}


class DemoFrame:
    def __init__(self, data):
        self.data = data

    def to_dict(self):
        return dict(self.data)


class DemoGlove:
    """Synthetic data only. Never imports or connects to the real SDK."""
    def __init__(self, hz, mode):
        self.hz, self.mode, self.start = hz, mode, time.monotonic()

    def read_frame(self, **kwargs):
        index = int((time.monotonic() - self.start) * self.hz)
        values = [None] * 2457
        for r0, r1, c0, c1 in [(8,23,0,7),(0,23,8,15),(0,23,16,23),
                               (0,23,24,31),(0,23,32,39),(24,63,8,39)]:
            for r in range(r0, r1):
                for c in range(c0, c1):
                    values[r * 39 + c] = 0
        values[5 * 39 + 10] = int(50 + 40 * math.sin(index / self.hz * 3))
        return DemoFrame({"serial_number": "SYNTHETIC_DEMO", "timestamp": time.time(),
            "timestamp_ms": round(index * 1000 / self.hz), "sequence": index % 65536,
            "mode": self.mode, "rows": 63, "cols": 39, "values": values, "gyro": None})


def parser():
    p = argparse.ArgumentParser(description="千觉USB手套轨迹采集；回车开始/停止，文字回车标记事件。")
    p.add_argument("--port", help="指定串口，如COM5；默认自动发现")
    p.add_argument("--hz", type=int, default=60, help="请求帧率10-120，默认60")
    p.add_argument("--mode", choices=["tare_adc", "adc", "raw"], default="tare_adc")
    p.add_argument("--skip-tare", action="store_true", help="tare_adc模式使用已有去皮结果")
    p.add_argument("--duration", type=float, default=0, help="开始后自动结束的秒数；0为手动")
    p.add_argument("--countdown", type=int, default=3, help="开始前倒计时，默认3秒")
    p.add_argument("--stale-seconds", type=float, default=3, help="无新帧自动停止超时，默认3秒")
    p.add_argument("--out", type=Path, default=Path(__file__).resolve().parent / "recorded_trajectories")
    p.add_argument("--label", default="", help="任务备注，例如轻力夹取")
    p.add_argument("--no-zip", action="store_true", help="结束后不自动生成zip")
    p.add_argument("--demo", action="store_true", help="仅生成模拟数据，完全不连接手套")
    return p


def main(argv=None):
    args = parser().parse_args(argv)
    if not 10 <= args.hz <= 120 or args.duration < 0 or args.countdown < 0 or args.stale_seconds <= 0:
        parser().error("检查hz(10-120)、duration/countdown(>=0)、stale-seconds(>0)")
    if not all(math.isfinite(v) for v in (args.duration, args.stale_seconds)):
        parser().error("时长必须是有限数值")
    glove = None
    folder = None
    metadata = None
    exit_code = 0
    try:
        if args.demo:
            args.duration = args.duration or 3
            glove = DemoGlove(args.hz, args.mode)
            before = after = {"serial_number": "SYNTHETIC_DEMO"}
            sdk_version = "not_loaded"
        else:
            try:
                sdk = importlib.import_module("xenseglovesdk")
            except ImportError as exc:
                raise RuntimeError("请在能运行原record.py的Python环境中执行；缺少官方xenseglovesdk wheel。") from exc
            sdk_version = getattr(sdk, "__version__", "unknown")
            if not args.port:
                ports = [p for p in sdk.XenseGlove.list_ports() if field(p, "is_glove")]
                if not ports:
                    raise RuntimeError("未发现手套。检查USB数据线、数据端口及串口是否被其他程序占用。")
                for i, p in enumerate(ports, 1):
                    print("%d: %s %s" % (i, field(p, "port"), device_info(field(p, "info", {}))))
                if len(ports) > 1:
                    choice = int(input("选择手套编号: "))
                    if not 1 <= choice <= len(ports):
                        raise ValueError("手套编号超出范围")
                    args.port = field(ports[choice - 1], "port")
                else:
                    args.port = field(ports[0], "port")
            glove = sdk.XenseGlove.open(args.port)
            before = device_info(glove.info())
            print("设备:", before)
            print("将设置USB输出为 %d Hz；该帧率设置会由设备保存。" % args.hz)
            glove.set_channel("serial")
            glove.set_frequency(args.hz, target="serial")
            if args.mode == "tare_adc" and not args.skip_tare:
                input("戴好手套，手掌自然张开，不接触物体。准备去皮时按回车: ")
                glove.tare()
                print("保持无负载。去皮后观察读数，确认稳定再开始。")
                for _ in range(3):
                    time.sleep(1)
                    try:
                        sample = glove.read_frame(timeout=1, data_cmd="tare_adc").to_dict()
                        vals = [v for v in sample["values"] if v is not None]
                        print("无负载预览: max=%s, total=%s" % (max(vals, default=0), sum(vals)))
                    except TimeoutError:
                        print("等待去皮后新帧...")
            glove.set_output_mode(args.mode)
            after = device_info(glove.info())
            input("确认无负载读数稳定、准备就绪后按回车，倒计时后开始录制: ")
            for second in range(args.countdown, 0, -1):
                print(second, flush=True)
                time.sleep(1)
        session = ("DEMO_" if args.demo else "") + dt.datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:6]
        folder = args.out.resolve() / session
        folder.mkdir(parents=True, exist_ok=False)
        metadata = {"schema_version": 1, "script_version": SCRIPT_VERSION,
            "status": "recording", "session_id": session, "started_utc": utc_now(),
            "synthetic": args.demo, "label": args.label, "port": args.port,
            "requested_hz": args.hz, "requested_mode": args.mode,
            "tare_performed": not args.demo and args.mode == "tare_adc" and not args.skip_tare,
            "sdk_version": sdk_version, "python": sys.version, "platform": platform.platform(),
            "device_before": before, "device_after": after, "docs": DOCS,
            "timing": "timestamp=SDK接收Unix秒; timestamp_ms=设备毫秒; _capture.elapsed_s=本次录制单调时钟秒",
            "imu": "仅保留压力帧附带gyro；未采集独立IMU流", "video": "本脚本不录视频"}
        write_json(folder / "metadata.json", metadata)
        print("保存位置:", folder)
        result = capture(glove, args, folder)
        metadata.update(result)
        metadata.update(status="error" if result["error"] else "completed", ended_utc=utc_now())
        exit_code = 1 if result["error"] else 0
    except (Exception, KeyboardInterrupt) as exc:
        exit_code = 1
        print("\n停止:", str(exc) or "用户中断", file=sys.stderr)
        if metadata is not None:
            metadata.update(status="error", error=str(exc) or "interrupted", ended_utc=utc_now())
    finally:
        if glove is not None and not args.demo:
            try:
                glove.close()
            except Exception as exc:
                print("关闭连接提示:", exc, file=sys.stderr)
        if folder is not None and metadata is not None:
            write_json(folder / "metadata.json", metadata)
    if folder is not None and (folder / "frames.jsonl").exists():
        try:
            summary = summarize(folder / "frames.jsonl", args.hz)
            write_json(folder / "summary.json", summary)
            print("\n已保存 %d 帧；停止原因: %s" % (summary["saved_frames"], metadata.get("stop_reason", "error")))
            print("设备时间估算的保存频率:", summary["saved_hz_from_device_time"])
            print("估计缺失序号:", summary["missing_sequence_estimate"])
            if metadata.get("error"):
                print("采集异常:", metadata["error"])
            for warning in summary["warnings"]:
                print("提示:", warning)
            if not args.no_zip:
                archive = folder.with_suffix(".zip")
                print("正在压缩，请等待完成...", flush=True)
                with zipfile.ZipFile(archive, "x", zipfile.ZIP_DEFLATED, compresslevel=1) as z:
                    for path in sorted(folder.iterdir()):
                        z.write(path, arcname=folder.name + "/" + path.name)
                print("可发回分析的文件:", archive)
            print("原始数据目录:", folder)
        except Exception as exc:
            exit_code = 1
            print("摘要/压缩未完成，原始目录仍保留: %s\n原因: %s" % (folder, exc), file=sys.stderr)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
