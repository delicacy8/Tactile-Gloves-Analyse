#!/usr/bin/env python3
"""Run on the Windows PC with the official glove SDK and USB glove.
python diagnose_xense_capture.py --port COM4
All phases keep current device settings. Output is local JSON/CSV plus a ZIP.
"""
import argparse
import csv
import hashlib
import importlib
import inspect
import json
import math
import platform
import queue
import statistics
import sys
import threading
import time
import uuid
import zipfile
from datetime import datetime
from pathlib import Path

VERSION = '1.0.0'


def write_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding='utf-8')


def field(obj, name, default=None):
    return obj.get(name, default) if isinstance(obj, dict) else getattr(obj, name, default)


def quantiles(values):
    if not values:
        return None
    a = sorted(values)
    return {'n': len(a), 'min': a[0], 'median': statistics.median(a),
            'p95': a[max(0, math.ceil(.95 * len(a)) - 1)], 'max': a[-1],
            'mean': sum(a) / len(a)}


def info_dict(info):
    return {k: field(info, k) for k in ['serial_number', 'version', 'hand', 'rows', 'cols',
                                      'frequency', 'ble_frequency', 'channel', 'output_mode_name']}


def sdk_inventory(sdk, folder):
    """Read only this installed package's source/stubs; never scan site-packages."""
    module_file = Path(sdk.__file__).resolve() if getattr(sdk, '__file__', None) else None
    result = {'version': getattr(sdk, '__version__', None), 'module_file': str(module_file),
              'public_api': {}, 'package_files': []}
    for cls_name in ['XenseGlove', 'Frame']:
        cls = getattr(sdk, cls_name, None)
        if cls is None:
            continue
        result['public_api'][cls_name] = {}
        for name in dir(cls):
            if name.startswith('_'):
                continue
            try:
                obj = inspect.getattr_static(cls, name)
                try:
                    signature = str(inspect.signature(obj))
                except (ValueError, TypeError):
                    signature = None
                result['public_api'][cls_name][name] = {'signature': signature,
                                                       'doc': (inspect.getdoc(obj) or '')[:5000]}
            except Exception as exc:
                result['public_api'][cls_name][name] = {'inspection_error': str(exc)}
    if module_file and module_file.exists():
        root = module_file.parent
        files = sorted(root.rglob('*')) if root.name == 'xenseglovesdk' else [module_file]
        budget = 1024 * 1024
        for path in files:
            if not path.is_file() or '__pycache__' in path.parts or path.is_symlink():
                continue
            if path.suffix.lower() not in ['.py', '.pyi', '.pyd', '.so', '.dll']:
                continue
            relative = path.relative_to(root)
            item = {'file': str(relative), 'bytes': path.stat().st_size}
            if item['bytes'] <= 64 * 1024 * 1024:
                item['sha256'] = hashlib.sha256(path.read_bytes()).hexdigest()
            if path.suffix in ['.py', '.pyi'] and item['bytes'] <= min(budget, 256 * 1024):
                dest = folder / 'sdk_source' / relative
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_bytes(path.read_bytes())
                budget -= item['bytes']
                item['text_copied'] = True
            result['package_files'].append(item)
    write_json(folder / 'sdk_inventory.json', result)
    return result


class Writer:
    def __init__(self, path):
        self.queue = queue.Queue(maxsize=2048)
        self.error = None
        self.count = 0
        self.observed_max_queue = 0
        self.serialize_ms = []
        self.file_write_ms = []
        self.flush_ms = []
        self.file = path.open('x', encoding='utf-8', buffering=1024 * 1024)
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        last = time.perf_counter()
        try:
            while True:
                try:
                    item = self.queue.get(timeout=.2)
                except queue.Empty:
                    t = time.perf_counter()
                    self.file.flush()
                    self.flush_ms.append((time.perf_counter() - t) * 1000)
                    continue
                if item is None:
                    break
                t = time.perf_counter()
                text = json.dumps(item, ensure_ascii=False, separators=(',', ':')) + '\n'
                self.serialize_ms.append((time.perf_counter() - t) * 1000)
                t = time.perf_counter()
                self.file.write(text)
                self.file_write_ms.append((time.perf_counter() - t) * 1000)
                self.count += 1
                if time.perf_counter() - last >= 1:
                    t = time.perf_counter()
                    self.file.flush()
                    self.flush_ms.append((time.perf_counter() - t) * 1000)
                    last = time.perf_counter()
        except Exception as exc:
            self.error = str(exc)
        finally:
            try:
                self.file.close()
            except Exception as exc:
                self.error = self.error or str(exc)

    def submit(self, item):
        if self.error:
            raise RuntimeError('Writer: ' + self.error)
        try:
            self.queue.put_nowait(item)
        except queue.Full:
            raise RuntimeError('Writer queue full; stopping without silent drops')
        self.observed_max_queue = max(self.observed_max_queue, self.queue.qsize())

    def close(self):
        while self.thread.is_alive():
            try:
                self.queue.put(None, timeout=.2)
                break
            except queue.Full:
                pass
        self.thread.join()
        if self.error:
            raise RuntimeError('Writer: ' + self.error)

    def summary(self):
        return {'written_frames': self.count, 'observed_max_queue': self.observed_max_queue,
                'json_serialize_ms': quantiles(self.serialize_ms),
                'buffered_file_write_ms': quantiles(self.file_write_ms),
                'flush_ms': quantiles(self.flush_ms), 'error': self.error}


def analyze_rows(rows):
    result = {'unique_frames': len(rows)}
    if len(rows) < 2:
        return result
    pairs = []
    discontinuities = 0
    for a, b in zip(rows, rows[1:]):
        ds = (b['sequence'] - a['sequence']) % 65536
        dt = (b['timestamp_ms'] - a['timestamp_ms']) % 4294967296
        if not (0 < ds < 32768 and 0 < dt < 2147483648):
            discontinuities += 1
            continue
        pairs.append((dt, ds, (b['sdk_timestamp_s'] - a['sdk_timestamp_s']) * 1000))
    result['discontinuities'] = discontinuities
    result['sdk_to_app_read_ms'] = quantiles([x['sdk_to_app_read_ms'] for x in rows])
    result['saved_interval_ms'] = quantiles([x[0] for x in pairs])
    result['sdk_timestamp_interval_ms'] = quantiles([x[2] for x in pairs])
    result['sequence_step_counts'] = {str(s): sum(x[1] == s for x in pairs) for s in sorted(set(x[1] for x in pairs))}
    result['batch_pair_lt1ms_device_ge10ms'] = sum(sdk < 1 and dt >= 10 for dt, ds, sdk in pairs)
    if pairs and discontinuities == 0:
        span = sum(x[0] for x in pairs) / 1000
        result.update(device_span_s=span, visible_hz_device_clock=len(pairs) / span,
                      sequence_implied_hz=sum(x[1] for x in pairs) / span,
                      missing_sequence_estimate=sum(x[1] - 1 for x in pairs),
                      visible_hz_host_clock=(len(rows) - 1) / (rows[-1]['read_mono_s'] - rows[0]['read_mono_s']))
    return result


def run_phase(glove, mode, name, seconds, poll_sleep, folder, start_key=None):
    full = name in ['B_convert_no_disk', 'C_convert_and_write']
    writer = Writer(folder / 'C_frames.jsonl') if name == 'C_convert_and_write' else None
    rows, polls = [], []
    calls = duplicates = timeouts = 0
    previous_key = start_key
    first = previous_poll = last_new = time.perf_counter()
    cpu_start = time.process_time()
    error = None
    try:
        while time.perf_counter() - first < seconds:
            begin = time.perf_counter()
            loop_gap_ms = (begin - previous_poll) * 1000
            previous_poll = begin
            if begin - last_new > 3:
                raise RuntimeError('No new sequence for >3s; device stream may have stopped')
            if writer and writer.error:
                raise RuntimeError(writer.error)
            calls += 1
            try:
                frame = glove.read_frame(timeout=.2, data_cmd=mode)
            except TimeoutError:
                timeouts += 1
                polls.append((begin-first, 'timeout', (time.perf_counter()-begin)*1000, 0, loop_gap_ms, None))
                continue
            read_end = time.perf_counter()
            data = None
            convert_ms = 0
            if full:
                t = time.perf_counter()
                data = frame.to_dict()
                convert_ms = (time.perf_counter() - t) * 1000
            obj = data if full else frame
            seq, ms, sdk_time = field(obj, 'sequence'), field(obj, 'timestamp_ms'), field(obj, 'timestamp')
            if not isinstance(seq, int) or not isinstance(ms, int) or not isinstance(sdk_time, (int, float)):
                raise RuntimeError('SDK frame lacks documented sequence/timestamp_ms/timestamp attributes')
            wall = time.time()
            end = time.perf_counter()
            key = (ms, seq)
            is_new = key != previous_key
            polls.append((begin-first, 'new' if is_new else 'cached', (read_end-begin)*1000,
                          convert_ms, loop_gap_ms, (wall-sdk_time)*1000))
            if not is_new:
                duplicates += 1
                time.sleep(poll_sleep)
                continue
            row = {'phase': name, 'index': len(rows), 'read_mono_s': end-first,
                   'sequence': seq, 'timestamp_ms': ms, 'sdk_timestamp_s': sdk_time,
                   'read_host_unix_s': wall, 'sdk_to_app_read_ms': (wall-sdk_time)*1000,
                   'read_call_ms': (read_end-begin)*1000, 'to_dict_ms': convert_ms}
            rows.append(row)
            if writer:
                data['_capture'] = {'frame_index': len(rows)-1, 'elapsed_s': end-first,
                                    'read_host_unix_s': wall, 'diagnostic_phase': name}
                writer.submit(data)
            previous_key, last_new = key, end
    except KeyboardInterrupt:
        error = 'Interrupted by user'
    except Exception as exc:
        error = str(exc)
    finally:
        measured_elapsed = time.perf_counter() - first
        cpu_elapsed = time.process_time() - cpu_start
        if writer:
            try:
                writer.close()
            except Exception as exc:
                error = error or str(exc)
        with (folder / (name + '_unique.csv')).open('w', newline='', encoding='utf-8') as h:
            w = csv.DictWriter(h, fieldnames=['phase','index','read_mono_s','sequence','timestamp_ms',
                'sdk_timestamp_s','read_host_unix_s','sdk_to_app_read_ms','read_call_ms','to_dict_ms'])
            w.writeheader()
            w.writerows(rows)
        with (folder / (name + '_polls.csv')).open('w', newline='', encoding='utf-8') as h:
            w = csv.writer(h)
            w.writerow(['elapsed_s','result','read_call_ms','to_dict_ms','loop_start_gap_ms','cache_age_ms'])
            w.writerows(polls)
    result = analyze_rows(rows)
    result.update(phase=name, elapsed_s=measured_elapsed, cpu_s=cpu_elapsed, calls=calls,
                  calls_per_s=calls/measured_elapsed, duplicate_reads=duplicates, timeouts=timeouts,
                  read_call_ms=quantiles([x[2] for x in polls]),
                  to_dict_ms=quantiles([x[3] for x in polls]) if full else None,
                  loop_start_gap_ms=quantiles([x[4] for x in polls]),
                  cached_frame_age_ms=quantiles([x[5] for x in polls if x[1]=='cached']),
                  writer=writer.summary() if writer else None, error=error)
    write_json(folder / (name + '_summary.json'), result)
    return result, previous_key


def describe(results):
    lines = ['SDK capture diagnosis: observational A-B-C-A comparison.',
             'A=attributes only; B=to_dict on every poll; C=B plus queued JSONL disk write;',
             'A2 repeats lightweight reading to check temporal drift. Device settings unchanged.']
    for r in results:
        lines.append('%s: unique=%d, visible_hz=%s, seq_hz=%s, missing=%s, error=%s' % (
            r['phase'],r['unique_frames'],r.get('visible_hz_device_clock'),r.get('sequence_implied_hz'),
            r.get('missing_sequence_estimate'),r.get('error')))
    lines += ['', 'Interpretation requires paired timings, not a single arbitrary pass threshold:',
              '- If both A phases remain around 9-10 Hz with burst timestamps, removing conversion/disk is insufficient; investigate serial/SDK delivery and latest-frame buffering.',
              '- If A improves and B falls, inspect per-poll to_dict cost and loop scheduling.',
              '- If B improves and C falls, inspect writer CPU, queue growth, serialization and flush timings.',
              '- SDK-source inspection can identify a receive buffer/timeout or frame queue; do not change private/binary internals without matching source.',
              '- This experiment does not directly measure internal ADC sample clocks or raw USB packet arrival.']
    return '\n'.join(lines) + '\n'


def main(argv=None):
    parser = argparse.ArgumentParser(description='Xense低保存率诊断，默认4个阶段各10秒。')
    parser.add_argument('--port', help='例如COM4；默认自动发现')
    parser.add_argument('--seconds', type=float, default=10, help='每阶段秒数，默认10')
    parser.add_argument('--poll-sleep', type=float, default=.001, help='缓存重复时休眠秒数，与原脚本默认一致')
    parser.add_argument('--out', type=Path, default=Path(__file__).resolve().parent/'diagnostic_runs')
    parser.add_argument('--yes', action='store_true', help='跳过开始前回车')
    args = parser.parse_args(argv)
    if not math.isfinite(args.seconds) or args.seconds <= 0 or not math.isfinite(args.poll_sleep) or args.poll_sleep < 0:
        parser.error('seconds须为正的有限数，poll-sleep须为非负有限数')
    folder = args.out.resolve() / (datetime.now().strftime('%Y%m%d_%H%M%S')+'_'+uuid.uuid4().hex[:6])
    folder.mkdir(parents=True)
    glove = None
    results = []
    meta = {'diagnostic_version':VERSION,'python':sys.version,'platform':platform.platform(),
            'seconds_per_phase':args.seconds,'poll_sleep_s':args.poll_sleep,'status':'starting'}
    exit_code = 0
    try:
        sdk = importlib.import_module('xenseglovesdk')
        sdk_inventory(sdk, folder)
        if not args.port:
            ports = [p for p in sdk.XenseGlove.list_ports() if field(p,'is_glove')]
            if len(ports) == 0:
                raise RuntimeError('未发现手套，请连接USB数据端口；或通过 --port COM4 指定')
            if len(ports) == 1:
                args.port = field(ports[0],'port')
            else:
                for i,p in enumerate(ports,1): print(i,field(p,'port'))
                choice = int(input('选择手套端口编号: '))
                if not 1 <= choice <= len(ports): raise ValueError('端口编号超出范围')
                args.port = field(ports[choice-1],'port')
        glove = sdk.XenseGlove.open(args.port)
        meta.update(sdk_version=getattr(sdk,'__version__','unknown'),port=args.port,
                    device=info_dict(glove.info()),status='running')
        write_json(folder/'metadata.json',meta)
        mode = meta['device'].get('output_mode_name')
        if mode not in ['tare_adc','adc','raw']:
            raise RuntimeError('无法识别当前数据模式，请先用原采集工具确认模式')
        print('设备:',meta['device'])
        print('将测4个阶段，每阶段%.1f秒。保持USB连接即可，不要求做动作。' % args.seconds)
        if not args.yes: input('准备好后按回车开始: ')
        cached = glove.read_frame(timeout=1,data_cmd=mode)
        key = (field(cached,'timestamp_ms'),field(cached,'sequence'))
        for name in ['A_minimal','B_convert_no_disk','C_convert_and_write','A2_minimal_repeat']:
            print('测量阶段:',name,flush=True)
            result,key = run_phase(glove,mode,name,args.seconds,args.poll_sleep,folder,key)
            results.append(result)
            print('新帧%d; 可见帧率=%s Hz' % (result['unique_frames'],result.get('visible_hz_device_clock')),flush=True)
            if result['error']: raise RuntimeError(result['error'])
        meta['status'] = 'completed'
    except (Exception, KeyboardInterrupt) as exc:
        meta.update(status='error',error=str(exc) or 'Interrupted by user')
        print('诊断停止:',meta['error'])
        exit_code = 1
    finally:
        if glove is not None:
            try: glove.close()
            except Exception as exc: meta['close_error']=str(exc);exit_code=1
        write_json(folder/'metadata.json',meta)
        write_json(folder/'all_phase_summaries.json',results)
        (folder/'comparison.txt').write_text(describe(results),encoding='utf-8')
        archive = folder.with_suffix('.zip')
        with zipfile.ZipFile(archive,'x',zipfile.ZIP_DEFLATED) as z:
            for path in sorted(folder.rglob('*')):
                if path.is_file():z.write(path,arcname=folder.name+'/'+str(path.relative_to(folder)))
        print('诊断结果ZIP:',archive)
        print('将这个ZIP上传到原GitHub仓库即可继续定位。')
    return exit_code


if __name__ == '__main__':
    raise SystemExit(main())
