import os
import sys
import json
import time
import threading
import hashlib
import requests
import shutil
import urllib.parse
import logging
import traceback
from flask import Flask, request, jsonify, Response
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s][%(levelname)s][SyncNode] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("SyncNode")

app = Flask(__name__)

pull_pause_flags = {}

# === ���ߺ��� ===

def file_md5(path: str, chunk_size: int = 8192) -> str:
    hash_md5 = hashlib.md5()
    try:
        with open(path, 'rb') as f:
            for chunk in iter(lambda: f.read(chunk_size), b""):
                hash_md5.update(chunk)
    except Exception as e:
        logger.error(f"����MD5ʧ��: {path}���쳣: {e}")
        return None
    return hash_md5.hexdigest()

def safe_path(base: str, path: str) -> str:
    combined = os.path.normpath(os.path.join(base, path))
    base = os.path.abspath(base)
    if not combined.startswith(base):
        raise Exception(f"�Ƿ�·������: {path}")
    return combined

def notify_pause_pull(peer_url: str, pause: bool):
    try:
        requests.post(f"{peer_url}/pause_pull", json={"pause": pause}, timeout=5)
        logger.info(f"֪ͨ {peer_url} {'��ͣ' if pause else '�ָ�'} ��ȡ")
    except Exception:
        logger.warning(f"֪ͨ {peer_url} ������ȡʧ��")
def try_remove_file(path, retries=5, delay=0.5):
    for i in range(retries):
        try:
            if os.path.exists(path):
                os.remove(path)
            return True
        except PermissionError:
            time.sleep(delay)
    return False
# === �ļ�ϵͳ������ ===

class SyncHandler(FileSystemEventHandler):
    def __init__(self, config: dict):
        self.config = config
        self.recent_synced = {}
        self.debounce_timers = {}
        self.name = config['name']
        self.peer_url = f"http://{config['peer_ip']}:{config['peer_port']}"

    def should_skip(self, path: str) -> bool:
        filename = os.path.relpath(path, self.config['shared_folder']).replace("\\", "/")
        if filename.endswith(".uploading") or not os.path.exists(path):
            return True
        md5 = file_md5(path)
        return md5 is None or self.recent_synced.get(filename) == md5

    def record_sync(self, path: str):
        filename = os.path.relpath(path, self.config['shared_folder']).replace("\\", "/")
        md5 = file_md5(path)
        if md5:
            self.recent_synced[filename] = md5

    def debounce_sync(self, path: str):
        filename = os.path.relpath(path, self.config['shared_folder']).replace("\\", "/")
        if filename.endswith(".uploading"):
            return
        if filename in self.debounce_timers:
            self.debounce_timers[filename].cancel()

        def do_sync():
            try:
                notify_pause_pull(self.peer_url, True)
                if not self.should_skip(path):
                    self.sync_file(path)
                    self.record_sync(path)
            finally:
                notify_pause_pull(self.peer_url, False)
                self.debounce_timers.pop(filename, None)

        timer = threading.Timer(0.5, do_sync)
        self.debounce_timers[filename] = timer
        timer.start()

    def sync_file(self, path: str):
        filename = os.path.relpath(path, self.config['shared_folder']).replace("\\", "/")
        encoded = urllib.parse.quote(filename)
        url = f"http://{self.config['peer_ip']}:{self.config['peer_port']}/upload/{encoded}"

        try:
            # === ��������ͣ������ȡ ===
            self.config['_pause_pull'] = True
            logger.info(f"[{self.name}] ��ͣ������ȡ")

            # === ֪ͨ�Զ���ͣ��ȡ ===
            peer_pause_url = f"http://{self.config['peer_ip']}:{self.config['peer_port']}/pause_pull"
            requests.post(peer_pause_url, timeout=5)
            logger.info(f"[{self.name}] ֪ͨ {self.config['peer_ip']} ��ͣ ��ȡ")

            size_mb = os.path.getsize(path) / 1024 / 1024
            logger.info(f"[{self.name}] �ϴ��ļ�: {filename} ({size_mb:.2f}MB)")
            with open(path, 'rb') as f:
                resp = requests.post(url, data=f, headers={'Content-Type': 'application/octet-stream'}, timeout=300)

            if resp.status_code == 200:
                logger.info(f"[{self.name}] �ϴ����: {filename}")
            else:
                logger.warning(f"[{self.name}] �ϴ�ʧ�� {filename}��״̬��: {resp.status_code}")

        except Exception as e:
            logger.error(f"[{self.name}] �ϴ��쳣: {filename}��{e}")

        finally:
            # === �ָ�������ȡ ===
            name = app.config['name']
            pull_pause_flags[name].clear()
            pull_pause_flags[self.name].clear()
            logger.info(f"[{self.name}] �ָ�������ȡ")

            # === ֪ͨ�Զ˻ָ���ȡ ===
            try:
                peer_resume_url = f"http://{self.config['peer_ip']}:{self.config['peer_port']}/resume_pull"
                requests.post(peer_resume_url, timeout=5)
                logger.info(f"[{self.name}] ֪ͨ {self.config['peer_ip']} �ָ� ��ȡ")
            except Exception as e:
                logger.warning(f"[{self.name}] ֪ͨ�Զ˻ָ���ȡʧ��: {e}")


    def sync_delete(self, filename: str):
        encoded = urllib.parse.quote(filename)
        try:
            requests.post(f"{self.peer_url}/delete/{encoded}", timeout=10)
            logger.info(f"[{self.name}] ɾ��Զ���ļ�: {filename}")
        except Exception as e:
            logger.error(f"[{self.name}] ɾ��Զ��ʧ�� {filename}��{e}")

    def sync_mkdir(self, foldername: str):
        encoded = urllib.parse.quote(foldername)
        try:
            requests.post(f"{self.peer_url}/mkdir/{encoded}", timeout=10)
            logger.info(f"[{self.name}] ����Զ���ļ���: {foldername}")
        except Exception as e:
            logger.error(f"[{self.name}] ����Զ���ļ���ʧ�� {foldername}��{e}")

    # === �����¼� ===

    def on_created(self, event):
        rel = os.path.relpath(event.src_path, self.config['shared_folder']).replace("\\", "/")
        if rel.endswith(".uploading"):
            return
        if event.is_directory:
            self.sync_mkdir(rel)
        else:
            self.debounce_sync(event.src_path)

    def on_modified(self, event):
        if not event.is_directory and not event.src_path.endswith(".uploading"):
            self.debounce_sync(event.src_path)

    def on_deleted(self, event):
        rel = os.path.relpath(event.src_path, self.config['shared_folder']).replace("\\", "/")
        if rel.endswith(".uploading"):
            return
        self.recent_synced.pop(rel, None)  # ����ɾ���ļ���ͬ����¼
        self.sync_delete(rel)


    def on_moved(self, event):
        old = os.path.relpath(event.src_path, self.config['shared_folder']).replace("\\", "/")
        new = os.path.relpath(event.dest_path, self.config['shared_folder']).replace("\\", "/")
        if old.endswith(".uploading") or new.endswith(".uploading"):
            return
        self.sync_delete(old)
        if event.is_directory:
            self.sync_mkdir(new)
        else:
            self.debounce_sync(event.dest_path)

# === Flask �ӿ� ===

@app.route('/upload/<path:filename>', methods=['POST'])
def upload(filename):
    try:
        real_path = safe_path(app.config['shared_folder'], filename)
        temp_path = real_path + ".uploading"
        os.makedirs(os.path.dirname(real_path), exist_ok=True)
        pull_pause_flags[app.config['name']].set()
        with open(temp_path, 'wb') as f:
            while True:
                chunk = request.stream.read(8192)
                if not chunk:
                    break
                f.write(chunk)
        os.replace(temp_path, real_path)
        logger.info(f"[{app.config['name']}] �ļ��������: {filename}")
        return 'OK'
    except Exception:
        logger.error(f"[{app.config['name']}] �ϴ��쳣:\n{traceback.format_exc()}")
        return '�ϴ�ʧ��', 400
    finally:
        pull_pause_flags[app.config['name']].clear()

@app.route('/delete/<path:filename>', methods=['POST'])
def delete(filename):
    try:
        path = safe_path(app.config['shared_folder'], filename)
        if os.path.isfile(path):
            success = try_remove_file(path)
            if not success:
                logger.error(f"[{app.config['name']}] ɾ���ļ�ʧ�ܣ���ռ�ã�: {filename}")
                return '�ļ���ռ�ã�ɾ��ʧ��', 400
            logger.info(f"[{app.config['name']}] ɾ���ļ��ɹ�: {filename}")
            return 'OK'
        if os.path.isdir(path):
            shutil.rmtree(path)
            logger.info(f"[{app.config['name']}] ɾ���ļ���: {filename}")
            return 'OK'
        logger.warning(f"[{app.config['name']}] �ļ�������: {filename}")
        return '�ļ�������', 404
    except Exception:
        logger.error(f"[{app.config['name']}] ɾ���쳣:\n{traceback.format_exc()}")
        return 'ɾ��ʧ��', 400


@app.route('/mkdir/<path:foldername>', methods=['POST'])
def mkdir(foldername):
    try:
        path = safe_path(app.config['shared_folder'], foldername)
        os.makedirs(path, exist_ok=True)
        logger.info(f"[{app.config['name']}] �����ļ���: {foldername}")
        return 'OK'
    except Exception:
        logger.error(f"[{app.config['name']}] �����쳣:\n{traceback.format_exc()}")
        return '����ʧ��', 400

@app.route('/list_files', methods=['GET'])
def list_files():
    file_list = []
    for root, _, files in os.walk(app.config['shared_folder']):
        for f in files:
            if f.endswith(".uploading"):
                continue
            full = os.path.join(root, f)
            rel = os.path.relpath(full, app.config['shared_folder']).replace("\\", "/")
            md5 = file_md5(full)
            if md5:
                file_list.append({"path": rel, "md5": md5})
    return jsonify({"files": file_list})

@app.route('/download/<path:filename>', methods=['GET'])
def download(filename):
    try:
        path = safe_path(app.config['shared_folder'], filename)
        if not os.path.isfile(path):
            return "�ļ�������", 404
        def generate():
            with open(path, 'rb') as f:
                while chunk := f.read(8192):
                    yield chunk
        return Response(generate(), mimetype='application/octet-stream')
    except Exception:
        logger.error(f"[{app.config['name']}] �����쳣:\n{traceback.format_exc()}")
        return '����ʧ��', 400

@app.route('/pause_pull', methods=['POST'])
def pause_pull():
    logger.info(f"[{app.config['name']}] �յ� /pause_pull ������ͣ��ȡ")
    pull_pause_flags[app.config['name']].set()
    return 'OK'
@app.route('/resume_pull', methods=['POST'])
def resume_pull():
    name = app.config['name']
    pull_pause_flags[name].clear()
    app.config['_resume_requested'] = True
    logger.info(f"[{name}] �յ��ָ���ȡ����")
    return 'OK'
# === ��ȡ�߳� ===
pending_deletes = {}  # ��ʽ: {path: retry_count}

def pull_loop(config: dict):

    peer_url = f"http://{config['peer_ip']}:{config['peer_port']}"
    pause_flag = pull_pause_flags[config['name']]
    full_scan_interval = 60  # ��
    last_full_scan = 0

    while True:
        if app.config.pop('_resume_requested', False):
            logger.info(f"[{config['name']}] �յ� resume_pull ��������ȡһ��")
        if pause_flag.is_set():
            time.sleep(1)
            continue
        try:
            r = requests.get(f"{peer_url}/list_files", timeout=10)
            if r.status_code == 200:
                peer_files = r.json().get("files", [])
                local_md5 = {}
                for root, _, files in os.walk(config['shared_folder']):
                    for f in files:
                        if f.endswith(".uploading"):
                            continue
                        path = os.path.join(root, f)
                        rel = os.path.relpath(path, config['shared_folder']).replace("\\", "/")
                        md5 = file_md5(path)
                        if md5:
                            local_md5[rel] = md5

                # ��ͨ������ȡ
                for file in peer_files:
                    rel, md5 = file["path"], file["md5"]
                    if local_md5.get(rel) != md5:
                        download_and_replace(peer_url, rel, config['shared_folder'])

                # ��Ƶȫ��ɨ�貹��
                now = time.time()
                if now - last_full_scan > full_scan_interval:
                    # �Աȱ�����Զ���ļ��б��ұ����е�Զ���޵��ļ���ɾ������
                    peer_file_set = set(f["path"] for f in peer_files)
                    local_file_set = set(local_md5.keys())
                    to_delete = local_file_set - peer_file_set
                    for rel_del in to_delete:
                        local_path = os.path.join(config['shared_folder'], rel_del)
                        try:
                            if os.path.isfile(local_path):
                                os.remove(local_path)
                            elif os.path.isdir(local_path):
                                shutil.rmtree(local_path)
                            logger.info(f"[{config['name']}] ȫ��ɨ�貹��ɾ�������ļ�: {rel_del}")
                        except Exception as e:
                            logger.warning(f"[{config['name']}] ɾ��ʧ��: {rel_del}�����������Զ��У��쳣: {e}")
                            pending_deletes[local_path] = 0

                    last_full_scan = now

        except Exception:
            logger.warning(f"[{config['name']}] ��ǰ pause ״̬: {pause_flag.is_set()}")
            logger.error(f"[{config['name']}] ��ȡ�쳣:\n{traceback.format_exc()}")
        # === ɾ�������߼� ===
        to_remove = []
        for path, count in pending_deletes.items():
            try:
                if os.path.isfile(path):
                    os.remove(path)
                    logger.info(f"[{config['name']}] ���Գɹ�ɾ���ļ�: {path}")
                    to_remove.append(path)
                elif os.path.isdir(path):
                    shutil.rmtree(path)
                    logger.info(f"[{config['name']}] ���Գɹ�ɾ��Ŀ¼: {path}")
                    to_remove.append(path)
            except Exception as e:
                pending_deletes[path] += 1
                if pending_deletes[path] >= 5:
                    logger.error(f"[{config['name']}] ���ɾ��ʧ�ܷ���: {path}������쳣: {e}")
                    to_remove.append(path)
                else:
                    logger.warning(f"[{config['name']}] ɾ��ʧ������({pending_deletes[path]}/5): {path}���쳣: {e}")

        for path in to_remove:
            pending_deletes.pop(path, None)
        time.sleep(config.get('pull_interval', 10))

def download_and_replace(peer_url, rel, shared_folder):
    encoded = urllib.parse.quote(rel)
    url = f"{peer_url}/download/{encoded}"
    try:
        resp = requests.get(url, stream=True, timeout=60)
        if resp.status_code == 200:
            local_path = os.path.join(shared_folder, rel)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            temp_path = local_path + ".uploading"
            with open(temp_path, 'wb') as f:
                for chunk in resp.iter_content(8192):
                    if chunk:
                        f.write(chunk)
            os.replace(temp_path, local_path)
            logger.info(f"��ȡ�ļ�: {rel}")
    except Exception as e:
        logger.warning(f"��ȡ�ļ�ʧ��: {rel}, �쳣: {e}")
def should_pause_pull():
    return pull_pause_flags[config['name']].is_set()

# === ���� ===

if __name__ == '__main__':
    if len(sys.argv) < 2:
        logger.error("�÷�: python sync_node_full.py config.json")
        sys.exit(1)
    with open(sys.argv[1], 'r', encoding='utf-8') as f:
        config = json.load(f)
    for k in ['name', 'shared_folder', 'my_port', 'peer_ip', 'peer_port']:
        if k not in config:
            logger.error(f"����ȱʧ: {k}")
            sys.exit(1)
    shared_folder = os.path.abspath(config['shared_folder'])
    os.makedirs(shared_folder, exist_ok=True)
    app.config.update({
        'shared_folder': shared_folder,
        'my_port': config['my_port'],
        'name': config['name']
    })
    pull_pause_flags[config['name']] = threading.Event()
    threading.Thread(target=lambda: app.run(host='0.0.0.0', port=config['my_port'], debug=False, use_reloader=False), daemon=True).start()
    observer = Observer()
    observer.schedule(SyncHandler(config), path=shared_folder, recursive=True)
    observer.start()
    threading.Thread(target=pull_loop, args=(config,), daemon=True).start()
    logger.info(f"[{config['name']}] �����ɹ��������˿�: {config['my_port']}��Ŀ¼: {shared_folder}")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("�˳���...")
        observer.stop()
        observer.join()