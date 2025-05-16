import logging
import os
import shutil
import subprocess
import threading
import queue
import time
from urllib.parse import urljoin
import concurrent.futures
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import platform
import sqlite3

class Config:
    def __init__(self, task_id):
        self.task_id = str(task_id)
        self.base_dir = f"task_{self.task_id}"
        self.log_dir = os.path.join(self.base_dir, "logs")
        self.download_path = os.path.join(self.base_dir, "ts_files")
        self.key_dir = os.path.join(self.base_dir, "keys")
        self.decrypted_dir = os.path.join(self.base_dir, "decrypted")
        self.output_dir = os.path.join(self.base_dir, "output")
        for directory in [self.log_dir, self.download_path, self.key_dir, self.decrypted_dir, self.output_dir]:
            os.makedirs(directory, exist_ok=True)
        self.log_file = os.path.join(self.log_dir, f"task_{self.task_id}.log")
        self.logger = self._setup_logger()

    def _setup_logger(self):
        logger = logging.getLogger(f"task_{self.task_id}")
        logger.setLevel(logging.INFO)
        file_handler = logging.FileHandler(self.log_file, encoding='utf-8')
        file_formatter = logging.Formatter("%(asctime)s - [Task %(name)s] - %(message)s", "%Y-%m-%d %H:%M:%S")
        file_handler.setFormatter(file_formatter)
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter("%(asctime)s - [Task %(name)s] - %(message)s", "%Y-%m-%d %H:%M:%S")
        console_handler.setFormatter(console_formatter)
        if logger.handlers:
            logger.handlers.clear()
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        return logger

class TaskStatus:
    PENDING = "等待中"
    DOWNLOADING = "正在下载"
    DECRYPTING = "正在解密"
    MERGING = "正在合并"
    COMPLETED = "已完成"
    FAILED = "失败"
    CLEANING = "清理中"

class DownloadTask:
    def __init__(self, task_id, m3u8_url, task_manager):
        self.task_id = task_id
        self.m3u8_url = m3u8_url
        self.status = TaskStatus.PENDING
        self.progress = 0
        self.config = Config(task_id)
        self.logger = self.config.logger
        self.key_info = None
        self.segment_count = 0
        self.downloaded_segments = 0
        self.task_manager = task_manager

    def create_session(self):
        session = requests.Session()
        retries = Retry(total=5, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])
        adapter = HTTPAdapter(pool_connections=10, pool_maxsize=50, max_retries=retries)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session

    def download_m3u8_segments(self):
        self.status = TaskStatus.DOWNLOADING
        self.task_manager.update_task_in_db(self.task_id, status=self.status)
        self.logger.info(f"开始下载 M3U8: {self.m3u8_url}")
        session = self.create_session()
        try:
            response = session.get(self.m3u8_url)
            response.raise_for_status()
            m3u8_content = response.text
            ts_urls = []
            for line in m3u8_content.splitlines():
                line = line.strip()
                if line.startswith("#EXT-X-KEY"):
                    self.key_info = line
                if line and not line.startswith("#"):
                    ts_urls.append(urljoin(self.m3u8_url, line))
            self.segment_count = len(ts_urls)
            self.logger.info(f"找到 {self.segment_count} 个TS文件链接")
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(20, os.cpu_count() * 2)) as executor:
                futures = {executor.submit(self.download_segment, session, url, i): i for i, url in enumerate(ts_urls)}
                for future in concurrent.futures.as_completed(futures):
                    try:
                        result = future.result()
                        if result:
                            self.downloaded_segments += 1
                            self.progress = int((self.downloaded_segments / self.segment_count) * 100)
                            self.task_manager.update_task_in_db(self.task_id, progress=self.progress)
                    except Exception as e:
                        self.logger.error(f"下载过程中出错: {e}")
            self.logger.info("所有TS文件下载完成")
            self.task_manager.update_task_in_db(self.task_id, status=TaskStatus.DECRYPTING if self.key_info else TaskStatus.MERGING)
            return True
        except requests.exceptions.RequestException as e:
            self.logger.error(f"下载M3U8文件失败: {e}")
            self.status = TaskStatus.FAILED
            self.task_manager.update_task_in_db(self.task_id, status=self.status)
            return False

    def download_segment(self, session, ts_url, index, max_retries=3):
        ts_filename = os.path.join(self.config.download_path, f"segment_{index + 1:04d}.ts")
        if os.path.exists(ts_filename) and os.path.getsize(ts_filename) > 0:
            return True
        for attempt in range(max_retries):
            try:
                ts_response = session.get(ts_url, stream=True, timeout=(5, 15))
                ts_response.raise_for_status()
                with open(ts_filename, 'wb') as f:
                    for chunk in ts_response.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
                self.logger.info(f"片段 {index + 1}/{self.segment_count} 下载完成")
                return True
            except requests.exceptions.RequestException as e:
                self.logger.error(f"下载片段 {index + 1} 尝试 {attempt + 1}/{max_retries} 失败: {e}")
        return False

    def extract_key_uri(self):
        if not self.key_info:
            return None
        parts = self.key_info.split(',')
        for part in parts:
            if part.startswith('URI='):
                uri = part.split('=', 1)[1].strip('"\'')
                return uri
        return None

    def extract_iv(self):
        if not self.key_info:
            return "00000000000000000000000000000000"
        parts = self.key_info.split(',')
        for part in parts:
            if part.startswith('IV='):
                iv = part.split('=', 1)[1]
                if iv.startswith('0x'):
                    iv = iv[2:]
                return iv
        return "00000000000000000000000000000000"

    def download_key(self, key_uri):
        key_file = os.path.join(self.config.key_dir, "encrypt.key")
        session = self.create_session()
        try:
            key_response = session.get(key_uri)
            key_response.raise_for_status()
            with open(key_file, 'wb') as f:
                f.write(key_response.content)
            self.logger.info(f"密钥已下载并保存到 {key_file}")
            return key_file
        except requests.exceptions.RequestException as e:
            self.logger.error(f"下载密钥失败: {e}")
            return None

    def decrypt_ts_files(self, key_file, iv):
        self.status = TaskStatus.DECRYPTING
        self.task_manager.update_task_in_db(self.task_id, status=self.status)  # 更新到数据库
        self.logger.info("开始解密TS文件...")
        if not self.check_external_tool("openssl"):
            self.logger.error("openssl 未安装，无法进行解密")
            self.status = TaskStatus.FAILED
            self.task_manager.update_task_in_db(self.task_id, status=self.status)
            return False
        if not os.path.exists(key_file):
            self.logger.error(f"{key_file} 不存在！")
            self.status = TaskStatus.FAILED
            self.task_manager.update_task_in_db(self.task_id, status=self.status)
            return False
        try:
            with open(key_file, 'rb') as f:
                key_content = f.read()
            key_hex = key_content.hex()
            ts_files = sorted([f for f in os.listdir(self.config.download_path) if f.startswith("segment_") and f.endswith(".ts")])
            total_files = len(ts_files)
            self.logger.info(f"开始解密 {total_files} 个TS文件")
            with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
                futures = {executor.submit(self.decrypt_single_file, filename, key_hex, iv): filename for filename in ts_files}
                decrypted_count = 0
                for future in concurrent.futures.as_completed(futures):
                    try:
                        if future.result():
                            decrypted_count += 1
                            self.progress = int((decrypted_count / total_files) * 100)
                            self.task_manager.update_task_in_db(self.task_id, progress=self.progress)
                    except Exception as e:
                        self.logger.error(f"解密过程中出错: {e}")
            self.logger.info("所有TS文件解密完成！")
            return True
        except Exception as e:
            self.logger.error(f"解密过程出错: {e}")
            self.status = TaskStatus.FAILED
            self.task_manager.update_task_in_db(self.task_id, status=self.status)
            return False

    def decrypt_single_file(self, filename, key_hex, iv):
        input_file = os.path.join(self.config.download_path, filename)
        output_file = os.path.join(self.config.decrypted_dir, filename)
        if os.path.getsize(input_file) == 0:
            return False
        try:
            subprocess.run(["openssl", "aes-128-cbc", "-d", "-in", input_file, "-out", output_file, "-nosalt", "-iv", iv, "-K", key_hex], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            return True
        except subprocess.CalledProcessError as e:
            self.logger.error(f"解密失败: {filename}, 错误: {e}")
            return False

    def copy_to_decrypted(self):
        self.logger.info("未找到加密信息，复制文件到解密目录")
        ts_files = [f for f in os.listdir(self.config.download_path) if f.startswith("segment_") and f.endswith(".ts")]
        with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = []
            for ts_file in ts_files:
                input_path = os.path.join(self.config.download_path, ts_file)
                if os.path.getsize(input_path) == 0:
                    continue
                output_path = os.path.join(self.config.decrypted_dir, ts_file)
                futures.append(executor.submit(shutil.copy, input_path, output_path))
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    self.logger.error(f"复制文件失败: {e}")
        return True

    def merge_ts_files(self):
        self.status = TaskStatus.MERGING
        self.task_manager.update_task_in_db(self.task_id, status=self.status)
        self.logger.info("开始合并TS文件...")
        if not self.check_external_tool("ffmpeg"):
            self.logger.error("ffmpeg 未安装，无法进行合并")
            self.status = TaskStatus.FAILED
            self.task_manager.update_task_in_db(self.task_id, status=self.status)
            return False
        file_list = os.path.join(self.config.base_dir, "file_list.txt")
        ts_files = sorted([f for f in os.listdir(self.config.decrypted_dir) if f.startswith("segment_") and f.endswith(".ts")])
        valid_ts_files = [ts_file for ts_file in ts_files if os.path.getsize(os.path.join(self.config.decrypted_dir, ts_file)) > 0]
        if not valid_ts_files:
            self.logger.error("没有有效的TS文件可以合并")
            self.status = TaskStatus.FAILED
            self.task_manager.update_task_in_db(self.task_id, status=self.status)
            return False
        with open(file_list, 'w', encoding='utf-8') as f:
            for ts_file in valid_ts_files:
                file_path = os.path.abspath(os.path.join(self.config.decrypted_dir, ts_file)).replace('\\', '/')
                f.write(f"file '{file_path}'\n")
        output_video = os.path.join(self.config.output_dir, f"task_{self.task_id}_output.mp4")
        if os.path.exists(output_video):
            os.remove(output_video)
        try:
            subprocess.run(["ffmpeg", "-f", "concat", "-safe", "0", "-i", file_list, "-c", "copy", output_video], check=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            if os.path.exists(output_video) and os.path.getsize(output_video) > 0:
                self.logger.info(f"成功生成输出文件，大小: {os.path.getsize(output_video) / (1024 * 1024):.2f} MB")
            os.remove(file_list)
            self.status = TaskStatus.COMPLETED
            self.task_manager.update_task_in_db(self.task_id, status=self.status, progress=100)
            return True
        except subprocess.CalledProcessError as e:
            self.logger.error(f"合并失败: {e}")
            self.logger.error(f"ffmpeg错误输出: {e.stderr.decode('utf-8', errors='replace')}")
            self.status = TaskStatus.FAILED
            self.task_manager.update_task_in_db(self.task_id, status=self.status)
            return False

    def clean_files(self):
        self.status = TaskStatus.CLEANING
        self.task_manager.update_task_in_db(self.task_id, status=self.status)
        self.logger.info(f"开始清理任务目录: {self.config.base_dir}")
        if os.path.exists(self.config.base_dir):
            try:
                shutil.rmtree(self.config.base_dir)
                self.logger.info(f"成功删除任务目录: {self.config.base_dir}")
            except Exception as e:
                self.logger.error(f"删除目录 {self.config.base_dir} 失败: {e}")
        else:
            self.logger.warning(f"目录不存在: {self.config.base_dir}")

    def check_external_tool(self, tool_name):
        try:
            subprocess.run([tool_name, '-version'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            return True
        except FileNotFoundError:
            return False

    def execute(self):
        try:
            if not self.download_m3u8_segments():
                return False
            if self.key_info:
                key_uri = self.extract_key_uri()
                iv = self.extract_iv()
                if key_uri:
                    key_file = self.download_key(urljoin(self.m3u8_url, key_uri))
                    if key_file and not self.decrypt_ts_files(key_file, iv):
                        return False
                else:
                    if not self.copy_to_decrypted():
                        return False
            else:
                if not self.copy_to_decrypted():
                    return False
            return self.merge_ts_files()
        except Exception as e:
            self.logger.error(f"任务执行过程中出错: {e}")
            self.status = TaskStatus.FAILED
            self.task_manager.update_task_in_db(self.task_id, status=self.status)
            return False

class TaskManager:
    def __init__(self):
        self.tasks = {}
        self.task_counter = 0
        self.task_queue = queue.Queue()
        self.max_concurrent_tasks = 50
        self.active_tasks = 0
        self.lock = threading.Lock()
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - [Manager] - %(message)s", datefmt="%Y-%m-%d %H:%M:%S", handlers=[logging.FileHandler("manager.log", encoding='utf-8'), logging.StreamHandler()])
        self.logger = logging.getLogger("Manager")
        self.worker_thread = threading.Thread(target=self._process_tasks)
        self.worker_thread.daemon = True
        self.worker_thread.start()
        
        self.db_connection = sqlite3.connect('tasks.db')
        self.db_cursor = self.db_connection.cursor()
        self.create_task_table()
        self.load_tasks_from_db()

    def create_task_table(self):
        self.db_cursor.execute('''
            CREATE TABLE IF NOT EXISTS tasks (
                task_id INTEGER PRIMARY KEY,
                m3u8_url TEXT,
                status TEXT,
                progress INTEGER
            )
        ''')
        self.db_connection.commit()
        self.logger.info("任务数据库表已创建或已存在")

    def load_tasks_from_db(self):
        self.db_cursor.execute("SELECT * FROM tasks")
        for row in self.db_cursor.fetchall():
            task_id, m3u8_url, status, progress = row
            task = DownloadTask(task_id, m3u8_url, self)
            task.status = status
            task.progress = progress
            self.tasks[task_id] = task
            self.task_queue.put(task_id)
            self.logger.info(f"从数据库加载任务: Task ID {task_id} - {m3u8_url}")

    def update_task_in_db(self, task_id, status=None, progress=None):
        query = "UPDATE tasks SET "
        updates = []
        if status:
            updates.append(f"status='{status}'")
        if progress is not None:
            updates.append(f"progress={progress}")
        if updates:
            query += ", ".join(updates) + f" WHERE task_id={task_id}"
            self.db_cursor.execute(query)
            self.db_connection.commit()
            self.logger.info(f"任务 {task_id} 在数据库中更新: 状态={status}, 进度={progress}")

    def add_task(self, m3u8_url):
        with self.lock:
            task_id = self.task_counter
            self.task_counter += 1
            task = DownloadTask(task_id, m3u8_url, self)
            self.tasks[task_id] = task
            self.task_queue.put(task_id)
            self.logger.info(f"新任务已添加: Task ID {task_id} - {m3u8_url}")
            self.db_cursor.execute("INSERT INTO tasks (task_id, m3u8_url, status, progress) VALUES (?, ?, ?, ?)",
                                   (task_id, m3u8_url, task.status, task.progress))
            self.db_connection.commit()
            return task_id

    def add_batch_tasks(self, url_file_path):
        added_task_ids = []
        try:
            with open(url_file_path, 'r', encoding='utf-8') as f:
                urls = [line.strip() for line in f if line.strip()]
            if not urls:
                self.logger.warning(f"文件 {url_file_path} 中没有找到有效的URL")
                return added_task_ids
            for url in urls:
                task_id = self.add_task(url)
                added_task_ids.append(task_id)
            self.logger.info(f"成功添加 {len(added_task_ids)} 个批量任务")
            return added_task_ids
        except Exception as e:
            self.logger.error(f"批量添加任务失败: {e}")
            return added_task_ids

    def _process_tasks(self):
        while True:
            with self.lock:
                if self.active_tasks >= self.max_concurrent_tasks:
                    time.sleep(1)
                    continue
            try:
                task_id = self.task_queue.get(block=True, timeout=1)
                with self.lock:
                    self.active_tasks += 1
                task_thread = threading.Thread(target=self._execute_task, args=(task_id,))
                task_thread.daemon = True
                task_thread.start()
            except queue.Empty:
                time.sleep(0.5)
            except Exception as e:
                self.logger.error(f"处理任务时出错: {e}")
                with self.lock:
                    self.active_tasks -= 1

    def _execute_task(self, task_id):
        try:
            task = self.tasks[task_id]
            self.logger.info(f"开始执行任务 {task_id}")
            result = task.execute()
            if result:
                self.logger.info(f"任务 {task_id} 已完成")
            else:
                self.logger.error(f"任务 {task_id} 失败")
        except Exception as e:
            self.logger.error(f"执行任务 {task_id} 时出错: {e}")
        finally:
            with self.lock:
                self.active_tasks -= 1

    def get_task_status(self, task_id):
        if task_id in self.tasks:
            task = self.tasks[task_id]
            return {
                "task_id": task_id,
                "status": task.status,
                "progress": task.progress,
                "url": task.m3u8_url
            }
        return None

    def get_all_task_status(self):
        status_list = []
        for task_id, task in self.tasks.items():
            status_list.append({
                "task_id": task_id,
                "status": task.status,
                "progress": task.progress,
                "url": task.m3u8_url
            })
        return status_list

    def clean_task(self, task_id):
        if task_id in self.tasks:
            task = self.tasks[task_id]
            self.logger.info(f"开始清理任务 {task_id} 的目录")
            task.clean_files()
            self.logger.info(f"任务 {task_id} 的清理已尝试完成")
            return True
        self.logger.warning(f"任务 {task_id} 不存在，无法清理")
        return False

def display_menu():
    print("\n" + "=" * 50)
    print("M3U8多任务下载器")
    print("=" * 50)
    print("1. 添加新下载任务")
    print("2. 批量添加下载任务")
    print("3. 查看所有任务状态")
    print("4. 清理已完成任务的临时文件")
    print("5. 退出程序")
    print("=" * 50)

def main():
    manager = TaskManager()
    while True:
        display_menu()
        choice = input("请选择操作 (1-5): ")
        if choice == '1':
            m3u8_url = input("请输入M3U8视频URL: ").strip()
            if m3u8_url:
                task_id = manager.add_task(m3u8_url)
                print(f"已添加任务，ID: {task_id}")
            else:
                print("URL不能为空！")
        elif choice == '2':
            file_path = input("请输入包含M3U8 URL的文件路径 (每行一个URL): ").strip()
            if os.path.exists(file_path):
                task_ids = manager.add_batch_tasks(file_path)
                if task_ids:
                    print(f"已成功添加 {len(task_ids)} 个任务，ID范围: {min(task_ids)} - {max(task_ids)}")
                else:
                    print("未添加任何任务，请检查文件内容")
            else:
                print(f"文件 {file_path} 不存在！")
        elif choice == '3':
            status_list = manager.get_all_task_status()
            if status_list:
                print("\n当前任务状态:")
                print("-" * 80)
                print(f"{'ID':<5} {'状态':<10} {'进度':<10} {'URL':<50}")
                print("-" * 80)
                for status in status_list:
                    url = status['url'][:42] + "..." if len(status['url']) > 45 else status['url']
                    print(f"{status['task_id']:<5} {status['status']:<10} {status['progress']:>3}% {url}")
            else:
                print("当前没有任务")
        elif choice == '4':
            task_id_input = input("请输入要清理的任务ID (输入 'all' 清理所有已完成任务): ")
            if task_id_input.lower() == 'all':
                status_list = manager.get_all_task_status()
                cleaned_count = 0
                for status in status_list:
                    if status['status'] == TaskStatus.COMPLETED:
                        if manager.clean_task(status['task_id']):
                            cleaned_count += 1
                print(f"已清理 {cleaned_count} 个任务的临时文件")
            else:
                try:
                    task_id = int(task_id_input)
                    if manager.clean_task(task_id):
                        print(f"任务 {task_id} 的临时文件已清理")
                    else:
                        print(f"任务 {task_id} 的清理失败")
                except ValueError:
                    print("无效的任务ID")
        elif choice == '5':
            print("正在退出程序...")
            break
        else:
            print("无效选择，请重试")
        time.sleep(1)

if __name__ == "__main__":
    main()