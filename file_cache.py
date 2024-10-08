# file_cache.py
import os
import hashlib
import json
import requests
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

def calculate_file_hash(file_path):
    try:
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except Exception as e:
        logger.error(f"ファイルハッシュの計算中にエラーが発生しました: {file_path}, エラー: {str(e)}")
        return None

def save_file_hashes(hashes, file_path):
    try:
        with open(file_path, 'w') as f:
            json.dump(hashes, f)
        logger.info(f"ファイルハッシュを保存しました: {file_path}")
    except Exception as e:
        logger.error(f"ファイルハッシュの保存中にエラーが発生しました: {file_path}, エラー: {str(e)}")

def load_file_hashes(file_path):
    try:
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                return json.load(f)
        logger.info(f"ファイルハッシュが見つかりません: {file_path}")
        return {}
    except Exception as e:
        logger.error(f"ファイルハッシュの読み込み中にエラーが発生しました: {file_path}, エラー: {str(e)}")
        return {}

def check_file_changes(files_or_url, hash_file, is_website=False):
    old_hashes = load_file_hashes(hash_file)
    logger.debug(f"ロードされた古いハッシュ: {old_hashes}")
    
    if is_website:
        return check_website_changes(files_or_url, old_hashes)
    else:
        return check_file_system_changes(files_or_url, old_hashes)

def check_website_changes(url, old_hashes):
    current_time = datetime.now()
    if url in old_hashes:
        try:
            old_hash_time = datetime.strptime(old_hashes[url], '%Y-%m-%d %H:%M:%S')
        except ValueError:
            old_hash_time = datetime.fromtimestamp(float(old_hashes[url]))
        
        if current_time - old_hash_time < timedelta(hours=24):
            logger.info(f"24時間以内の変更がないため、再チェックは不要です: {url}")
            return False, old_hashes
    
    logger.info(f"ウェブサイトに変更があるため、再生成します: {url}")
    return True, {url: current_time.strftime('%Y-%m-%d %H:%M:%S')}

def check_file_system_changes(files, old_hashes):
    current_hashes = {}
    files_changed = False
    for file_path in files:
        current_hash = calculate_file_hash(file_path)
        if current_hash is None:
            continue
        current_hashes[file_path] = current_hash
        logger.debug(f"ファイル: {file_path}, 古いハッシュ: {old_hashes.get(file_path)}, 新しいハッシュ: {current_hash}")
        
        if old_hashes.get(file_path) != current_hash:
            logger.info(f"変更が検出されたファイル: {file_path}")
            files_changed = True
    
    if files_changed:
        logger.info("変更が検出されました。新しいハッシュを返します。")
    else:
        logger.info("ファイルに変更がありません。")
    
    return files_changed, current_hashes

def get_website_last_modified(url):
    try:
        response = requests.head(url)
        last_modified = response.headers.get('Last-Modified')
        if last_modified:
            return datetime.strptime(last_modified, '%a, %d %b %Y %H:%M:%S GMT')
    except Exception as e:
        logger.error(f"ウェブサイトの最終更新日時の取得中にエラーが発生しました: {url}, エラー: {str(e)}")
    return datetime.now()