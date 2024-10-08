#userlog_utils.py
import streamlit as st
import datetime
import os

def generate_log_content(messages, data_source, reference_type, selected_source_name):
    log_lines = []
    
    # データソース情報（最初のメッセージの前にのみ追加）
    log_lines.append(f"データソース: {reference_type} - {selected_source_name}")
    statistics = data_source.get_statistics()
    for key, value in statistics.items():
        log_lines.append(f"{key}: {value}")
    log_lines.append("\n")  # データソース情報の後に空行を追加
    
    for i, message in enumerate(messages):
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if message['role'] == 'user':
            log_lines.append(f"質問 ({timestamp}):")
            log_lines.append(message['content'])
        elif message['role'] == 'assistant':
            log_lines.append(f"回答 ({timestamp}):")
            log_lines.append(message['content'])
            
            # 詳細な参照元の情報を追加
            if 'detailed_sources' in message:
                log_lines.append("\n詳細な参照元:")
                for source in message['detailed_sources']:
                    log_lines.append(f"  ファイル名: {os.path.basename(source['source'])}")
                    log_lines.append(f"  URL: {source['source']}")
                    log_lines.append(f"  ページ: {source['page']}")
                    last_modified = data_source.get_last_modified(source['source'])
                    if last_modified:
                        log_lines.append(f"  最終更新日: {last_modified.strftime('%Y-%m-%d %H:%M:%S')}")
                    log_lines.append(f"  抜粋: {source['content'][:200]}...")
                    log_lines.append("")  # 各ソースの後に空行を追加
        
        log_lines.append("")  # 各メッセージの後に空行を追加
    
    return "\n".join(log_lines)

def display_download_button(messages, data_source, reference_type, selected_source_name, key):
    if messages:
        # ログの内容を生成
        log_content = generate_log_content(messages, data_source, reference_type, selected_source_name)
        
        # ダウンロードボタンを表示
        current_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        filename = f"{current_time}_chatoutput.txt"
        
        st.download_button(
            label="📥 ログをダウンロード",
            data=log_content,
            file_name=filename,
            mime="text/plain",
            key=key  # 提供されたキーを使用
        )