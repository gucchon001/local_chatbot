import streamlit as st
import os
from document_processor import get_file_statistics
from web_scraper import get_web_statistics
from data_sources import FileDataSource, WebDataSource
from userlog_utils import display_download_button

def set_page_config():
    st.set_page_config(page_title="ストミンAIチャット", layout="wide")

def display_custom_css():
    css_content = """
        <style>
        body { background-color: #ffffff; }
        .stApp { background-color: #ffffff; }
        .css-1d391kg { padding-top: 0rem; }
        .stTextInput>div>div>input { background-color: #f0f2f6; }
        .stButton>button {
            background-color: #4CAF50;
            color: white;
            border-radius: 20px;
        }
        .chat-container { 
            display: flex; 
            flex-direction: column; 
            align-items: flex-start; 
            max-width: 800px; 
            margin: 0 auto; 
        }
        .chat-message { 
            display: flex; 
            align-items: flex-start; 
            margin-bottom: 1rem; 
            width: 100%; 
            padding: 1rem;
            border-radius: 0.5rem;
        }
        .chat-message.user { 
            justify-content: flex-end; 
            background-color: #e6f3ff;
            border-bottom-right-radius: 0;
        }
        .chat-message.bot { 
            background-color: #f0f2f6; 
            border-bottom-left-radius: 0;
        }
        .chat-message .avatar { 
            width: 40px; 
            height: 40px; 
            margin: 0 10px; 
            border-radius: 50%;
            object-fit: cover;
        }
        .message-content { 
            max-width: calc(100% - 60px); 
        }
        .input-container { 
            display: flex; 
            flex-direction: column; 
            align-items: stretch; 
            max-width: 800px; 
            margin: 1rem auto; 
        }
        .input-container .stTextArea { margin-bottom: 10px; }
        .input-container .stButton { display: flex; justify-content: flex-end; }
        .input-container .stButton > button { width: auto; }
        .main-title {
            display: flex;
            align-items: center;
            font-size: 2.5rem;
            font-weight: bold;
            margin-bottom: 0.5rem;
        }
        .main-title svg {
            margin-right: 0.5rem;
        }
        .data-source-info {
            font-size: 1rem;
            margin-bottom: 0.5rem;
        }
        .stTextArea textarea {
            font-size: 1rem;
        }
        </style>
    """
    st.markdown(css_content, unsafe_allow_html=True)

def display_main_title():
    st.markdown("""
        <div class="main-title">
            <svg xmlns="http://www.w3.org/2000/svg" width="36" height="36" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"></path>
            </svg>
            ストミンAIチャット
        </div>
    """, unsafe_allow_html=True)

def display_sidebar_info(config):
    st.sidebar.subheader("システム情報")
    
    st.sidebar.write(f"使用モデル (チャット): {config['openai_model']}")
    st.sidebar.write(f"使用モデル (Embeddings): {config['embeddings_model']}")
    st.sidebar.write(f"Temperature: {config['temperature']}")

def display_chat_messages(messages, data_source):
    st.markdown('<div class="chat-container">', unsafe_allow_html=True)
    for message in messages:
        role_class = "user" if message['role'] == 'user' else "bot"
        avatar_url = "https://via.placeholder.com/40/4CAF50/ffffff?text=You" if role_class == "user" else "https://via.placeholder.com/40/2196F3/ffffff?text=Bot"
        st.markdown(f'''
            <div class="chat-message {role_class}">
                <img src="{avatar_url}" class="avatar" alt="{role_class}">
                <div class="message-content">{message["content"]}</div>
            </div>
        ''', unsafe_allow_html=True)
        if message['role'] == 'assistant' and 'detailed_sources' in message:
            with st.expander("詳細な参照元"):
                for source in message['detailed_sources']:
                    st.markdown(f"**ファイル名**: {os.path.basename(source['source'])}")
                    st.markdown(f"**URL**: {source['source']}")
                    st.markdown(f"**ページ**: {source['page']}")
                    last_modified = data_source.get_last_modified(source['source'])
                    if last_modified:
                        st.markdown(f"**最終更新日**: {last_modified.strftime('%Y-%m-%d %H:%M:%S')}")
                    st.markdown(f"**抜粋**: {source['content'][:200]}...")
                    st.markdown("---")
    st.markdown('</div>', unsafe_allow_html=True)

def display_chat_interface(messages, data_source, reference_type, selected_source_name, conversation_count):
    # チャットメッセージを表示
    display_chat_messages(messages, data_source)
    
    # ダウンロードボタンを表示（メッセージがあり、かつ会話が開始された後のみ）
    if messages and conversation_count > 0:
        st.markdown("<div style='margin-bottom: 1rem;'>", unsafe_allow_html=True)
        display_download_button(
            messages, 
            data_source, 
            reference_type, 
            selected_source_name,
            key=f"download_button_{conversation_count}"
        )
        st.markdown("</div>", unsafe_allow_html=True)
    
    # 入力フィールドを表示
    user_input, send_button = display_input_field(
        key=f"user_input_{conversation_count}",
        reference_type=reference_type,
        selected_source_name=selected_source_name,
        data_source=data_source,
        conversation_count=conversation_count
    )
    
    return user_input, send_button

def display_statistics(data_source):
    statistics = data_source.get_statistics()
    if isinstance(data_source, WebDataSource):
        if '警告' in statistics:
            st.warning(statistics['警告'])
        else:
            display_web_statistics(statistics)
    elif isinstance(data_source, FileDataSource):
        display_file_statistics(statistics)
    else:
        st.markdown("統計情報を取得できませんでした。")

def display_input_field(key, reference_type, selected_source_name, data_source, conversation_count):
    with st.container():
        st.markdown('<div class="input-container">', unsafe_allow_html=True)
        
        # 最初の会話の時のみデータ参照先の情報をアコーディオンで表示
        if conversation_count == 0:
            with st.expander(f"データ参照先：{reference_type} - {selected_source_name}"):
                display_statistics(data_source)
        
        user_input = st.text_area("質問を入力してください:", key=key, height=100)
        col1, col2, col3 = st.columns([7, 2, 1])
        with col3:
            send_button = st.button("送信")
        st.markdown('</div>', unsafe_allow_html=True)
    return user_input, send_button

def display_web_statistics(statistics):
    st.markdown(f"**クロール深さ**: {statistics.get('crawl_depth', 'N/A')}")
    st.markdown(f"**クロールされたページ数**: {statistics.get('crawled_pages', 'N/A')}")
    st.markdown(f"**総ページ数**: {statistics.get('total_pages', 'N/A')}")
    st.markdown(f"**最終更新日**: {statistics.get('last_updated', 'N/A')}")

    if statistics.get('crawled_pages', 0) == 0:
        st.warning("クロールされたページがありません。データの更新が必要かもしれません。")

def display_file_statistics(stats):
    if not stats or not isinstance(stats, dict):
        st.warning("統計情報が利用できません。")
        return

    st.subheader("ファイル統計情報")

    # 最終更新日の表示
    if '最終更新日' in stats:
        st.write(f"最終更新日: {stats['最終更新日']}")
    elif '警告' in stats:
        st.warning(stats['警告'])
    else:
        st.write("最終更新日: 情報なし")

    # ファイル数の表示
    file_count = stats.get('ファイル数', '情報なし')
    st.write(f"総ファイル数: {file_count}")

    # 総サイズの表示
    total_size = stats.get('総サイズ', '情報なし')
    if isinstance(total_size, (int, float)):
        st.write(f"総サイズ: {total_size:,} バイト")
    else:
        st.write(f"総サイズ: {total_size}")

    # ファイルタイプ別数の表示
    file_types = stats.get('ファイルタイプ', {})
    if file_types:
        st.write("ファイルタイプ別数:")
        for file_type, count in file_types.items():
            st.write(f"  {file_type}: {count}")
    else:
        st.write("ファイルタイプ別数: 情報なし")