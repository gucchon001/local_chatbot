#app.py
import streamlit as st
from config import load_config
from chat_processing import process_user_input
from ui_components import set_page_config, display_custom_css, display_sidebar_info, display_chat_interface, display_main_title
import logging
from database import DatabaseManager
from data_sources import FileDataSource, WebDataSource
from database import DatabaseManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def clear_cache():
    keys_to_keep = ['config', 'db_manager', 'previous_source', 'reference_type', 'selected_source_name']
    for key in list(st.session_state.keys()):
        if key not in keys_to_keep:
            del st.session_state[key]
    st.cache_resource.clear()
    st.cache_data.clear()
    if 'db_manager' in st.session_state:
        st.session_state.db_manager.clear_cache()
    if 'data_source' in st.session_state:
        st.session_state.data_source.clear_cache()
    logger.info("セッション状態とキャッシュをクリアしました")

def handle_data_source_change(selected_source_name, selected_source_config):
    logger.info(f"handle_data_source_change called with: {selected_source_name}, {selected_source_config}")
    if 'previous_source' not in st.session_state or st.session_state.previous_source != selected_source_name:
        logger.info(f"データソースが変更されました: {st.session_state.get('previous_source')} -> {selected_source_name}")
        st.session_state.previous_source = selected_source_name
        clear_cache()
        if selected_source_config['参照形式'] == 'ファイル':
            logger.info(f"FileDataSource を作成します: {selected_source_config}")
            st.session_state.data_source = FileDataSource(selected_source_config)
        elif selected_source_config['参照形式'] == 'Webサイト':
            logger.info(f"WebDataSource を作成します: {selected_source_config}")
            st.session_state.data_source = WebDataSource(selected_source_config)
        else:
            logger.error(f"Unsupported data source type: {selected_source_config['参照形式']}")
            raise ValueError(f"Unsupported data source type: {selected_source_config['参照形式']}")
        logger.info(f"Created data source: {type(st.session_state.data_source)}")
        return True
    return False


def main():
    logger.info("アプリケーションを開始しました")
    set_page_config()
    display_custom_css()
    display_main_title() 

    if 'config' not in st.session_state:
        st.session_state.config = load_config()
        logger.info("設定をロードしました")

    config = st.session_state.config

    if 'db_manager' not in st.session_state:
        st.session_state.db_manager = DatabaseManager(config)
    
    db_manager = st.session_state.db_manager

    st.sidebar.title("データソース選択")

    reference_type = st.sidebar.radio(
        "参照形式を選択してください:",
        ('ファイル', 'Webサイト'),
        key='reference_type'
    )

    filtered_sources = [source for source in config['data_sources'] if source['参照形式'] == reference_type]
    source_names = [source['名称'] for source in filtered_sources]

    selected_source_name = st.sidebar.selectbox(
        f"利用する{reference_type}を選択してください:",
        options=source_names,
        key='selected_source_name'
    )

    logger.info(f"選択されたデータソース: {reference_type} - {selected_source_name}")

    selected_source_config = next(source for source in filtered_sources if source['名称'] == selected_source_name)

    # データソースが変更されたかどうかをチェック
    if handle_data_source_change(selected_source_name, selected_source_config):
        st.rerun()

    # サイドバーの情報表示
    display_sidebar_info(config)

    if st.sidebar.button("詳細なデバッグ情報を表示"):
        st.sidebar.json(selected_source_config)
        st.sidebar.write("Session State Keys:", list(st.session_state.keys()))
        if 'data_source' in st.session_state:
            st.sidebar.write("Data Source Type:", type(st.session_state.data_source).__name__)
            st.sidebar.write("Data Source Statistics:", st.session_state.data_source.get_statistics())
        if 'df' in st.session_state:
            st.sidebar.write("DataFrame Info:", st.session_state.df.info())
        if 'index' in st.session_state:
            st.sidebar.write("FAISS Index Total:", st.session_state.index.ntotal)

    # データベースのロード
    if 'df' not in st.session_state or 'index' not in st.session_state:
        logger.info("データベースのロードを開始します")
        logger.info(f"選択されたデータソースの設定: {selected_source_config}")
        try:
            logger.info(f"DatabaseManager.load_or_create_db を呼び出します: {selected_source_config}")
            df, index, default_role, embeddings, message = st.session_state.db_manager.load_or_create_db(selected_source_config)
    
            if df is None or index is None:
                logger.error("データベースの読み込みに失敗しました")
                st.error("データベースの読み込みに失敗しました。詳細はログを確認してください。")
                return
            st.session_state.df = df
            st.session_state.index = index
            st.session_state.default_role = default_role
            st.session_state.embeddings = embeddings
            logger.info(f"データベースのロードが完了しました (行数: {len(df)}, インデックスサイズ: {index.ntotal})")
        except Exception as e:
            logger.error(f"データベースのロード中にエラーが発生しました: {str(e)}")
            st.error(f"データベースのロード中にエラーが発生しました: {str(e)}")
            return
    else:
        logger.info("既存のデータベースを使用します")

    if 'custom_role' not in st.session_state:
        st.session_state.custom_role = st.session_state.default_role

    if 'messages' not in st.session_state:
        st.session_state.messages = []

    if 'conversation_count' not in st.session_state:
        st.session_state.conversation_count = 0

    if 'input_key' not in st.session_state:
        st.session_state.input_key = 0

    user_input, send_button = display_chat_interface(
        st.session_state.messages,
        st.session_state.data_source,
        reference_type,
        selected_source_name,
        st.session_state.conversation_count
    )

    if send_button and user_input:
        process_user_input(user_input, st.session_state.df, st.session_state.index, st.session_state.embeddings, config, st.session_state.data_source)
        st.session_state.input_key += 1
        st.session_state.conversation_count += 1
        st.rerun()
    
if __name__ == "__main__":
    main()