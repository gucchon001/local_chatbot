# utils.py
import streamlit as st
import logging
from database import load_or_create_db
from langchain_openai import OpenAIEmbeddings
from data_sources import FileDataSource, WebDataSource

logger = logging.getLogger(__name__)

def format_size(size_bytes):
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} TB"

#@st.cache_resource
def load_database_once(config, data_source):
    logger.info(f"load_database_once が呼び出されました (ソース: {type(data_source).__name__})")
    try:
        df, index, role, message = data_source.load_or_create_db(config)
        logger.info(f"データベースのロードが成功しました: {message}")
        
        source_config = data_source.source_config
        db_type = source_config['参照形式']
        db_path = source_config['persist_directory']
        
        logger.info(f"参照データベース: 名称={source_config['名称']}, タイプ={db_type}, パス={db_path}")
        
        st.sidebar.success(message)
        
        embeddings = OpenAIEmbeddings(model=config['embeddings_model'])
        logger.info("OpenAIEmbeddingsが初期化されました")
        
        return df, index, role, embeddings
    except Exception as e:
        logger.error(f"データベースのロード中にエラーが発生しました: {str(e)}", exc_info=True)
        st.sidebar.error(f"エラー: {str(e)}")
        return None, None, None, None