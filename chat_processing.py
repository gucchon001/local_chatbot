#chat_processing.py
import streamlit as st
import logging
from response_processor import process_response
from database import search_db
from ai_models import AIModelManager

logger = logging.getLogger(__name__)

def process_user_input(user_input, df, index, embeddings, config, data_source):
    logger.info("process_user_input が呼び出されました")
    st.session_state.messages.append({"role": "user", "content": user_input})
    
    if 'ai_manager' not in st.session_state:
        st.session_state.ai_manager = AIModelManager(config)
    
    try:
        search_results = search_db(user_input, df, index, embeddings)
        logger.info(f"検索結果: {len(search_results)} 件")
    except Exception as e:
        logger.error(f"search_db でエラーが発生しました: {e}")
        st.error("検索中にエラーが発生しました")
        return
    
    with st.spinner('回答を生成中...'):
        try:
            processed_response = process_response(user_input, search_results, config, st.session_state.custom_role, st.session_state.ai_manager)
            logger.info("回答が正常に生成されました")
        except Exception as e:
            logger.error(f"process_response でエラーが発生しました: {e}")
            st.error("回答の生成中にエラーが発生しました")
            return
    
    response = f"{processed_response['answer']}\n\n"
    
    if processed_response["important_points"]:
        response += "**重要ポイント:**\n"
        for point in processed_response["important_points"]:
            response += f"- {point}\n"
        response += "\n"
    
    if processed_response["additional_info"]:
        response += f"**補足情報:**\n{processed_response['additional_info']}\n\n"
    
    if processed_response["sources"]:
        response += f"**参照元:**\n{processed_response['sources']}"
    
    st.session_state.messages.append({
        "role": "assistant", 
        "content": response,
        "detailed_sources": processed_response["detailed_sources"]
    })

    logger.info("セッション状態を更新しました")