# role_generator.py

from langchain_openai import ChatOpenAI

def generate_role_from_db(df, config):
    # データベースの内容を分析してロールを生成
    file_types = df['source'].apply(lambda x: x.split('.')[-1] if '.' in x else 'unknown').value_counts().to_dict()
    total_pages = len(df)
    
    content_summary = df['content'].str.cat(sep=' ')[:1000]  # 最初の1000文字を使用
    
    llm = ChatOpenAI(model_name=config['openai_model'], temperature=0.7)
    prompt = f"""
    以下の情報に基づいて、AIアシスタントの役割を100文字以内で生成してください：

    - ファイルタイプ: {file_types}
    - 総ページ数: {total_pages}
    - コンテンツサンプル: {content_summary}

    役割には以下の点を含めてください：
    1. アシスタントの主な特徴
    2. 対応できる質問の種類
    3. 情報提供の方法や特徴
    """
    
    messages = [{"role": "user", "content": prompt}]
    response = llm.invoke(messages)
    return response.content.strip()