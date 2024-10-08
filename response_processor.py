# response_processor.py
from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain.output_parsers import ResponseSchema, StructuredOutputParser
from langchain.schema import HumanMessage, SystemMessage

def process_response(query, search_results, config, custom_role):
    llm = ChatOpenAI(model_name=config['openai_model'], temperature=config['temperature'])

    response_schemas = [
        ResponseSchema(name="answer", description="The main answer to the user's question"),
        ResponseSchema(name="important_points", description="A list of important points related to the answer"),
        ResponseSchema(name="additional_info", description="Any additional relevant information"),
        ResponseSchema(name="sources", description="The sources of the information, including document names and page numbers")
    ]
    output_parser = StructuredOutputParser.from_response_schemas(response_schemas)

    format_instructions = output_parser.get_format_instructions()

    template = f"""
    {custom_role}
    以下のコンテキストを使用して、ユーザーの質問に答えてください：

    {{context}}

    Human: {{query}}

    Assistant: 提供されたコンテキストに基づいて、質問に日本語で答えます。
    {{format_instructions}}

    必ず、以下の点に注意してください：
    1. 回答は全て日本語で行ってください。
    2. 重要ポイントは、各ポイントを新しい行に、ダッシュ(-)で始まるリスト形式で記述してください。
    3. 情報源は、ドキュメント名とページ番号を含めて記述してください。
    4. 回答の形式は厳密に守ってください。特に、JSONのような形式は避けてください。
    5. 常に一貫した形式で回答を提供し、2回目以降の応答でも同じ形式を維持してください。
    """

    prompt = ChatPromptTemplate.from_template(template)

    context = "\n".join([f"- {result['content']}" for result in search_results])
    
    messages = prompt.format_messages(context=context, query=query, format_instructions=format_instructions)
    
    # ここを修正
    response = llm.invoke(messages)

    try:
        parsed_response = output_parser.parse(response.content)
        if isinstance(parsed_response["important_points"], str):
            parsed_response["important_points"] = [point.strip() for point in parsed_response["important_points"].split('-') if point.strip()]
        
        # 詳細な参照元を追加
        parsed_response["detailed_sources"] = search_results
        
        return parsed_response
    except Exception as e:
        print(f"Error parsing response: {e}")
        return {
            "answer": response.content,
            "important_points": [],
            "additional_info": "",
            "sources": "",
            "detailed_sources": search_results
        }

def format_sources(search_results):
    sources = set()
    for result in search_results:
        sources.add(f"{result['source']} (ページ: {result['page']})")
    return list(sources)