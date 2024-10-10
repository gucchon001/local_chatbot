# ai_models.py
from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain.output_parsers import ResponseSchema, StructuredOutputParser
from langchain.schema import HumanMessage, SystemMessage, AIMessage
import logging

logger = logging.getLogger(__name__)

class AIModelManager:
    def __init__(self, config):
        self.config = config
        self.llm = ChatOpenAI(model_name=config['openai_model'], temperature=config.get('temperature', 0.7))
        self.system_message = config.get('system_message', "You are a helpful AI assistant.")
        self.conversation_history = []

    def generate_response(self, messages, new_user_input):
        full_messages = [SystemMessage(content=self.system_message)]
        full_messages.extend(self.conversation_history)
        full_messages.extend(messages)
        full_messages.append(HumanMessage(content=new_user_input))
        
        logger.info(f"生成する質問: {new_user_input}")
        try:
            response = self.llm.invoke(full_messages)
            logger.info(f"生成された応答:\n{response.content}")
            
            # 会話履歴を更新
            self.conversation_history.append(HumanMessage(content=new_user_input))
            self.conversation_history.append(AIMessage(content=response.content))
            
            # 会話履歴が長くなりすぎないように制限
            if len(self.conversation_history) > 10:  # 例えば、最新の5往復だけを保持
                self.conversation_history = self.conversation_history[-10:]
            
            return response.content
        except Exception as e:
            logger.error(f"応答生成中にエラーが発生しました: {str(e)}", exc_info=True)
            return f"申し訳ありません。回答の生成中にエラーが発生しました。: {str(e)}"

def create_output_parser():
    response_schemas = [
        ResponseSchema(name="answer", description="The main answer to the user's question"),
        ResponseSchema(name="important_points", description="A list of important points related to the answer"),
        ResponseSchema(name="additional_info", description="Any additional relevant information"),
        ResponseSchema(name="sources", description="The sources of the information, including document names and page numbers")
    ]
    return StructuredOutputParser.from_response_schemas(response_schemas)

def create_prompt_template(custom_role):
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
    return ChatPromptTemplate.from_template(template)