#tool_integration.py
import logging
from langchain.tools import Tool
from langchain.agents import create_react_agent, AgentExecutor
from langchain.prompts import PromptTemplate
from langchain_openai import ChatOpenAI
from datetime import datetime
import pytz

logger = logging.getLogger(__name__)

def get_current_time():
    japan_tz = pytz.timezone('Asia/Tokyo')
    current_time = datetime.now(japan_tz)
    return current_time.strftime("%Y年%m月%d日 %H時%M分%S秒")

def get_current_time_wrapper(input_string: str = "") -> str:
    """
    get_current_time() 関数をラップし、入力文字列を無視します。
    これにより、ツールとして使用する際に引数が渡されても問題なく動作します。
    """
    return get_current_time()

def calculate(expression: str) -> str:
    """
    安全に数式を評価します。
    :param expression: 評価する数式
    :return: 計算結果
    """
    try:
        return str(eval(expression, {"__builtins__": None}, {"abs": abs, "round": round}))
    except Exception as e:
        return f"計算エラー: {str(e)}"

class ToolManager:
    def __init__(self, llm):
        self.llm = llm
        self.tools = [
            Tool(
                name="CurrentTime",
                func=get_current_time_wrapper,
                description="現在の日本時間を取得します。日付や時刻に関する質問に使用します。"
            ),
            Tool(
                name="Calculator",
                func=calculate,
                description="簡単な数式を計算します。計算に関する質問に使用します。"
            )
        ]
        
        prompt = PromptTemplate.from_template(
            "あなたは役立つAIアシスタントです。以下のツールを使用して質問に答えてください：\n"
            "{tools}\n"
            "利用可能なツール名: {tool_names}\n"
            "人間: {input}\n"
            "AI: それでは、質問に答えるためにステップバイステップで考えていきましょう。\n"
            "必ず以下の形式で回答してください：\n"
            "Thought: 考えたこと\n"
            "Action: 使用するツール名\n"
            "Action Input: ツールに渡す入力\n"
            "Observation: ツールからの出力\n"
            "... (この過程を必要な回数繰り返します)\n"
            "Thought: 最終的な考え\n"
            "Final Answer: 人間への最終的な回答\n\n"
            "{agent_scratchpad}"
        )
        
        tool_names = ", ".join([tool.name for tool in self.tools])
        
        agent = create_react_agent(self.llm, self.tools, prompt)
        self.agent_executor = AgentExecutor(
            agent=agent, 
            tools=self.tools, 
            verbose=True,
            handle_parsing_errors=True,
            max_iterations=5  # 無限ループを防ぐために最大イテレーション数を設定
        )

    def run(self, query: str) -> str:
        try:
            tool_names = ", ".join([tool.name for tool in self.tools])
            result = self.agent_executor.invoke({
                "input": query,
                "tool_names": tool_names
            })
            logger.info(f"エージェントの実行結果: {result}")
            
            # 思考プロセスと最終回答を抽出
            thoughts = []
            final_answer = ""
            for step in result.get('intermediate_steps', []):
                action = step[0]
                observation = step[1]
                thoughts.append(f"Action: {action.tool}\nAction Input: {action.tool_input}\nObservation: {observation}")
            
            if 'output' in result:
                final_answer = result['output']
            
            formatted_result = "\n\n".join(thoughts + [f"Final Answer: {final_answer}"])
            logger.info(f"フォーマットされた結果:\n{formatted_result}")
            
            return formatted_result
        except Exception as e:
            logger.error(f"エラーが発生しました: {str(e)}", exc_info=True)
            return f"エラーが発生しました: {str(e)}"