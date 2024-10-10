from langchain.memory import ConversationBufferMemory
from langchain.schema import HumanMessage, AIMessage

class ConversationManager:
    def __init__(self, max_token_limit=1000):
        self.memory = ConversationBufferMemory(return_messages=True)
        self.max_token_limit = max_token_limit

    def add_user_message(self, message):
        self.memory.chat_memory.add_user_message(message)
        self._truncate_memory()

    def add_ai_message(self, message):
        self.memory.chat_memory.add_ai_message(message)
        self._truncate_memory()

    def get_conversation_history(self):
        return self.memory.chat_memory.messages

    def _truncate_memory(self):
        while self._get_memory_tokens() > self.max_token_limit:
            self.memory.chat_memory.messages.pop(0)

    def _get_memory_tokens(self):
        return sum(len(msg.content.split()) for msg in self.memory.chat_memory.messages)

    def clear(self):
        self.memory.clear()