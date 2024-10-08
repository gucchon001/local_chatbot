from langchain.chains import ConversationalRetrievalChain
from langchain_openai import ChatOpenAI

def initialize_chatbot(db, model_name, temperature):
    qa = ConversationalRetrievalChain.from_llm(
        ChatOpenAI(model_name=model_name, temperature=temperature),
        db.as_retriever(),
        return_source_documents=True
    )
    return qa