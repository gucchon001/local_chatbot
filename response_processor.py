# response_processor.py
from ai_models import AIModelManager, create_output_parser, create_prompt_template

def process_response(query, search_results, config, custom_role, ai_manager):
    output_parser = create_output_parser()
    format_instructions = output_parser.get_format_instructions()
    prompt = create_prompt_template(custom_role)

    context = "\n".join([f"- {result['content']}" for result in search_results])
    
    messages = prompt.format_messages(context=context, query=query, format_instructions=format_instructions)
    
    response = ai_manager.generate_response(messages, query)

    try:
        parsed_response = output_parser.parse(response)
        if isinstance(parsed_response["important_points"], str):
            parsed_response["important_points"] = [point.strip() for point in parsed_response["important_points"].split('-') if point.strip()]
        
        # 詳細な参照元を追加
        parsed_response["detailed_sources"] = search_results
        
        return parsed_response
    except Exception as e:
        print(f"Error parsing response: {e}")
        return {
            "answer": response,
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