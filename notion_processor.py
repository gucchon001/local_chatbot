#notion_processor.py
import logging
from notion_client import Client
from langchain.schema import Document
from notion_client.errors import APIResponseError

logger = logging.getLogger(__name__)

def get_notion_pages(notion_client, database_id, limit=None):
    try:
        pages = []
        has_more = True
        start_cursor = None
        while has_more:
            response = notion_client.databases.query(
                database_id=database_id,
                start_cursor=start_cursor,
                page_size=min(100, limit - len(pages) if limit else 100)
            )
            pages.extend(response['results'])
            has_more = response['has_more'] and (limit is None or len(pages) < limit)
            start_cursor = response['next_cursor']
            if limit and len(pages) >= limit:
                break
        return pages[:limit] if limit else pages
    except APIResponseError as e:
        if 'validation' in str(e).lower() and 'database_id' in str(e).lower():
            logger.error(f"無効なNotion データベースID: {database_id}")
            raise ValueError(f"無効なNotion データベースID: {database_id}. 正しいIDを確認してください。")
        else:
            logger.error(f"Notion APIエラー: {str(e)}")
            raise

def extract_page_content(notion_client, page_id):
    blocks = notion_client.blocks.children.list(block_id=page_id)['results']
    content = ""
    for block in blocks:
        if block['type'] == 'paragraph':
            content += block['paragraph']['rich_text'][0]['plain_text'] if block['paragraph']['rich_text'] else ""
        elif block['type'] == 'heading_1':
            content += "# " + block['heading_1']['rich_text'][0]['plain_text'] if block['heading_1']['rich_text'] else ""
        elif block['type'] == 'heading_2':
            content += "## " + block['heading_2']['rich_text'][0]['plain_text'] if block['heading_2']['rich_text'] else ""
        elif block['type'] == 'heading_3':
            content += "### " + block['heading_3']['rich_text'][0]['plain_text'] if block['heading_3']['rich_text'] else ""
        elif block['type'] == 'bulleted_list_item':
            content += "- " + block['bulleted_list_item']['rich_text'][0]['plain_text'] if block['bulleted_list_item']['rich_text'] else ""
        elif block['type'] == 'numbered_list_item':
            content += "1. " + block['numbered_list_item']['rich_text'][0]['plain_text'] if block['numbered_list_item']['rich_text'] else ""
        # 他のブロックタイプも必要に応じて追加
        content += "\n"
    return content.strip()

def process_notion_database(notion_client, database_id, page_ids=None):
    try:
        if page_ids:
            pages = [notion_client.pages.retrieve(page_id) for page_id in page_ids]
        else:
            pages = get_notion_pages(notion_client, database_id)
        
        documents = []
        for page in pages:
            page_id = page['id']
            title = page['properties'].get('Name', {}).get('title', [{}])[0].get('plain_text', "Untitled")
            content = extract_page_content(notion_client, page_id)
            doc = Document(
                page_content=content,
                metadata={"source": page_id, "title": title}
            )
            documents.append(doc)
        
        logger.info(f"{len(documents)} 個のドキュメントを Notion データベースから取得しました。")
        return documents
    except Exception as e:
        logger.error(f"Notion データベースの処理中にエラーが発生しました: {str(e)}", exc_info=True)
        raise