# config.py
import configparser
import os
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_config():
    config = configparser.ConfigParser()
    config.read('settings.ini', encoding='utf-8')

    # Google Sheets API の設定
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    SERVICE_ACCOUNT_FILE = config['GoogleSheets']['SERVICE_ACCOUNT_FILE']
    SPREADSHEET_ID = config['GoogleSheets']['SPREADSHEET_ID']
    SHEET_NAME = config['GoogleSheets']['SHEET_NAME']
    RANGE_NAME = f'{SHEET_NAME}!A1:E'

    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=creds)

    # スプレッドシートから設定を読み込む
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()
    values = result.get('values', [])

    # OpenAI APIキーを環境変数に設定
    os.environ['OPENAI_API_KEY'] = config['API']['openai_api_key']

    config_dict = {
        'openai_model': config['API']['openai_model'],
        'embeddings_model': config['API']['embeddings_model'],
        'temperature': float(config['ChatBot']['temperature']),
        'max_depth': int(config['WebScraper']['max_depth']),
        'data_sources': [],
        'system_message': "あなたは親切で知識豊富なAIアシスタントです。ユーザーの質問に対して、提供された情報源に基づいて日本語で回答してください。",
        'notion_token': config['Notion']['Notion_token']  # Notion API トークンを追加
    }

    logger.info(f"設定が読み込まれました: {config_dict}")

    # スプレッドシートの値を解析
    headers = values[0]
    for row in values[1:]:
        source = dict(zip(headers, row))
        source['depth'] = int(source['階層']) if source['参照形式'] == 'Webサイト' else None
        
        # 各ソースに embeddings_model を追加
        source['embeddings_model'] = config_dict['embeddings_model']
        source['openai_model'] = config_dict['openai_model']

        # Notion トークンを Notion データソースに追加
        if source['参照形式'] == 'Notion':
            source['notion_token'] = config_dict['notion_token']

        if source['参照形式'] == 'Webサイト':
            # Webサイト用のディレクトリ作成
            source['persist_directory_web'] = os.path.abspath(os.path.join(source['参照フォルダ'], source['名称']))
            logger.info(f"Web用 persist_directory_web の設定: {source['persist_directory_web']}")  # ログ追加

            try:
                os.makedirs(source['persist_directory_web'], exist_ok=True)
                logger.info(f"Web用ディレクトリが作成されました: {source['persist_directory_web']}")
            except Exception as e:
                logger.error(f"Web用ディレクトリの作成に失敗しました: {e}")
                continue

            source['parquet_file'] = os.path.join(source['persist_directory_web'], 'vector_store.parquet')
            source['faiss_index_file'] = os.path.join(source['persist_directory_web'], 'faiss_index.bin')
            logger.info(f"Web用 Parquet ファイル: {source['parquet_file']}")  # ログ追加
            logger.info(f"Web用 Faiss インデックス ファイル: {source['faiss_index_file']}")  # ログ追加
        elif source['参照形式'] in ['ファイル', 'Notion']:
            # ファイルまたはNotion用のディレクトリ作成
            source['persist_directory'] = os.path.abspath(os.path.join(source['参照フォルダ'], source['名称']))
            try:
                os.makedirs(source['persist_directory'], exist_ok=True)
                logger.info(f"{source['参照形式']}用ディレクトリが作成されました: {source['persist_directory']}")
            except Exception as e:
                logger.error(f"{source['参照形式']}用ディレクトリの作成に失敗しました: {e}")
                continue

            source['parquet_file'] = os.path.join(source['persist_directory'], 'vector_store.parquet')
            source['faiss_index_file'] = os.path.join(source['persist_directory'], 'faiss_index.bin')

        # ディレクトリとファイルの存在を確認し、ログに記録
        logger.info(f"データソース '{source['名称']}' の設定:")
        logger.info(f"  参照先: {source['参照先']}")
        logger.info(f"  参照先の存在: {os.path.exists(source['参照先']) if source['参照形式'] != 'Notion' else 'N/A (Notion)'}")
        logger.info(f"  参照先のタイプ: {'ディレクトリ' if os.path.isdir(source['参照先']) else 'ファイル' if os.path.isfile(source['参照先']) else 'URL' if source['参照形式'] == 'Webサイト' else 'Notion DB' if source['参照形式'] == 'Notion' else '不明'}")

        config_dict['data_sources'].append(source)

    logger.info(f"読み込まれたデータソース: {[source['名称'] for source in config_dict['data_sources']]}")

    return config_dict