#data_sources.py
import logging
from abc import ABC, abstractmethod
from document_processor import get_file_statistics
from web_scraper import get_web_statistics
from notion_client import Client
from notion_processor import get_notion_pages
import os
from datetime import datetime
import requests

logger = logging.getLogger(__name__)

class DataSource(ABC):
    def __init__(self, source_config):
        self.source_config = source_config
        self._statistics = None
        self._last_modified_cache = {}

    @abstractmethod
    def _fetch_statistics(self):
        pass

    @abstractmethod
    def _fetch_last_modified(self, item):
        pass

    def get_statistics(self):
        if self._statistics is None:
            self._statistics = self._fetch_statistics()
        return self._statistics

    def get_last_modified(self, item):
        if item not in self._last_modified_cache:
            self._last_modified_cache[item] = self._fetch_last_modified(item)
        return self._last_modified_cache[item]

    def clear_cache(self):
        self._statistics = None
        self._last_modified_cache.clear()
        logger.info(f"キャッシュをクリアしました: {self.source_config['名称']}")

class FileDataSource(DataSource):
    def __init__(self, source_config):
        super().__init__(source_config)
        logger.info(f"FileDataSourceが初期化されました: {source_config['名称']}")
        logger.info(f"FileDataSource 設定: {source_config}")

    def _fetch_statistics(self):
        logger.info(f"FileDataSource: 統計情報を取得します: {self.source_config['名称']}")
        try:
            stats = get_file_statistics(self.source_config['参照先'])
            if stats is None:
                logger.warning(f"get_file_statisticsがNoneを返しました: {self.source_config['参照先']}")
                return {"警告": "統計情報を取得できませんでした"}
            logger.info(f"統計情報を取得しました: {stats}")
            return stats
        except Exception as e:
            logger.error(f"統計情報の取得中にエラーが発生しました: {str(e)}", exc_info=True)
            return {"エラー": str(e)}

    def _fetch_last_modified(self, file_path):
        logger.info(f"FileDataSource: ファイルの最終更新日時を取得します: {file_path}")
        try:
            last_modified = datetime.fromtimestamp(os.path.getmtime(file_path))
            logger.info(f"最終更新日時: {last_modified}")
            return last_modified
        except Exception as e:
            logger.error(f"ファイルの最終更新日時の取得に失敗しました: {str(e)}")
            return None

class WebDataSource(DataSource):
    def __init__(self, source_config):
        super().__init__(source_config)
        self.url = source_config['参照先']
        logger.info(f"WebDataSourceが初期化されました: {source_config['名称']} - {self.url}")
        logger.info(f"WebDataSource 設定: {source_config}")

    def _fetch_statistics(self):
        logger.info(f"WebDataSource: 統計情報を取得します: {self.source_config['名称']} - {self.url}")
        stats = get_web_statistics(self.source_config)
        logger.info(f"統計情報を取得しました: {stats}")
        return stats

    def _fetch_last_modified(self, url):
        if not url.startswith(('http://', 'https://')):
            logger.warning(f"無効なURLです。スキップします: {url}")
            return None

        logger.info(f"WebDataSource: ページの最終更新日時を取得します: {url}")
        try:
            response = requests.head(url, allow_redirects=True)
            last_modified = response.headers.get('Last-Modified')
            if last_modified:
                last_modified_date = datetime.strptime(last_modified, '%a, %d %b %Y %H:%M:%S GMT')
                logger.info(f"最終更新日時: {last_modified_date}")
                return last_modified_date
        except Exception as e:
            logger.error(f"最終更新日時の取得に失敗しました: {str(e)}")
        return None

class NotionDataSource(DataSource):
    def __init__(self, source_config):
        super().__init__(source_config)
        self.notion_id = source_config['参照先']
        self.notion = Client(auth=source_config['notion_token'])
        logger.info(f"NotionDataSourceが初期化されました: {source_config['名称']} - {self.notion_id}")

    def _fetch_statistics(self):
        logger.info(f"NotionDataSource: 統計情報を取得します: {self.source_config['名称']} - {self.notion_id}")
        try:
            pages = get_notion_pages(self.notion, self.notion_id)
            stats = {
                "ページ数": len(pages),
                "最終更新日": max(page['last_edited_time'] for page in pages) if pages else "N/A"
            }
            logger.info(f"統計情報を取得しました: {stats}")
            return stats
        except Exception as e:
            logger.error(f"統計情報の取得中にエラーが発生しました: {str(e)}", exc_info=True)
            return {"エラー": str(e)}

    def _fetch_last_modified(self, page_id):
        logger.info(f"NotionDataSource: ページの最終更新日時を取得します: {page_id}")
        try:
            page = self.notion.pages.retrieve(page_id)
            last_modified = datetime.fromisoformat(page['last_edited_time'].replace('Z', '+00:00'))
            logger.info(f"最終更新日時: {last_modified}")
            return last_modified
        except Exception as e:
            logger.error(f"ページの最終更新日時の取得に失敗しました: {str(e)}")
            return None

def create_data_source(source_config):
    logger.info(f"create_data_source called with: {source_config}")
    if source_config['参照形式'] == 'ファイル':
        return FileDataSource(source_config)
    elif source_config['参照形式'] == 'Webサイト':
        return WebDataSource(source_config)
    elif source_config['参照形式'] == 'Notion':
        return NotionDataSource(source_config)
    else:
        logger.error(f"Unsupported data source type: {source_config['参照形式']}")
        raise ValueError(f"Unsupported data source type: {source_config['参照形式']}")