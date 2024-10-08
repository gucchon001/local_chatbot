#web_scraper.py
import pandas as pd
import os
import numpy as np
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time
from datetime import datetime
import logging
from langchain.schema import Document
from langchain_openai import OpenAIEmbeddings
from vector_store import create_faiss_index, save_to_parquet, save_faiss_index, load_faiss_index
from file_cache import check_file_changes, save_file_hashes
from role_generator import generate_role_from_db

logger = logging.getLogger(__name__)

# ログ設定の追加
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def get_domain(url):
    return urlparse(url).netloc

def is_valid_url(url, base_url):
    parsed_base = urlparse(base_url)
    parsed_url = urlparse(url)
    
    if parsed_base.netloc != parsed_url.netloc:
        return False
    
    return parsed_url.path.startswith(parsed_base.path)

def get_relative_depth(url, base_url):
    base_parts = base_url.rstrip('/').split('/')
    url_parts = url.rstrip('/').split('/')
    
    return max(len(url_parts) - len(base_parts), 0)

def extract_content(soup):
    title = soup.title.string if soup.title else ""
    description = soup.find('meta', attrs={'name': 'description'})
    description = description['content'] if description else ""

    for tag in soup(['script', 'style']):
        tag.decompose()
    content = ' '.join(soup.stripped_strings)

    return title, description, content

def create_documents(pages):
    documents = []
    for i, (url, soup) in enumerate(pages):
        title, description, content = extract_content(soup)
        last_modified = get_last_modified_date(url)
        doc = Document(
            page_content=content,
            metadata={
                "source": url,
                "title": title,
                "description": description,
                "page": str(i + 1),
                "last_modified": last_modified
            }
        )
        documents.append(doc)
    return documents

def analyze_website_structure(base_url, max_depth):
    visited = set()
    to_visit = [(base_url, 0)]
    structure = {i: 0 for i in range(max_depth + 1)}
    total_links = 0
    url_list = []  # URLを保存するリストを追加

    logger.info(f"サイト構造の分析を開始: {base_url}, 最大深さ: {max_depth}")

    start_time = time.time()

    while to_visit:
        url, depth = to_visit.pop(0)
        logger.debug(f"現在のURL: {url}, 深さ: {depth}, 訪問済みURL数: {len(visited)}")
        
        if url in visited or depth > max_depth or not is_valid_url(url, base_url):
            logger.debug(f"スキップされたURL: {url} (訪問済み: {url in visited}, 深さ: {depth})")
            continue

        visited.add(url)
        structure[depth] += 1
        total_links += 1
        url_list.append((url, depth))  # URLとその深さを保存

        try:
            response = requests.get(url, timeout=10)
            soup = BeautifulSoup(response.text, 'html.parser')

            logger.info(f"ページを取得しました: {url}, 深さ: {depth}, リンク数: {len(soup.find_all('a'))}")

            if depth < max_depth:
                new_links = 0
                for link in soup.find_all('a', href=True):
                    new_url = urljoin(url, link['href'])
                    if is_valid_url(new_url, base_url) and new_url not in visited:
                        new_depth = get_relative_depth(new_url, base_url)
                        to_visit.append((new_url, new_depth))
                        new_links += 1

                logger.info(f"新しいリンクを {new_links} 個発見しました (深さ: {depth})")

        except Exception as e:
            logger.error(f"URL分析中にエラーが発生しました: {url}, エラー: {str(e)}")

        if total_links % 10 == 0:
            elapsed_time = time.time() - start_time
            pages_per_second = total_links / elapsed_time
            logger.info(f"進捗: {total_links} ページを分析済み (速度: {pages_per_second:.2f} ページ/秒)")

    total_pages = sum(structure.values())
    elapsed_time = time.time() - start_time

    logger.info(f"サイト構造の分析完了。総ページ数: {total_pages}")
    logger.info(f"分析に要した時間: {elapsed_time:.2f} 秒")
    logger.info(f"平均分析速度: {total_pages / elapsed_time:.2f} ページ/秒")

    for depth, count in structure.items():
        logger.info(f"深さ {depth}: {count} ページ")

    return structure, total_pages, url_list  # url_listも返す

def crawl_website(url_list, last_crawl_time=None):
    pages = []
    crawled_pages = 0

    logger.info(f"クローリングを開始: {len(url_list)} ページ")

    for url, depth in url_list:
        logger.debug(f"現在のURL: {url}, 深さ: {depth}, クロール済みページ数: {crawled_pages}")

        crawled_pages += 1

        logger.info(f"クロール中: {url} (深さ: {depth})")

        try:
            response = requests.get(url, timeout=10)
            soup = BeautifulSoup(response.text, 'html.parser')
            pages.append((url, soup))

            logger.info(f"ページを取得しました: {url}, 深さ: {depth}")

        except Exception as e:
            logger.error(f"ページのクロール中にエラーが発生しました: {url}, エラー: {str(e)}")

    logger.info(f"クローリング完了。取得したページ数: {crawled_pages}")
    return pages, crawled_pages

def scrape_website(url, config, last_crawl_time=None):
    logger.info(f"ウェブサイトのスクレイピングを開始: {url}")
    logger.info(f"スクレイピングの設定: {config}")
    
    hash_file = os.path.join(config['persist_directory_web'], 'web_hashes.json')
    parquet_file = config['parquet_file']
    faiss_index_file = config['faiss_index_file']

    logger.info(f"使用されるディレクトリ: {config['persist_directory_web']}")
    logger.info(f"Parquet ファイル: {parquet_file}")
    logger.info(f"Faiss インデックス ファイル: {faiss_index_file}")

    files_changed, current_hashes = check_file_changes(url, hash_file, is_website=True)
    logger.info(f"ファイル変更の確認結果: {files_changed}")

    if not files_changed and os.path.exists(parquet_file) and os.path.exists(faiss_index_file):
        logger.info("ウェブサイトに変更がないため、既存のデータベースを使用します。")
        try:
            df = pd.read_parquet(parquet_file)
            index = load_faiss_index(faiss_index_file)
            role = generate_role_from_db(df, config)
            embeddings = OpenAIEmbeddings(model=config['embeddings_model'])
            logger.info("既存のデータベースを正常に読み込みました。")
            return df, index, role, embeddings, "既存のウェブデータベースを読み込みました。"
        except Exception as e:
            logger.error(f"既存データベースの読み込み中にエラーが発生しました: {str(e)}")
            return None, None, None, None, f"データベース読み込み中にエラーが発生しました: {str(e)}"

    logger.info("ウェブサイトの構造を分析します。")
    try:
        crawl_depth = int(config.get('階層', 2))
        structure, total_pages, url_list = analyze_website_structure(url, crawl_depth)
        logger.info(f"構造分析完了。推定総ページ数: {total_pages}")
    except Exception as e:
        logger.error(f"構造分析中にエラーが発生しました: {str(e)}")
        return None, None, None, None, f"構造分析中にエラーが発生しました: {str(e)}"
    
    logger.info(f"推定クロール時間: {total_pages * 2} 秒")

    logger.info("ウェブサイトをクロールしてデータベースを作成します。")
    try:
        pages, crawled_pages = crawl_website(url_list, last_crawl_time)
        logger.info(f"クローリング完了。取得したページ数: {crawled_pages}")
    except Exception as e:
        logger.error(f"クローリング中にエラーが発生しました: {str(e)}")
        return None, None, None, None, f"クローリング中にエラーが発生しました: {str(e)}"
    
    if not pages:
        logger.info(f"新しいページや変更されたページがありません: {url}")
        return None, None, None, None, "変更なし"

    logger.info("ドキュメントを生成します。")
    try:
        documents = create_documents(pages)
        if not documents:
            logger.error(f"生成されたドキュメントがありません: {url}")
            return None, None, None, None, "生成されたドキュメントがありません"
        logger.info(f"生成されたドキュメント数: {len(documents)}")
    except Exception as e:
        logger.error(f"ドキュメント生成中にエラーが発生しました: {str(e)}")
        return None, None, None, None, f"ドキュメント生成中にエラーが発生しました: {str(e)}"

    embeddings_model = config.get('embeddings_model')
    if not embeddings_model:
        raise ValueError("embeddings_model が設定されていません。")

    logger.info(f"使用される embeddings_model: {embeddings_model}")
    embeddings = OpenAIEmbeddings(model=embeddings_model)

    logger.info("埋め込みを作成します。")
    try:
        vectors = embeddings.embed_documents([doc.page_content for doc in documents])
    except Exception as e:
        logger.error(f"埋め込み生成中にエラーが発生しました: {str(e)}")
        return None, None, None, None, f"埋め込み生成中にエラーが発生しました: {str(e)}"
    
    df = pd.DataFrame({
        'content': [doc.page_content for doc in documents],
        'source': [doc.metadata['source'] for doc in documents],
        'title': [doc.metadata.get('title', '') for doc in documents],
        'description': [doc.metadata.get('description', '') for doc in documents]
    })

    logger.info(f"Parquet ファイルを保存します: {parquet_file}")
    try:
        save_to_parquet(df, parquet_file)
        logger.info(f"Parquet ファイルを保存しました: {parquet_file}")
    except Exception as e:
        logger.error(f"Parquet ファイルの保存中にエラーが発生しました: {str(e)}")
        return None, None, None, None, f"Parquet ファイルの保存中にエラーが発生しました: {str(e)}"

    logger.info(f"FAISS インデックスを作成します: {faiss_index_file}")
    try:
        index = create_faiss_index(np.array(vectors))
        save_faiss_index(index, faiss_index_file)
        logger.info(f"FAISS インデックスを保存しました: {faiss_index_file}")
    except Exception as e:
        logger.error(f"FAISS インデックスの作成中にエラーが発生しました: {str(e)}")
        return None, None, None, None, f"FAISS インデックスの作成中にエラーが発生しました: {str(e)}"

    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    save_file_hashes({url: current_time}, hash_file)

    role = generate_role_from_db(df, config)

    logger.info(f"新しいウェブデータベースを作成しました。処理されたページ数: {crawled_pages}")
    return df, index, role, embeddings, f"新しいウェブデータベースを作成しました。処理されたページ数: {crawled_pages}"

def get_web_statistics(config):
    persist_directory = config.get('persist_directory_web')
    if not persist_directory:
        logger.warning("Web統計情報: persist_directory_web が設定されていません")
        return {
            "crawl_depth": config.get('depth', 'N/A'),
            "crawled_pages": 0,
            "last_updated": "N/A",
            "warning": "Web統計情報を完全に取得できません"
        }
    
    # parquet_file を config から直接取得
    parquet_file = config.get('parquet_file')
    if not parquet_file:
        logger.warning("Web統計情報: parquet_file が設定されていません")
        return {
            "crawl_depth": config.get('depth', 'N/A'),
            "crawled_pages": 0,
            "last_updated": "N/A",
            "warning": "Web統計情報を完全に取得できません"
        }
    
    hash_file = os.path.join(persist_directory, 'web_hashes.json')

    crawl_depth = config.get('階層', 2) if config.get('階層') else 2

    statistics = {
        "crawl_depth": crawl_depth,
        "crawled_pages": 0,
        "last_updated": "N/A"
    }

    if os.path.exists(parquet_file):
        df = pd.read_parquet(parquet_file)
        statistics["crawled_pages"] = len(df)
        statistics["total_pages"] = len(df)
        
        # parquetファイルの最終更新日を取得
        last_modified = os.path.getmtime(parquet_file)
        statistics["last_updated"] = datetime.fromtimestamp(last_modified).strftime("%Y-%m-%d %H:%M:%S")
    else:
        logger.warning(f"Parquet ファイルが見つかりません: {parquet_file}")

    logger.info(f"Web統計情報を生成しました: {statistics}")
    return statistics

def get_last_modified_date(url):
    try:
        response = requests.head(url, allow_redirects=True)
        last_modified = response.headers.get('Last-Modified')
        if last_modified:
            return datetime.strptime(last_modified, '%a, %d %b %Y %H:%M:%S GMT').strftime('%Y-%m-%d')
    except:
        pass
    return 'Unknown'

def get_last_updated(parquet_file):
    try:
        last_modified_time = os.path.getmtime(parquet_file)
        last_updated = datetime.fromtimestamp(last_modified_time).strftime('%Y-%m-%d %H:%M:%S')
        return last_updated
    except Exception as e:
        logger.error(f"Parquetファイルの最終更新日時を取得中にエラーが発生しました: {str(e)}")
        return "Unknown"