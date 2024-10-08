#database.py
import os
import numpy as np
import pandas as pd
from langchain_openai import OpenAIEmbeddings, ChatOpenAI
from langchain.schema import HumanMessage
from file_cache import check_file_changes, save_file_hashes, load_file_hashes
from document_processor import process_document, find_documents
from vector_store import (create_faiss_index, save_to_parquet, load_from_parquet,
                          save_faiss_index, load_faiss_index)
from web_scraper import scrape_website
import logging
import time
from datetime import datetime
from tenacity import retry, wait_exponential, stop_after_attempt
import random

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, config):
        self.config = config
        self.faiss_index = None
        self._cache = {}
        self.embeddings = None
        self.ensure_embeddings()

    def ensure_embeddings(self):
        if self.embeddings is None:
            try:
                self.embeddings = OpenAIEmbeddings(model=self.config['embeddings_model'])
                logger.info(f"Embeddings オブジェクトを初期化しました: {self.embeddings}")
            except Exception as e:
                logger.error(f"Embeddings オブジェクトの初期化に失敗しました: {str(e)}", exc_info=True)
                raise

    @retry(wait=wait_exponential(multiplier=1, min=4, max=60), stop=stop_after_attempt(5))
    def generate_embeddings(self, texts):
        try:
            self.ensure_embeddings()
            embeddings = self.embeddings.embed_documents(texts)
            logger.info(f"生成されたembeddingsの型: {type(embeddings)}")
            logger.info(f"生成されたembeddingsの長さ: {len(embeddings)}")
            if embeddings:
                logger.info(f"最初のembeddingの型: {type(embeddings[0])}")
                logger.info(f"最初のembeddingの長さ: {len(embeddings[0])}")
            return np.array(embeddings)  # リストをNumPy配列に直接変換
        except Exception as e:
            logger.error(f"Embeddings生成中にエラーが発生しました: {str(e)}", exc_info=True)
            raise

    def process_chunks_with_progress(self, chunks, batch_size=200):
        total_chunks = len(chunks)
        processed_chunks = 0
        all_embeddings = []

        for i in range(0, total_chunks, batch_size):
            batch = chunks[i:i+batch_size]
            try:
                batch_embeddings = self.generate_embeddings([chunk.page_content for chunk in batch])
                if batch_embeddings is not None:
                    all_embeddings.append(batch_embeddings)
                    processed_chunks += len(batch)
                    
                    progress = (processed_chunks / total_chunks) * 100
                    logger.info(f"処理進捗: {progress:.2f}% ({processed_chunks}/{total_chunks})")
                else:
                    logger.error(f"バッチ {i} の embeddings 生成に失敗しました")
                
                time.sleep(random.uniform(10.0, 15.0))  # APIレート制限を回避するための待機時間
            
            except Exception as e:
                logger.error(f"バッチ処理中にエラーが発生しました: {str(e)}", exc_info=True)
                user_input = input("処理を再開しますか？ (y/n): ")
                if user_input.lower() != 'y':
                    logger.info("処理を中断しました")
                    break
                else:
                    logger.info("処理を再開します")
                    continue

        if not all_embeddings:
            return None
        
        # 全てのバッチのエンベディングを1つの大きなNumPy配列に結合
        combined_embeddings = np.vstack(all_embeddings)
        logger.info(f"結合後のエンベディングの形状: {combined_embeddings.shape}")
        return combined_embeddings

    def load_or_create_db(self, source_config):
        logger.info(f"load_or_create_db called with source_config: {source_config}")
        if source_config['参照形式'] == 'ファイル':
            logger.info("ファイルデータベースを作成します")
            return self.load_or_create_file_db(source_config)
        elif source_config['参照形式'] == 'Webサイト':
            logger.info("Webデータベースを作成します")
            return self.load_or_create_web_db(source_config)
        else:
            logger.error(f"Unsupported data source type: {source_config['参照形式']}")
            raise ValueError(f"Unsupported data source type: {source_config['参照形式']}")

    def _check_file_timestamps(self, parquet_file, hash_file):
        if os.path.exists(parquet_file) and os.path.exists(hash_file):
            parquet_mtime = os.path.getmtime(parquet_file)
            hash_mtime = os.path.getmtime(hash_file)
            current_time = time.time()
            if parquet_mtime == hash_mtime and (current_time - parquet_mtime) < 86400:  # 1日 = 86400秒
                logger.info("ファイルの更新日時が一致し、1日以内の更新のため、ハッシュチェックをスキップします")
                return True
        return False

    def _find_documents(self, directory):
        return find_documents(directory)

    def load_or_create_file_db(self, source_config):
        logger.info(f"load_or_create_file_db が呼び出されました: {source_config['名称']}")

        cache_key = source_config['名称']
        if cache_key in self._cache:
            logger.info(f"キャッシュされたデータベースを使用します: {cache_key}")
            return self._cache[cache_key]

        parquet_file = source_config['parquet_file']
        faiss_index_file = source_config['faiss_index_file']
        persist_directory = source_config['persist_directory']

        os.makedirs(persist_directory, exist_ok=True)

        hash_file = os.path.join(persist_directory, 'file_hashes.json')

        document_files = self._find_documents(source_config['参照先'])
        files_changed, current_hashes = check_file_changes(document_files, hash_file)

        if os.path.exists(parquet_file) and os.path.exists(faiss_index_file):
            if not files_changed and self._check_file_timestamps(parquet_file, hash_file):
                result = self._use_existing_db(parquet_file, faiss_index_file)
            else:
                result = self._update_existing_db(source_config, document_files, current_hashes, parquet_file, faiss_index_file, hash_file)
        else:
            result = self._create_new_db_and_index(source_config, document_files, current_hashes, parquet_file, faiss_index_file, hash_file)

        self._cache[cache_key] = result
        return result

    def _use_existing_db(self, parquet_file, faiss_index_file):
        try:
            df = load_from_parquet(parquet_file)
            if self.faiss_index is None:
                self.faiss_index = load_faiss_index(faiss_index_file)
            return df, self.faiss_index, None, self.embeddings, "既存のデータベースを使用しました。"
        except Exception as e:
            logger.error(f"既存のデータベース読み込み中にエラー: {str(e)}")
            return None, None, None, None, f"既存のデータベース読み込み中にエラー: {str(e)}"
    
    def _process_documents(self, document_files):
        all_chunks = []
        for doc_file in document_files:
            logger.info(f"処理中のファイル: {doc_file}")
            chunks = process_document(doc_file)
            all_chunks.extend(chunks)
        logger.info(f"チャンク化されたドキュメント数: {len(all_chunks)}")
        return all_chunks
    
    def _update_existing_db(self, source_config, document_files, current_hashes, parquet_file, faiss_index_file, hash_file):
        try:
            df, index, _, _, _ = self._use_existing_db(parquet_file, faiss_index_file)
            old_hashes = load_file_hashes(hash_file)
            
            new_or_changed_files = [file for file in document_files if file not in old_hashes or old_hashes[file] != current_hashes[file]]
            
            if new_or_changed_files:
                logger.info(f"新規または変更されたファイル: {new_or_changed_files}")
                new_chunks = self._process_documents(new_or_changed_files)
                
                if new_chunks:
                    new_vectors = self.process_chunks_with_progress(new_chunks)
                    new_df = pd.DataFrame({
                        'content': [chunk.page_content for chunk in new_chunks],
                        'source': [chunk.metadata['source'] for chunk in new_chunks],
                        'page': [str(chunk.metadata.get('page', 'N/A')) for chunk in new_chunks]
                    })
                    
                    df = pd.concat([df, new_df], ignore_index=True)
                    index.add(np.array(new_vectors))
                    
                    save_to_parquet(df, parquet_file)
                    save_faiss_index(index, faiss_index_file)
                    save_file_hashes(current_hashes, hash_file)
                    os.utime(hash_file, (os.path.getatime(parquet_file), os.path.getmtime(parquet_file)))
                    
                    logger.info(f"データベースを更新しました。新規チャンク数: {len(new_chunks)}")
                    return df, index, None, self.embeddings, "データベースを更新しました。"
            
            return df, index, None, self.embeddings, "変更はありませんでした。"
        except Exception as e:
            logger.error(f"データベースの更新中にエラーが発生しました: {str(e)}")
            return self._create_new_db_and_index(source_config, document_files, current_hashes, parquet_file, faiss_index_file, hash_file)

    def _create_new_db_and_index(self, source_config, document_files, current_hashes, parquet_file, faiss_index_file, hash_file):
        try:
            all_chunks = self._process_documents(document_files)
            
            logger.info(f"チャンク数: {len(all_chunks)}")
            
            all_vectors = self.process_chunks_with_progress(all_chunks)
            
            if all_vectors is None or len(all_vectors) == 0:
                logger.error("ベクトルの生成に失敗しました")
                return None, None, None, None, "ベクトルの生成に失敗しました"

            logger.info(f"ベクトルの数: {len(all_vectors)}")
            logger.info(f"ベクトルの型: {type(all_vectors)}")
            logger.info(f"ベクトルの形状: {all_vectors.shape}")

            df = pd.DataFrame({
                'content': [chunk.page_content for chunk in all_chunks],
                'source': [chunk.metadata['source'] for chunk in all_chunks],
                'page': [str(chunk.metadata.get('page', 'N/A')) for chunk in all_chunks]
            })

            save_to_parquet(df, parquet_file)

            logger.info(f"NumPy配列の形状: {all_vectors.shape}")
            
            if all_vectors.shape[0] > 0:
                self.faiss_index = create_faiss_index(all_vectors)
                save_faiss_index(self.faiss_index, faiss_index_file)
            else:
                logger.error("空のベクトル配列のため、FAISSインデックスを作成できません")
                return None, None, None, None, "空のベクトル配列のため、FAISSインデックスを作成できません"

            save_file_hashes(current_hashes, hash_file)
            os.utime(hash_file, (os.path.getatime(parquet_file), os.path.getmtime(parquet_file)))

            return df, self.faiss_index, None, self.embeddings, "新しいデータベースを作成しました。"
        except Exception as e:
            logger.error(f"データベースの作成中にエラーが発生しました: {str(e)}", exc_info=True)
            return None, None, None, None, f"データベースの作成中にエラーが発生しました: {str(e)}"

    def load_or_create_web_db(self, source_config):
        logger.info(f"load_or_create_web_db が呼び出されました: {source_config['名称']}")
        try:
            persist_directory_web = source_config.get('persist_directory_web', None)
            if persist_directory_web is None:
                raise ValueError("persist_directory_web が None です。設定を確認してください。")

            logger.info(f"使用される persist_directory_web: {persist_directory_web}")
            
            df, index, role, embeddings, message = scrape_website(source_config['参照先'], source_config)

            if df is None or index is None:
                logger.error("スクレイピングが失敗しました。")
                return None, None, None, None, message
            
            logger.info(f"スクレイピングが完了しました。データフレームの行数: {len(df)}, インデックスサイズ: {index.ntotal}")
            
            return df, index, role, embeddings, message

        except Exception as e:
            logger.error(f"Webデータベースの作成またはロード中にエラーが発生しました: {str(e)}", exc_info=True)
            return None, None, None, None, f"Webデータベースの作成またはロード中にエラーが発生しました: {str(e)}"

    def load_database_once(self, source_config):
        cache_key = source_config['名称']
        if cache_key in self._cache:
            logger.info(f"キャッシュされたデータベースを使用します: {cache_key}")
            return self._cache[cache_key]

        result = self.load_or_create_db(source_config)
        self._cache[cache_key] = result
        return result
    
    def clear_cache(self):
        self._cache.clear()
        self.faiss_index = None
        logger.info("DatabaseManagerのキャッシュをクリアしました")

# 以下の関数はクラスの外部に配置されます
def generate_role_from_db(df, config):
    """データベースに基づいてAIアシスタントの役割を生成する"""
    file_types = df['source'].apply(lambda x: os.path.splitext(x)[1]).value_counts().to_dict()
    total_pages = df['page'].nunique()

    content_summary = df['content'].str.cat(sep=' ')[:1000]  # 最初の1000文字を使用

    llm = ChatOpenAI(model_name=config['openai_model'], temperature=0.7)
    prompt = f"""
    以下の情報に基づいて、AIアシスタントの役割を100文字以内で生成してください：

    - ファイルタイプ: {file_types}
    - 総ページ数: {total_pages}
    - コンテンツサンプル: {content_summary}

    役割には以下の点を含めてください：
    1. アシスタントの主な特徴
    2. 対応できる質問の種類
    3. 情報提供の方法や特徴
    """

    messages = [HumanMessage(content=prompt)]
    response = llm.invoke(messages)
    return response.content.strip()

def search_db(query, df, index, embeddings, k=5):
    query_vector = embeddings.embed_query(query)
    query_vector_np = np.array(query_vector).reshape(1, -1)  # NumPy配列に変換し、2D形状に変更
    D, I = index.search(query_vector_np, k)
    return [{
        'content': df.iloc[i]['content'],
        'source': df.iloc[i]['source'],
        'page': df.iloc[i].get('page', 'N/A')
    } for i in I[0]]