# vector_store.py
import faiss
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import base64
import os
import shutil
import logging
import uuid

logger = logging.getLogger(__name__)

def create_faiss_index(vectors):
    try:
        logger.info(f"ベクトルの形状: {vectors.shape}")
        index = faiss.IndexFlatL2(vectors.shape[1])
        index.add(vectors)
        logger.info(f"FAISSインデックスを作成しました。サイズ: {index.ntotal}")
        return index
    except Exception as e:
        logger.error(f"FAISSインデックスの作成中にエラーが発生しました: {str(e)}", exc_info=True)
        raise

def save_to_parquet(df, file_path):
    try:
        table = pa.Table.from_pandas(df)
        pq.write_table(table, file_path)
        logger.info(f"データフレームをParquetファイルとして保存しました: {file_path}")
    except Exception as e:
        logger.error(f"Parquetファイルの保存中にエラーが発生しました: {str(e)}", exc_info=True)
        raise

def load_from_parquet(file_path):
    try:
        df = pd.read_parquet(file_path)
        df['page'] = df['page'].astype(str)
        logger.info(f"Parquetファイルからデータフレームを読み込みました: {file_path}")
        return df
    except Exception as e:
        logger.error(f"Parquetファイルの読み込み中にエラーが発生しました: {str(e)}", exc_info=True)
        raise

def save_faiss_index(index, file_path):
    temp_file_path = None
    try:
        unique_id = uuid.uuid4().hex
        encoded_file_path = base64.urlsafe_b64encode(f"{file_path}_{unique_id}".encode('utf-8')).decode('utf-8')
        temp_directory = "C:\\ref_file\\temp_faiss"
        temp_file_path = os.path.join(temp_directory, encoded_file_path)

        os.makedirs(temp_directory, exist_ok=True)

        logger.info(f"FAISSインデックスを一時ディレクトリに保存します: {temp_file_path}")
        faiss.write_index(index, temp_file_path)
        logger.info(f"FAISSインデックスの保存に成功しました: {temp_file_path}")

        shutil.move(temp_file_path, file_path)
        logger.info(f"FAISSインデックスを一時ディレクトリから移動しました: {file_path}")
    except Exception as e:
        logger.error(f"FAISSインデックスの保存中にエラーが発生しました: {str(e)}", exc_info=True)
        raise
    finally:
        if temp_file_path and os.path.exists(temp_file_path):
            os.remove(temp_file_path)
            logger.info(f"一時ファイルを削除しました: {temp_file_path}")

def load_faiss_index(file_path):
    temp_file_path = None
    try:
        encoded_file_path = base64.urlsafe_b64encode(file_path.encode('utf-8')).decode('utf-8')
        temp_directory = "C:\\ref_file\\temp_faiss"
        temp_file_path = os.path.join(temp_directory, encoded_file_path)

        if os.path.exists(file_path):
            os.makedirs(temp_directory, exist_ok=True)
            
            shutil.copy(file_path, temp_file_path)
            logger.info(f"FAISSインデックスを一時ディレクトリにコピーしました: {temp_file_path}")

            index = faiss.read_index(temp_file_path)
            logger.info(f"FAISSインデックスを読み込みました: {temp_file_path}")
            return index
        else:
            raise FileNotFoundError(f"FAISSインデックスファイルが見つかりません: {file_path}")
    except Exception as e:
        logger.error(f"FAISSインデックスの読み込み中にエラーが発生しました: {str(e)}", exc_info=True)
        raise
    finally:
        if temp_file_path and os.path.exists(temp_file_path):
            os.remove(temp_file_path)
            logger.info(f"一時ファイルを削除しました: {temp_file_path}")