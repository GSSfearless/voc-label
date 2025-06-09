#!/usr/bin/env python3
"""
批量文本预处理服务调用脚本
用于并发调用预处理接口对文本数据进行清洗
"""

import asyncio
import json
import pandas as pd
import aiohttp
import time
from typing import List, Dict, Any, Optional, Set
from dataclasses import dataclass
from pathlib import Path
import logging
import numpy as np

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class PreprocessorConfig:
    """预处理API配置类"""
    base_url: str = "http://localhost:8001"
    max_concurrent: int = 10
    timeout: int = 30
    retry_attempts: int = 3
    retry_delay: int = 1
    
    # 预处理选项
    remove_pii: bool = True
    emoji_convert: bool = True
    emoji_remove: bool = False
    remove_social_mentions: bool = True
    remove_weibo_reposts: bool = True
    remove_hashtags: bool = True
    enable_author_blacklist: bool = False
    remove_ads: bool = True
    remove_urls: bool = True
    normalize_whitespace: bool = True
    normalize_unicode: bool = True
    convert_fullwidth: bool = True
    detect_language: bool = False
    split_sentences: bool = False
    max_length: int = 10000
    min_length: int = 1

@dataclass
class ProcessConfig:
    """处理配置类"""
    input_csv: str
    output_csv: str
    text_column: str = "content"  # 待处理的文本列名
    author_column: Optional[str] = None  # 作者列名（可选）
    id_column: Optional[str] = None  # ID列名（可选）
    max_rows: Optional[int] = None
    random_sample_size: Optional[int] = None
    random_seed: Optional[int] = None
    jsonl_file: Optional[str] = None  # 进度保存文件
    batch_size: int = 50  # 每多少行保存一次
    filter_column: Optional[str] = None  # 筛选字段名
    filter_values: Optional[List[Any]] = None  # 筛选值列表
    filter_condition: Optional[str] = "in"  # 筛选条件

class BatchPreprocessor:
    """批量预处理器"""
    
    def __init__(self, api_config: PreprocessorConfig, process_config: ProcessConfig):
        self.api_config = api_config
        self.process_config = process_config
        self.semaphore = asyncio.Semaphore(api_config.max_concurrent)
        self.session = None
        
        # 设置默认的jsonl文件路径
        if self.process_config.jsonl_file is None:
            base_name = Path(self.process_config.output_csv).stem
            self.process_config.jsonl_file = f"{base_name}_preprocessor_progress.jsonl"
    
    async def __aenter__(self):
        """异步上下文管理器入口"""
        connector = aiohttp.TCPConnector(limit=self.api_config.max_concurrent * 2)
        timeout = aiohttp.ClientTimeout(total=self.api_config.timeout)
        self.session = aiohttp.ClientSession(connector=connector, timeout=timeout)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器退出"""
        if self.session:
            await self.session.close()
    
    def load_data(self) -> pd.DataFrame:
        """加载CSV数据"""
        try:
            df = pd.read_csv(self.process_config.input_csv)
            logger.info(f"成功加载数据，共 {len(df)} 行")
            
            if self.process_config.text_column not in df.columns:
                raise ValueError(f"列 '{self.process_config.text_column}' 不存在于CSV文件中")
            
            # 随机抽样处理
            if self.process_config.random_sample_size:
                if self.process_config.random_sample_size > len(df):
                    logger.warning(f"随机抽样数量 {self.process_config.random_sample_size} 大于总行数 {len(df)}，将处理全部数据")
                    self.process_config.random_sample_size = len(df)
                
                # 设置随机种子以确保可重复性
                if self.process_config.random_seed is not None:
                    np.random.seed(self.process_config.random_seed)
                    logger.info(f"设置随机种子为 {self.process_config.random_seed}")
                
                # 随机抽样
                df = df.sample(n=self.process_config.random_sample_size).reset_index(drop=True)
                logger.info(f"随机抽样 {self.process_config.random_sample_size} 行数据进行处理")
            
            # 如果设置了最大行数限制（在随机抽样之后应用）
            elif self.process_config.max_rows:
                df = df.head(self.process_config.max_rows)
                logger.info(f"限制处理行数为 {self.process_config.max_rows}")
            
            return df
        except Exception as e:
            logger.error(f"加载数据失败: {e}")
            raise
    
    def apply_filter(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """应用筛选条件，返回(需要处理的数据, 完整数据)"""
        if not self.process_config.filter_column or self.process_config.filter_values is None:
            # 如果没有配置筛选，所有数据都需要处理
            return df.copy(), df.copy()
        
        # 检查筛选字段是否存在
        if self.process_config.filter_column not in df.columns:
            raise ValueError(f"筛选字段 '{self.process_config.filter_column}' 不存在于CSV文件中")
        
        filter_col = self.process_config.filter_column
        filter_values = self.process_config.filter_values
        filter_condition = self.process_config.filter_condition
        
        # 应用筛选条件
        if filter_condition == "in":
            mask = df[filter_col].isin(filter_values)
        elif filter_condition == "not_in":
            mask = ~df[filter_col].isin(filter_values)
        elif filter_condition == "equals":
            mask = df[filter_col] == filter_values[0] if filter_values else False
        elif filter_condition == "not_equals":
            mask = df[filter_col] != filter_values[0] if filter_values else True
        else:
            raise ValueError(f"不支持的筛选条件: {filter_condition}")
        
        filtered_df = df[mask].copy()
        total_count = len(df)
        filtered_count = len(filtered_df)
        
        logger.info(f"筛选条件: {filter_col} {filter_condition} {filter_values}")
        logger.info(f"筛选结果: {filtered_count}/{total_count} 行数据将进入预处理")
        
        return filtered_df, df.copy()
    
    async def call_preprocessor_api(self, text: str, author: Optional[str] = None, 
                                   row_id: Optional[str] = None, row_index: int = 0) -> Dict[str, Any]:
        """调用预处理API并返回结果"""
        async with self.semaphore:
            for attempt in range(self.api_config.retry_attempts):
                try:
                    headers = {
                        "Content-Type": "application/json",
                        "Accept": "application/json",
                        "User-Agent": "batch-preprocessor/1.0.0"
                    }
                    
                    # 构建请求payload
                    payload = {
                        "text": text,
                        "options": {
                            "remove_pii": self.api_config.remove_pii,
                            "emoji_convert": self.api_config.emoji_convert,
                            "emoji_remove": self.api_config.emoji_remove,
                            "remove_social_mentions": self.api_config.remove_social_mentions,
                            "remove_weibo_reposts": self.api_config.remove_weibo_reposts,
                            "remove_hashtags": self.api_config.remove_hashtags,
                            "enable_author_blacklist": self.api_config.enable_author_blacklist,
                            "remove_ads": self.api_config.remove_ads,
                            "remove_urls": self.api_config.remove_urls,
                            "normalize_whitespace": self.api_config.normalize_whitespace,
                            "normalize_unicode": self.api_config.normalize_unicode,
                            "convert_fullwidth": self.api_config.convert_fullwidth,
                            "detect_language": self.api_config.detect_language,
                            "split_sentences": self.api_config.split_sentences,
                            "max_length": self.api_config.max_length,
                            "min_length": self.api_config.min_length
                        }
                    }
                    
                    # 添加可选字段
                    if row_id:
                        payload["id"] = row_id
                    if author:
                        payload["author"] = author
                    
                    url = f"{self.api_config.base_url}/v1/nlp/preprocess"
                    async with self.session.post(url, headers=headers, json=payload) as response:
                        if response.status == 200:
                            result = await response.json()
                            logger.info(f"行 {row_index} 预处理成功")
                            return {
                                "row_index": row_index,
                                "success": True,
                                "result": result,
                                "error": None
                            }
                        else:
                            error_text = await response.text()
                            logger.warning(f"行 {row_index} API调用失败 (状态码: {response.status}): {error_text}")
                            if attempt < self.api_config.retry_attempts - 1:
                                await asyncio.sleep(self.api_config.retry_delay * (attempt + 1))
                                continue
                            else:
                                return {
                                    "row_index": row_index,
                                    "success": False,
                                    "result": None,
                                    "error": f"HTTP {response.status}: {error_text}"
                                }
                
                except asyncio.TimeoutError:
                    logger.warning(f"行 {row_index} 请求超时 (第{attempt + 1}次尝试)")
                    if attempt < self.api_config.retry_attempts - 1:
                        await asyncio.sleep(self.api_config.retry_delay * (attempt + 1))
                        continue
                    else:
                        return {
                            "row_index": row_index,
                            "success": False,
                            "result": None,
                            "error": "请求超时"
                        }
                
                except Exception as e:
                    logger.error(f"行 {row_index} 处理异常: {e}")
                    if attempt < self.api_config.retry_attempts - 1:
                        await asyncio.sleep(self.api_config.retry_delay * (attempt + 1))
                        continue
                    else:
                        return {
                            "row_index": row_index,
                            "success": False,
                            "result": None,
                            "error": str(e)
                        }
    
    async def process_batch(self) -> pd.DataFrame:
        """批量处理数据"""
        # 加载数据
        df = self.load_data()
        
        # 应用筛选条件
        filtered_df, full_df = self.apply_filter(df)
        
        if len(filtered_df) == 0:
            logger.warning("没有数据需要处理")
            return full_df
        
        # 加载已处理的进度
        processed_indices = self.load_processed_indices()
        logger.info(f"已处理 {len(processed_indices)} 行数据")
        
        # 找出还需要处理的行
        remaining_df = filtered_df[~filtered_df.index.isin(processed_indices)].copy()
        
        if len(remaining_df) == 0:
            logger.info("所有数据都已处理完成")
            # 从jsonl文件加载结果
            saved_results = self.load_from_jsonl()
            return self.merge_results_with_original(full_df, saved_results)
        
        logger.info(f"剩余 {len(remaining_df)} 行数据需要处理")
        
        # 准备并发任务
        tasks = []
        for idx, row in remaining_df.iterrows():
            text = str(row[self.process_config.text_column]) if pd.notna(row[self.process_config.text_column]) else ""
            author = str(row[self.process_config.author_column]) if self.process_config.author_column and pd.notna(row[self.process_config.author_column]) else None
            row_id = str(row[self.process_config.id_column]) if self.process_config.id_column and pd.notna(row[self.process_config.id_column]) else None
            
            task = self.call_preprocessor_api(text, author, row_id, idx)
            tasks.append(task)
        
        # 分批处理
        results = []
        batch_size = self.process_config.batch_size
        start_time = time.time()
        
        for i in range(0, len(tasks), batch_size):
            batch_tasks = tasks[i:i + batch_size]
            logger.info(f"处理批次 {i // batch_size + 1}/{(len(tasks) + batch_size - 1) // batch_size}，包含 {len(batch_tasks)} 个任务")
            
            # 执行当前批次
            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            
            # 处理异常结果
            for j, result in enumerate(batch_results):
                if isinstance(result, Exception):
                    logger.error(f"任务异常: {result}")
                    actual_idx = remaining_df.iloc[i + j].name
                    results.append({
                        "row_index": actual_idx,
                        "success": False,
                        "result": None,
                        "error": str(result)
                    })
                else:
                    results.append(result)
            
            # 保存进度
            self.save_to_jsonl(batch_results)
            
            # 记录进度
            processed_count = min(i + batch_size, len(tasks))
            elapsed_time = time.time() - start_time
            avg_time_per_item = elapsed_time / processed_count
            eta = avg_time_per_item * (len(tasks) - processed_count)
            
            logger.info(f"已处理 {processed_count}/{len(tasks)}，"
                       f"平均耗时 {avg_time_per_item:.2f}s/条，"
                       f"预计剩余时间 {eta:.1f}s")
        
        # 合并结果
        all_results = self.load_from_jsonl()
        result_df = self.merge_results_with_original(full_df, all_results)
        
        # 保存最终结果
        self.save_results(result_df)
        
        return result_df
    
    def merge_results_with_original(self, original_df: pd.DataFrame, results: List[Dict[str, Any]]) -> pd.DataFrame:
        """将处理结果与原始数据合并"""
        result_df = original_df.copy()
        
        # 添加新列
        result_df['cleaned_text'] = ''
        result_df['original_length'] = 0
        result_df['cleaned_length'] = 0
        result_df['char_removed'] = 0
        result_df['pii_count'] = 0
        result_df['emoji_count'] = 0
        result_df['mentions_removed'] = 0
        result_df['hashtags_removed'] = 0
        result_df['processing_success'] = False
        result_df['processing_error'] = ''
        result_df['detected_language'] = ''
        result_df['warnings'] = ''
        
        # 句子切分相关列
        result_df['sentence_count'] = 0
        result_df['sentences_text'] = ''  # 用分隔符分隔的句子文本
        result_df['sentences_detail'] = ''  # JSON格式的详细句子信息
        
        # 填充结果
        for result in results:
            if not result.get('success', False):
                continue
                
            row_idx = result['row_index']
            if row_idx not in result_df.index:
                continue
                
            api_result = result.get('result', {})
            data = api_result.get('data', {})
            
            result_df.loc[row_idx, 'cleaned_text'] = data.get('cleaned_text', '')
            result_df.loc[row_idx, 'processing_success'] = True
            
            # 统计信息
            stats = data.get('statistics', {})
            result_df.loc[row_idx, 'original_length'] = stats.get('original_length', 0)
            result_df.loc[row_idx, 'cleaned_length'] = stats.get('cleaned_length', 0)
            result_df.loc[row_idx, 'char_removed'] = stats.get('char_removed', 0)
            
            # 移除元素统计
            removed = data.get('removed_elements', {})
            result_df.loc[row_idx, 'pii_count'] = removed.get('pii_count', 0)
            result_df.loc[row_idx, 'emoji_count'] = removed.get('emoji_count', 0)
            result_df.loc[row_idx, 'mentions_removed'] = removed.get('mentions_removed', 0)
            result_df.loc[row_idx, 'hashtags_removed'] = removed.get('hashtags_removed', 0)
            
            # 语言检测
            lang_detect = data.get('language_detection', {})
            if lang_detect:
                result_df.loc[row_idx, 'detected_language'] = lang_detect.get('primary_lang', '')
            
            # 句子切分结果
            sentence_splitting = data.get('sentence_splitting', {})
            if sentence_splitting:
                sentences = sentence_splitting.get('sentences', [])
                result_df.loc[row_idx, 'sentence_count'] = len(sentences)
                
                # 提取句子文本，用 ||| 分隔
                if sentences:
                    sentence_texts = [s.get('text', '') for s in sentences]
                    result_df.loc[row_idx, 'sentences_text'] = '|||'.join(sentence_texts)
                    
                    # 保存详细的句子信息为JSON格式
                    try:
                        import json
                        result_df.loc[row_idx, 'sentences_detail'] = json.dumps(sentences, ensure_ascii=False)
                    except Exception as e:
                        logger.warning(f"句子详情序列化失败: {e}")
                        result_df.loc[row_idx, 'sentences_detail'] = str(sentences)
            
            # 警告信息
            warnings = data.get('warnings', [])
            if warnings:
                result_df.loc[row_idx, 'warnings'] = '; '.join(warnings)
        
        # 填充失败的结果
        for result in results:
            if result.get('success', False):
                continue
                
            row_idx = result['row_index']
            if row_idx not in result_df.index:
                continue
                
            result_df.loc[row_idx, 'processing_success'] = False
            result_df.loc[row_idx, 'processing_error'] = result.get('error', '未知错误')
        
        return result_df
    
    def save_results(self, df: pd.DataFrame):
        """保存处理结果"""
        try:
            df.to_csv(self.process_config.output_csv, index=False, encoding='utf-8-sig')
            logger.info(f"结果已保存到 {self.process_config.output_csv}")
        except Exception as e:
            logger.error(f"保存结果失败: {e}")
            raise
    
    def load_processed_indices(self) -> Set[int]:
        """从jsonl文件加载已处理的行索引"""
        try:
            processed_indices = set()
            jsonl_path = Path(self.process_config.jsonl_file)
            
            if not jsonl_path.exists():
                return processed_indices
            
            with open(jsonl_path, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        result = json.loads(line.strip())
                        if isinstance(result, dict) and 'row_index' in result:
                            processed_indices.add(result['row_index'])
            
            return processed_indices
        except Exception as e:
            logger.error(f"加载处理进度失败: {e}")
            return set()
    
    def save_to_jsonl(self, results: List[Dict[str, Any]]):
        """保存结果到jsonl文件"""
        try:
            with open(self.process_config.jsonl_file, 'a', encoding='utf-8') as f:
                for result in results:
                    if isinstance(result, dict):
                        f.write(json.dumps(result, ensure_ascii=False) + '\n')
        except Exception as e:
            logger.error(f"保存进度失败: {e}")
    
    def load_from_jsonl(self) -> List[Dict[str, Any]]:
        """从jsonl文件加载所有结果"""
        try:
            results = []
            jsonl_path = Path(self.process_config.jsonl_file)
            
            if not jsonl_path.exists():
                return results
            
            with open(jsonl_path, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        result = json.loads(line.strip())
                        if isinstance(result, dict):
                            results.append(result)
            
            return results
        except Exception as e:
            logger.error(f"加载结果失败: {e}")
            return []

async def main():
    """示例使用方法"""
    # API配置
    api_config = PreprocessorConfig(
        base_url="http://localhost:8001",  # 修改为您的预处理服务地址
        max_concurrent=10,  # 并发数
        timeout=30,
        retry_attempts=3,
        # 预处理选项
        remove_pii=True,
        emoji_convert=True,
        remove_social_mentions=True,
        remove_hashtags=True,
        remove_ads=True,
        detect_language=True,
    )
    
    # 处理配置
    process_config = ProcessConfig(
        input_csv="test_data.csv",  # 输入文件
        output_csv="test_data_cleaned.csv",  # 输出文件
        text_column="content",  # 文本列名
        author_column=None,  # 作者列名（如果有）
        max_rows=100,  # 测试时限制行数
    )
    
    # 执行预处理
    async with BatchPreprocessor(api_config, process_config) as processor:
        result_df = await processor.process_batch()
        logger.info(f"处理完成，共处理 {len(result_df)} 行数据")

if __name__ == "__main__":
    asyncio.run(main()) 