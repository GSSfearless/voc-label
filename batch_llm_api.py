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
import hashlib
import os

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 为了调试，可以设置为DEBUG级别
# logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

@dataclass
class APIConfig:
    """API配置类"""
    base_url: str = "https://api.siliconflow.cn/v1/"
    api_key: str = ""
    model: str = "openai/gpt-4o"
    max_concurrent: int = 10
    timeout: int = 30
    retry_attempts: int = 3
    retry_delay: int = 1
    system_prompt: Optional[str] = None  # 系统提示词
    # 缓存配置
    enable_cache: bool = True  # 是否启用缓存
    cache_file: str = "llm_cache.json"  # 缓存文件路径
    cache_ttl: Optional[int] = None  # 缓存过期时间（秒），None表示永不过期

@dataclass
class ProcessConfig:
    """处理配置类"""
    input_csv: str
    output_csv: str
    input_column: str
    prompt_template: str
    output_json_fields: List[str]
    max_rows: Optional[int] = None
    random_sample_size: Optional[int] = None  # 随机抽样数量
    random_seed: Optional[int] = None  # 随机种子，用于可重复的随机抽样
    jsonl_file: Optional[str] = None  # 阶段性保存的jsonl文件路径
    batch_size: int = 50  # 每多少行保存一次jsonl
    filter_column: Optional[str] = None  # 筛选字段名
    filter_values: Optional[List[Any]] = None  # 筛选值列表（包含这些值的行会被处理）
    filter_condition: Optional[str] = "in"  # 筛选条件：'in'包含, 'not_in'不包含, 'equals'等于, 'not_equals'不等于

class LLMBatchProcessor:
    """批量LLM API调用处理器"""
    
    def __init__(self, api_config: APIConfig, process_config: ProcessConfig):
        self.api_config = api_config
        self.process_config = process_config
        self.semaphore = asyncio.Semaphore(api_config.max_concurrent)
        self.session = None
        self._cache = {}  # 内存缓存
        self._cache_loaded = False  # 缓存是否已加载
        
        # 设置默认的jsonl文件路径
        if self.process_config.jsonl_file is None:
            base_name = Path(self.process_config.output_csv).stem
            self.process_config.jsonl_file = f"{base_name}_progress.jsonl"
    
    def _get_cache_key(self, prompt: str) -> str:
        """生成缓存键"""
        # 使用prompt和系统提示词的组合生成哈希
        content = prompt
        if self.api_config.system_prompt:
            content = f"{self.api_config.system_prompt}\n{prompt}"
        
        return hashlib.md5(content.encode('utf-8')).hexdigest()
    
    def _load_cache(self):
        """加载缓存文件"""
        if not self.api_config.enable_cache:
            return
            
        if self._cache_loaded:
            return
            
        cache_path = Path(self.api_config.cache_file)
        if cache_path.exists():
            try:
                with open(cache_path, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                    
                # 检查缓存过期
                current_time = time.time()
                valid_cache = {}
                
                for key, value in cache_data.items():
                    cache_time = value.get('timestamp', 0)
                    
                    # 如果设置了TTL且已过期，则跳过
                    if self.api_config.cache_ttl and (current_time - cache_time) > self.api_config.cache_ttl:
                        continue
                    
                    valid_cache[key] = value
                
                self._cache = valid_cache
                logger.info(f"加载了 {len(self._cache)} 条有效缓存记录")
                
                # 如果有过期记录，重新保存缓存文件
                if len(valid_cache) != len(cache_data):
                    expired_count = len(cache_data) - len(valid_cache)
                    logger.info(f"清理了 {expired_count} 条过期缓存记录")
                    self._save_cache()
                    
            except Exception as e:
                logger.warning(f"加载缓存文件失败: {e}")
                self._cache = {}
        
        self._cache_loaded = True
    
    def _save_cache(self):
        """保存缓存到文件"""
        if not self.api_config.enable_cache:
            return
            
        cache_path = Path(self.api_config.cache_file)
        try:
            # 确保目录存在
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump(self._cache, f, ensure_ascii=False, indent=2)
            
            logger.debug(f"缓存已保存到 {cache_path}")
        except Exception as e:
            logger.error(f"保存缓存文件失败: {e}")
    
    def _get_from_cache(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """从缓存获取结果"""
        if not self.api_config.enable_cache:
            return None
            
        self._load_cache()
        
        if cache_key in self._cache:
            cache_entry = self._cache[cache_key]
            
            # 检查是否过期
            if self.api_config.cache_ttl:
                cache_time = cache_entry.get('timestamp', 0)
                current_time = time.time()
                if (current_time - cache_time) > self.api_config.cache_ttl:
                    # 过期，删除缓存
                    del self._cache[cache_key]
                    return None
            
            logger.debug(f"缓存命中: {cache_key}")
            return cache_entry.get('result')
        
        return None
    
    def _save_to_cache(self, cache_key: str, result: Dict[str, Any]):
        """保存结果到缓存"""
        if not self.api_config.enable_cache:
            return
            
        self._load_cache()
        
        cache_entry = {
            'result': result,
            'timestamp': time.time()
        }
        
        self._cache[cache_key] = cache_entry
        logger.debug(f"结果已缓存: {cache_key}")
        
        # 定期保存缓存（每10条新记录保存一次）
        if len(self._cache) % 10 == 0:
            self._save_cache()
    
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
            
            if self.process_config.input_column not in df.columns:
                raise ValueError(f"列 '{self.process_config.input_column}' 不存在于CSV文件中")
            
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
        logger.info(f"筛选结果: {filtered_count}/{total_count} 行数据将进入模型处理")
        
        return filtered_df, df.copy()
    
    def create_prompt(self, input_text: str) -> str:
        """根据模板创建prompt"""
        return self.process_config.prompt_template.format(input_text=input_text)
    
    async def call_api(self, prompt: str, row_index: int) -> Dict[str, Any]:
        """调用API并返回结果，支持缓存"""
        # 检查缓存
        cache_key = self._get_cache_key(prompt)
        cached_result = self._get_from_cache(cache_key)
        
        if cached_result is not None:
            logger.info(f"行 {row_index} 使用缓存结果")
            return {
                "row_index": row_index,
                "success": True,
                "content": cached_result,
                "error": None,
                "from_cache": True
            }
        
        # 缓存未命中，调用API
        async with self.semaphore:
            for attempt in range(self.api_config.retry_attempts):
                try:
                    headers = {
                        "Authorization": f"Bearer {self.api_config.api_key}",
                        "Content-Type": "application/json",
                        "HTTP-Referer": "https://github.com/your-repo",
                        "X-Title": "LLM Batch Processor"
                    }
                    
                    # 构建消息列表
                    messages = []
                    
                    # 如果配置了系统提示词，则添加system消息
                    if self.api_config.system_prompt:
                        messages.append({
                            "role": "system",
                            "content": self.api_config.system_prompt
                        })
                    
                    # 添加用户消息
                    messages.append({
                        "role": "user",
                        "content": prompt
                    })
                    
                    payload = {
                        "model": self.api_config.model,
                        "messages": messages,
                        "temperature": 0.1,  # 降低温度以获得更稳定的输出
                        "max_tokens": 4000
                    }
                    
                    async with self.session.post(
                        f"{self.api_config.base_url}/chat/completions",
                        headers=headers,
                        json=payload
                    ) as response:
                        if response.status == 200:
                            result = await response.json()
                            content = result["choices"][0]["message"]["content"]
                            logger.info(f"行 {row_index} 处理成功（API调用）")
                            logger.debug(f"行 {row_index} 返回内容: {content}")
                            
                            # 保存到缓存
                            self._save_to_cache(cache_key, content)
                            
                            return {
                                "row_index": row_index,
                                "success": True,
                                "content": content,
                                "error": None,
                                "from_cache": False
                            }
                        else:
                            error_text = await response.text()
                            logger.warning(f"行 {row_index} API调用失败 (状态码: {response.status}): {error_text}")
                            if attempt < self.api_config.retry_attempts - 1:
                                await asyncio.sleep(self.api_config.retry_delay * (attempt + 1))
                            else:
                                return {
                                    "row_index": row_index,
                                    "success": False,
                                    "content": None,
                                    "error": f"HTTP {response.status}: {error_text}",
                                    "from_cache": False
                                }
                                
                except Exception as e:
                    logger.warning(f"行 {row_index} 请求异常 (尝试 {attempt + 1}): {e}")
                    if attempt < self.api_config.retry_attempts - 1:
                        await asyncio.sleep(self.api_config.retry_delay * (attempt + 1))
                    else:
                        return {
                            "row_index": row_index,
                            "success": False,
                            "content": None,
                            "error": str(e),
                            "from_cache": False
                        }
    
    def parse_json_response(self, content: str) -> Dict[str, Any]:
        """解析模型返回的JSON内容"""
        try:
            # 清理内容，去除多余的空白字符
            content = content.strip()
            
            # 记录原始内容用于调试
            logger.debug(f"原始返回内容: {content[:200]}...")
            
            # 尝试直接解析JSON
            if content.startswith('{') and content.endswith('}'):
                return json.loads(content)
            
            if content.startswith('[') and content.endswith(']'):
                return json.loads(content)
            
            # 尝试提取代码块中的JSON
            if '```json' in content:
                start = content.find('```json') + 7
                end = content.find('```', start)
                if end != -1:
                    json_str = content[start:end].strip()
                    return json.loads(json_str)
            
            # 尝试提取```代码块中的内容（没有json标识）
            if content.count('```') >= 2:
                start = content.find('```') + 3
                end = content.find('```', start)
                if end != -1:
                    json_str = content[start:end].strip()
                    # 去掉可能的语言标识
                    if json_str.startswith('json\n'):
                        json_str = json_str[5:]
                    try:
                        return json.loads(json_str)
                    except json.JSONDecodeError:
                        pass
            
            # 尝试提取第一个完整的JSON对象或数组
            # 查找第一个 { 或 [ 
            json_start = -1
            for i, char in enumerate(content):
                if char in '{[':
                    json_start = i
                    break
            
            if json_start != -1:
                # 找到对应的结束位置
                bracket_count = 0
                start_char = content[json_start]
                end_char = '}' if start_char == '{' else ']'
                
                for i in range(json_start, len(content)):
                    char = content[i]
                    if char == start_char:
                        bracket_count += 1
                    elif char == end_char:
                        bracket_count -= 1
                        if bracket_count == 0:
                            json_str = content[json_start:i+1]
                            try:
                                return json.loads(json_str)
                            except json.JSONDecodeError:
                                break
            
            # 尝试逐行查找JSON
            lines = content.split('\n')
            json_lines = []
            in_json = False
            bracket_count = 0
            
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                    
                # 检查是否开始JSON
                if not in_json and (line.startswith('{') or line.startswith('[')):
                    in_json = True
                    json_lines = [line]
                    bracket_count = line.count('{') + line.count('[') - line.count('}') - line.count(']')
                elif in_json:
                    json_lines.append(line)
                    bracket_count += line.count('{') + line.count('[') - line.count('}') - line.count(']')
                    
                    if bracket_count <= 0:
                        # JSON结束
                        json_str = '\n'.join(json_lines)
                        try:
                            return json.loads(json_str)
                        except json.JSONDecodeError:
                            pass
                        in_json = False
                        json_lines = []
                        bracket_count = 0
            
            # 如果所有方法都失败，尝试修复常见的JSON格式问题
            fixed_content = content
            
            # 修复常见问题
            fixed_content = fixed_content.replace('```json', '').replace('```', '')
            fixed_content = fixed_content.replace('\n', ' ')
            fixed_content = ' '.join(fixed_content.split())  # 标准化空白字符
            
            # 再次尝试解析
            if fixed_content.startswith('{') or fixed_content.startswith('['):
                try:
                    return json.loads(fixed_content)
                except json.JSONDecodeError:
                    pass
            
            # 如果都无法解析，返回原始内容
            logger.warning(f"无法解析JSON，原始内容: {content[:500]}...")
            return {"raw_content": content}
            
        except json.JSONDecodeError as e:
            logger.warning(f"JSON解析失败: {e}, 内容前500字符: {content[:500]}")
            return {"raw_content": content, "parse_error": str(e)}
        except Exception as e:
            logger.error(f"解析过程发生异常: {e}")
            return {"raw_content": content, "parse_error": str(e)}
    
    async def process_batch(self) -> pd.DataFrame:
        """批量处理数据，支持断点续传和阶段性保存"""
        # 加载数据
        df = self.load_data()
        
        # 应用筛选条件，获取需要处理的数据和完整数据
        filtered_df, full_df = self.apply_filter(df)
        
        # 检查是否有已处理的数据
        processed_indices = self.load_processed_indices()
        
        # 从筛选后的数据中排除已处理的行
        if processed_indices:
            remaining_df = filtered_df[~filtered_df.index.isin(processed_indices)].copy()
            logger.info(f"发现 {len(processed_indices)} 行已处理，剩余 {len(remaining_df)} 行待处理")
        else:
            remaining_df = filtered_df.copy()
            logger.info(f"开始全新处理，共 {len(remaining_df)} 行")
        
        if len(remaining_df) == 0:
            logger.info("所有符合筛选条件的数据已处理完成")
            # 从jsonl文件加载所有结果并返回
            all_results = self.load_from_jsonl()
            if all_results:
                result_df = pd.DataFrame(all_results)
                result_df = result_df.sort_values("row_index")
                # 合并完整原始数据（不是筛选后的数据）
                df_with_results = full_df.reset_index().rename(columns={"index": "row_index"})
                final_df = pd.merge(df_with_results, result_df, on="row_index", how="left")
                return final_df
            else:
                return full_df
        
        # 分批处理
        total_processed = 0
        batch_results = []
        api_calls_count = 0  # 统计实际API调用次数
        cache_hits_count = 0  # 统计缓存命中次数
        
        for start_idx in range(0, len(remaining_df), self.process_config.batch_size):
            end_idx = min(start_idx + self.process_config.batch_size, len(remaining_df))
            batch_df = remaining_df.iloc[start_idx:end_idx]
            
            logger.info(f"处理批次 {start_idx//self.process_config.batch_size + 1}: 第 {start_idx+1}-{end_idx} 行")
            
            # 创建当前批次的任务
            tasks = []
            for index, row in batch_df.iterrows():
                input_text = str(row[self.process_config.input_column])
                prompt = self.create_prompt(input_text)
                task = self.call_api(prompt, index)
                tasks.append(task)
            
            # 执行当前批次
            start_time = time.time()
            batch_api_results = await asyncio.gather(*tasks, return_exceptions=True)
            end_time = time.time()
            
            # 统计缓存命中情况
            batch_api_calls = sum(1 for r in batch_api_results if isinstance(r, dict) and not r.get('from_cache', False) and r.get('success', False))
            batch_cache_hits = sum(1 for r in batch_api_results if isinstance(r, dict) and r.get('from_cache', False))
            api_calls_count += batch_api_calls
            cache_hits_count += batch_cache_hits
            
            logger.info(f"批次处理完成，耗时: {end_time - start_time:.2f} 秒，API调用: {batch_api_calls}, 缓存命中: {batch_cache_hits}")
            
            # 处理当前批次的结果
            current_batch_results = []
            for result in batch_api_results:
                if isinstance(result, Exception):
                    logger.error(f"任务执行异常: {result}")
                    continue

                processed_result = self.process_single_result(result)
                if processed_result:
                    current_batch_results.extend(processed_result)
            
            # 保存当前批次到jsonl
            if current_batch_results:
                self.save_to_jsonl(current_batch_results)
                batch_results.extend(current_batch_results)
            
            total_processed += len(batch_df)
            logger.info(f"已处理 {total_processed}/{len(remaining_df)} 行")
        
        # 保存最终缓存
        if self.api_config.enable_cache:
            self._save_cache()
            logger.info(f"缓存统计: API调用 {api_calls_count} 次，缓存命中 {cache_hits_count} 次，节省 {cache_hits_count/(api_calls_count + cache_hits_count)*100:.1f}% 的API调用")
        
        # 最终合并所有结果
        logger.info("开始生成最终结果...")
        all_results = self.load_from_jsonl()
        
        if all_results:
            result_df = pd.DataFrame(all_results)
            result_df = result_df.sort_values("row_index")
            
            # 合并完整原始数据（包含未处理的行）
            df_with_results = full_df.reset_index().rename(columns={"index": "row_index"})
            final_df = pd.merge(df_with_results, result_df, on="row_index", how="left")
            
            return final_df
        else:
            return full_df
    
    def process_single_result(self, result: Dict[str, Any]) -> List[Dict[str, Any]]:
        """处理单个API调用结果"""
        processed_results = []
        
        if result["success"]:
            parsed_json = self.parse_json_response(result["content"])
            row_data = {"row_index": result["row_index"]}
            
            # 检查解析是否成功
            if "parse_error" in parsed_json or "raw_content" in parsed_json:
                # 解析失败的情况
                for field in self.process_config.output_json_fields:
                    row_data[field] = None
                row_data["raw_response"] = result["content"]
                row_data["parsing_success"] = False
                row_data["error"] = parsed_json.get("parse_error", "JSON格式解析失败")
                processed_results.append(row_data)
                return processed_results
            
            if isinstance(parsed_json, list):
                # 处理数组格式的返回
                for item in parsed_json:
                    item_data = {"row_index": result["row_index"]}
                    for field in self.process_config.output_json_fields:
                        item_data[field] = item.get(field, None)
                    item_data["raw_response"] = result["content"]
                    item_data["parsing_success"] = True
                    processed_results.append(item_data)
            elif isinstance(parsed_json, dict):
                # 处理对象格式的返回
                for field in self.process_config.output_json_fields:
                    row_data[field] = parsed_json.get(field, None)
                
                row_data["raw_response"] = result["content"]
                row_data["parsing_success"] = True
                processed_results.append(row_data)
            else:
                # 格式不正确
                for field in self.process_config.output_json_fields:
                    row_data[field] = None
                row_data["raw_response"] = result["content"]
                row_data["parsing_success"] = False
                row_data["error"] = "返回格式不是有效的JSON对象或数组"
                processed_results.append(row_data)

        else:
            # 处理失败的情况
            row_data = {"row_index": result["row_index"]}
            for field in self.process_config.output_json_fields:
                row_data[field] = None
            row_data["raw_response"] = None
            row_data["parsing_success"] = False
            row_data["error"] = result["error"]
            processed_results.append(row_data)
        
        return processed_results
    
    def save_results(self, df: pd.DataFrame):
        """保存结果到CSV"""
        try:
            df.to_csv(self.process_config.output_csv, index=False, encoding='utf-8')
            logger.info(f"结果已保存到: {self.process_config.output_csv}")
        except Exception as e:
            logger.error(f"保存结果失败: {e}")
            raise

    def load_processed_indices(self) -> Set[int]:
        """加载已处理的行索引"""
        processed_indices = set()
        jsonl_path = Path(self.process_config.jsonl_file)
        
        if jsonl_path.exists():
            try:
                with open(jsonl_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        if line.strip():
                            data = json.loads(line)
                            processed_indices.add(data.get('row_index', -1))
                logger.info(f"从 {jsonl_path} 加载了 {len(processed_indices)} 条已处理记录")
            except Exception as e:
                logger.warning(f"读取jsonl文件失败: {e}")
        
        return processed_indices
    
    def save_to_jsonl(self, results: List[Dict[str, Any]]):
        """保存结果到jsonl文件"""
        if not results:
            return
            
        jsonl_path = Path(self.process_config.jsonl_file)
        try:
            with open(jsonl_path, 'a', encoding='utf-8') as f:
                for result in results:
                    f.write(json.dumps(result, ensure_ascii=False) + '\n')
            logger.info(f"保存 {len(results)} 条记录到 {jsonl_path}")
        except Exception as e:
            logger.error(f"保存jsonl文件失败: {e}")
    
    def load_from_jsonl(self) -> List[Dict[str, Any]]:
        """从jsonl文件加载所有结果"""
        results = []
        jsonl_path = Path(self.process_config.jsonl_file)
        
        if jsonl_path.exists():
            try:
                with open(jsonl_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        if line.strip():
                            results.append(json.loads(line))
                logger.info(f"从 {jsonl_path} 加载了 {len(results)} 条记录")
            except Exception as e:
                logger.error(f"读取jsonl文件失败: {e}")
        
        return results

async def main():
    """主函数示例"""
    # API配置
    api_config = APIConfig(
        api_key="your-openrouter-api-key-here",  # 请替换为您的API密钥
        model="openai/gpt-4o",
        max_concurrent=5,  # 根据您的API限制调整
        timeout=30,
        retry_attempts=3,
        system_prompt="""你是一个专业的文本分析师，专门负责情感分析任务。
请严格按照JSON格式返回分析结果，确保输出格式的一致性和准确性。
你的分析应该客观、准确，并提供有用的关键词和总结。""",
        # 缓存配置
        enable_cache=True,
        cache_file="llm_cache.json",
        cache_ttl=None  # 永不过期
    )
    
    # 处理配置
    process_config = ProcessConfig(
        input_csv="input_data.csv",  # 输入CSV文件路径
        output_csv="output_results.csv",  # 输出CSV文件路径
        input_column="text",  # 要处理的列名
        prompt_template="""请分析以下文本的情感倾向：

文本: {input_text}

请返回以下格式的JSON：
{{
    "sentiment": "positive/negative/neutral",
    "confidence": 0.95,
    "keywords": ["关键词1", "关键词2"],
    "summary": "简短摘要"
}}""",
        output_json_fields=["sentiment", "confidence", "keywords", "summary"],  # 要提取的JSON字段
        max_rows=None,  # 限制处理的行数，None表示处理全部
        random_sample_size=None,  # 随机抽样数量
        random_seed=None,  # 随机种子，用于可重复的随机抽样
        jsonl_file=None,  # 阶段性保存的jsonl文件路径
        batch_size=50,  # 每多少行保存一次jsonl
        filter_column=None,  # 筛选字段名
        filter_values=None,  # 筛选值列表（包含这些值的行会被处理）
        filter_condition="in"  # 筛选条件：'in'包含, 'not_in'不包含, 'equals'等于, 'not_equals'不等于
    )
    
    # 执行处理
    async with LLMBatchProcessor(api_config, process_config) as processor:
        result_df = await processor.process_batch()
        processor.save_results(result_df)
        
        # 显示统计信息
        total_rows = len(result_df)
        success_rows = len(result_df[result_df['parsing_success'] == True])
        logger.info(f"处理统计: 总计 {total_rows} 行，成功 {success_rows} 行，成功率: {success_rows/total_rows*100:.1f}%")

if __name__ == "__main__":
    asyncio.run(main()) 