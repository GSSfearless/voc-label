"""
数据清洗模块
负责对原始数据进行清洗、过滤和标准化
"""

import pandas as pd
import numpy as np
import re
import logging
from typing import Dict, List, Optional, Union, Callable

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def clean_text(text: str) -> str:
    """
    清洗文本内容
    
    Args:
        text: 原始文本
        
    Returns:
        清洗后的文本
    """
    if pd.isna(text) or not isinstance(text, str):
        return ""
    
    # 删除URL
    text = re.sub(r'https?://\S+|www\.\S+', '', text)
    
    # 删除HTML标签
    text = re.sub(r'<.*?>', '', text)
    
    # 删除表情符号和特殊符号
    text = re.sub(r'\[.*?\]', '', text)  # 删除[表情]
    text = re.sub(r'#.*?#', '', text)    # 删除微博话题标签
    
    # 替换特殊字符和多余空格
    text = re.sub(r'[^\w\s\u4e00-\u9fff，。！？：；""''（）【】《》、]+', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    
    # 移除表情符号 (注释掉这行以保留中文字符)
    # text = text.encode('ascii', 'ignore').decode('ascii')
    
    # 修正常见错误
    text = re.sub(r'(https?)', '', text)
    
    return text.strip()

def filter_relevant_content(df: pd.DataFrame, keywords: List[str]) -> pd.DataFrame:
    """
    过滤与关键词相关的内容
    
    Args:
        df: 原始数据DataFrame
        keywords: 关键词列表
        
    Returns:
        过滤后的DataFrame
    """
    if 'text_content' not in df.columns:
        logger.error("数据中缺少text_content列")
        return df
    
    # 确保text_content列为字符串类型
    df['text_content'] = df['text_content'].astype(str)
    
    # 构建关键词正则表达式
    pattern = '|'.join(keywords)
    
    # 过滤包含关键词的行
    mask = df['text_content'].str.contains(pattern, case=False, na=False)
    filtered_df = df[mask].copy()
    
    logger.info(f"过滤前行数: {len(df)}, 过滤后行数: {len(filtered_df)}")
    return filtered_df

def filter_meaningful_content(df: pd.DataFrame, min_length: int = 5, min_chinese_chars: int = 2) -> pd.DataFrame:
    """
    过滤有意义的内容，去除无意义的短评论
    
    Args:
        df: 原始数据DataFrame
        min_length: 最小文本长度
        min_chinese_chars: 最小中文字符数
        
    Returns:
        过滤后的DataFrame
    """
    if 'text_content' not in df.columns:
        logger.error("数据中缺少text_content列")
        return df
    
    # 确保text_content列为字符串类型
    df['text_content'] = df['text_content'].astype(str)
    
    # 计算中文字符数
    def count_chinese_chars(text):
        if not isinstance(text, str):
            return 0
        return len(re.findall(r'[\u4e00-\u9fff]', text))
    
    # 应用过滤条件
    df['text_length'] = df['text_content'].str.len()
    df['chinese_chars'] = df['text_content'].apply(count_chinese_chars)
    
    # 过滤掉过短或没有足够中文字符的评论
    filtered_df = df[(df['text_length'] >= min_length) & (df['chinese_chars'] >= min_chinese_chars)].copy()
    
    # 过滤掉通用无意义评论
    noise_patterns = [
        r'^[好赞棒真不错嗯哦是]+$',  # 单纯的好、赞、棒等
        r'^[？。，！]+$',           # 只有标点符号
        r'^[0-9一二三四五六七八九十百千万亿]+$',  # 只有数字
        r'^(666+|垃圾|呵呵|哈哈+|厉害|可以|nice|good|ok|😊|。。。)$',  # 常见无意义评论
        r'^([.。]{2,}|[?？]{2,}|[!！]{2,})$',  # 重复标点
        r'快来抢购',   # 促销类垃圾评论
        r'纯支持',     # 纯支持类无实际内容
        r'纯元气',     # 无意义互动类
        r'^下单',      # 仅表示下单
        r'^已购',      # 仅表示已购买
        r'^前来',      # 前来打卡类
        r'^(安装师傅|师傅|物流)',   # 仅提及安装师傅或物流
        r'^(收到|到货)',  # 仅表示收到货
        r'^[\s\t\r\n]*$'  # 空白评论
    ]
    
    for pattern in noise_patterns:
        filtered_df = filtered_df[~filtered_df['text_content'].str.contains(pattern, regex=True, na=False)]
    
    # 移除辅助列
    filtered_df.drop(columns=['text_length', 'chinese_chars'], inplace=True)
    
    logger.info(f"过滤前行数: {len(df)}, 过滤掉无意义内容后行数: {len(filtered_df)}")
    return filtered_df

def remove_duplicates(df: pd.DataFrame, subset: Optional[List[str]] = None) -> pd.DataFrame:
    """
    移除重复数据
    
    Args:
        df: 原始数据DataFrame
        subset: 用于判断重复的列，默认为['text_content']
        
    Returns:
        去重后的DataFrame
    """
    if subset is None:
        subset = ['text_content']
    
    # 检查列是否存在
    for col in subset:
        if col not in df.columns:
            logger.warning(f"列 {col} 不存在，将从去重子集中移除")
            subset.remove(col)
    
    if not subset:
        logger.error("没有有效的列用于去重")
        return df
    
    # 去重
    before_count = len(df)
    df_dedup = df.drop_duplicates(subset=subset, keep='first')
    after_count = len(df_dedup)
    
    logger.info(f"移除了 {before_count - after_count} 条重复数据")
    return df_dedup

def filter_by_date_range(df: pd.DataFrame, 
                        start_date: Optional[str] = None, 
                        end_date: Optional[str] = None, 
                        date_col: str = 'timestamp') -> pd.DataFrame:
    """
    按日期范围过滤数据
    
    Args:
        df: 数据DataFrame
        start_date: 起始日期，格式'YYYY-MM-DD'
        end_date: 结束日期，格式'YYYY-MM-DD'
        date_col: 日期列名，默认为'timestamp'
        
    Returns:
        过滤后的DataFrame
    """
    if date_col not in df.columns:
        logger.error(f"列 {date_col} 不存在")
        return df
    
    # 尝试转换日期列
    try:
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    except Exception as e:
        logger.error(f"转换日期列失败: {e}")
        return df
    
    # 应用日期过滤
    if start_date is not None:
        try:
            start = pd.to_datetime(start_date)
            df = df[df[date_col] >= start]
            logger.info(f"过滤出 {start_date} 之后的数据")
        except Exception as e:
            logger.error(f"转换起始日期失败: {e}")
    
    if end_date is not None:
        try:
            end = pd.to_datetime(end_date)
            df = df[df[date_col] <= end]
            logger.info(f"过滤出 {end_date} 之前的数据")
        except Exception as e:
            logger.error(f"转换结束日期失败: {e}")
    
    return df

def standardize_product_models(df: pd.DataFrame, model_col: str = 'product_model') -> pd.DataFrame:
    """
    标准化产品型号名称
    
    Args:
        df: 数据DataFrame
        model_col: 产品型号列名
        
    Returns:
        标准化后的DataFrame
    """
    if model_col not in df.columns:
        logger.warning(f"列 {model_col} 不存在，无法标准化产品型号")
        return df
    
    # 确保产品型号列为字符串类型
    df[model_col] = df[model_col].astype(str)
    
    # 定义型号替换规则 (正则表达式 -> 标准型号)
    model_mapping = {
        r'(?i)a2z': 'A2Z',
        r'(?i)a1z': 'A1Z',
        r'(?i)c25': 'C25',
        r'(?i)c40': 'C40',
        r'(?i)c65': 'C65',
        r'(?i)c80': 'C80',
        r'(?i)m95c': 'M95C',
        r'(?i)m65': 'M65',
        r'(?i)n[0-9]+c': lambda x: x.upper(),  # 将NxxC格式转为大写
        r'(?i)q[0-9]+': lambda x: x.upper(),   # 将Qxx格式转为大写
    }
    
    # 应用替换规则
    original_models = df[model_col].copy()
    
    for pattern, replacement in model_mapping.items():
        if callable(replacement):
            # 如果replacement是函数，用正则表达式提取匹配的子串并应用函数
            mask = df[model_col].str.contains(pattern, na=False)
            matches = df.loc[mask, model_col].str.extract(f'({pattern})', expand=False)
            df.loc[mask, model_col] = matches.apply(replacement)
        else:
            # 否则直接替换
            df[model_col] = df[model_col].str.replace(pattern, replacement, regex=True)
    
    # 统计替换情况
    changed_count = (df[model_col] != original_models).sum()
    logger.info(f"已标准化 {changed_count} 个产品型号")
    
    return df

def validate_ecommerce_comments(df: pd.DataFrame, min_length: int = 5) -> pd.DataFrame:
    """
    验证电商评论数据，添加is_valid标志字段，用于标记是否需要大模型进行下一步分析
    
    Args:
        df: 电商评论数据DataFrame
        min_length: 评论内容最小长度，默认为5
        
    Returns:
        添加is_valid字段的DataFrame
    """
    if '评论内容' not in df.columns:
        logger.error("数据中缺少评论内容列")
        return df
    
    # 复制DataFrame避免修改原始数据
    df_validated = df.copy()
    
    # 初始化is_valid字段为True
    df_validated['is_valid'] = True
    
    # 定义无效评论的条件
    # 1. 空评论或默认文本
    empty_patterns = [
        r'此用户',  # 匹配所有包含"此用户"的评论
        r'^$',  # 空字符串
        r'^\s+$'  # 只包含空白
    ]
    
    for pattern in empty_patterns:
        mask = df_validated['评论内容'].str.contains(pattern, regex=True, na=True)
        df_validated.loc[mask, 'is_valid'] = False
    
    # 2. 过短的评论
    df_validated.loc[df_validated['评论内容'].str.len() < min_length, 'is_valid'] = False
    
    # 3. 无实质性内容的评论
    generic_patterns = [
        r'^[好赞棒真不错嗯哦是]+$',  # 单纯的好、赞、棒等
        r'^[？。，！]+$',           # 只有标点符号
        r'^[0-9一二三四五六七八九十百千万亿]+$',  # 只有数字
        r'^(666+|垃圾|呵呵|哈哈+|厉害|可以|nice|good|ok|😊|。。。)$',  # 常见无意义评论
        r'^([.。]{2,}|[?？]{2,}|[!！]{2,})$',  # 重复标点
    ]
    
    for pattern in generic_patterns:
        mask = df_validated['评论内容'].str.contains(pattern, regex=True, na=False)
        df_validated.loc[mask, 'is_valid'] = False
    
    # 对于NaN值标记为无效
    df_validated.loc[df_validated['评论内容'].isna(), 'is_valid'] = False
    
    # 统计有效和无效评论数量
    valid_count = df_validated['is_valid'].sum()
    total_count = len(df_validated)
    invalid_count = total_count - valid_count
    
    logger.info(f"总评论数: {total_count}, 有效评论数: {valid_count}, 无效评论数: {invalid_count}")
    logger.info(f"有效评论比例: {valid_count/total_count:.2%}")
    
    return df_validated

def clean_dataframe(df: pd.DataFrame, 
                   keywords: Optional[List[str]] = None, 
                   start_date: Optional[str] = None, 
                   end_date: Optional[str] = None) -> pd.DataFrame:
    """
    对数据框进行全面清洗
    
    Args:
        df: 原始数据DataFrame
        keywords: 关键词列表，用于过滤相关内容
        start_date: 起始日期，格式'YYYY-MM-DD'
        end_date: 结束日期，格式'YYYY-MM-DD'
        
    Returns:
        清洗后的DataFrame
    """
    logger.info(f"开始清洗数据，原始数据行数: {len(df)}")
    
    # 检查必要列是否存在
    required_cols = ['text_content']
    for col in required_cols:
        if col not in df.columns:
            logger.error(f"数据中缺少必要的列: {col}")
            return df
    
    # 1. 清洗文本内容
    logger.info("清洗文本内容...")
    df['text_content'] = df['text_content'].apply(clean_text)
    
    # 2. 删除空文本
    df = df[df['text_content'].str.len() > 0].reset_index(drop=True)
    logger.info(f"删除空文本后的行数: {len(df)}")
    
    # 3. 过滤有意义的内容
    logger.info("过滤无意义内容...")
    df = filter_meaningful_content(df, min_length=5, min_chinese_chars=2)
    
    # 4. 关键词过滤（如果提供）
    if keywords:
        logger.info(f"使用关键词过滤: {keywords}")
        df = filter_relevant_content(df, keywords)
    
    # 5. 日期范围过滤（如果提供）
    if start_date or end_date:
        logger.info(f"使用日期范围过滤: {start_date} 到 {end_date}")
        df = filter_by_date_range(df, start_date, end_date)
    
    # 6. 标准化产品型号
    if 'product_model' in df.columns:
        logger.info("标准化产品型号...")
        df = standardize_product_models(df)
    
    # 7. 去重
    df = remove_duplicates(df)
    
    logger.info(f"数据清洗完成，最终行数: {len(df)}")
    return df.reset_index(drop=True)

def clean_social_media_data(
    df: pd.DataFrame,
    text_col: str = 'text_content',
    author_col: Optional[str] = 'author',
    author_blacklist: Optional[List[str]] = None,
    meaningless_patterns: Optional[List[str]] = None,
    min_text_length: int = 3,
    clean_weibo_tags: bool = False
) -> pd.DataFrame:
    """
    对来自社交媒体等公域渠道的数据进行通用清洗。
    此版本不会删除行，而是添加 'is_valid' 标记列。

    Args:
        df: 包含待清洗数据的 Pandas DataFrame。
        text_col: 包含主要文本内容的列名，默认为 'text_content'。
        author_col: 包含作者信息的列名，默认为 'author'。如果为 None 或列不存在，则跳过作者过滤。
        author_blacklist: 作者关键词黑名单列表。如果作者列内容包含任一关键词，该行将被标记为无效 (is_valid=False)。
        meaningless_patterns: 用于匹配无意义内容的正则表达式列表。如果文本内容匹配任一模式，该行将被标记为无效 (is_valid=False)。
        min_text_length: 清洗后文本内容的最小长度，低于此长度的行将被标记为无效 (is_valid=False)。
        clean_weibo_tags: 是否清洗微博标签（#标签内容#）。

    Returns:
        包含原始数据以及 'is_valid', 'invalidation_reason', 'cleaned_text' 列的 Pandas DataFrame。
    """
    logger.info(f"开始通用社交媒体数据清洗（标记模式），初始行数: {len(df)}")
    original_count = len(df)
    cleaned_df = df.copy()

    # --- 初始化新列 ---
    cleaned_df['is_valid'] = True  # 默认为有效
    cleaned_df['invalidation_reason'] = '' # 重命名 removal_reason
    # 初始化 cleaned_text，确保处理 NaN 和非字符串类型
    cleaned_df['cleaned_text'] = cleaned_df[text_col].fillna('').astype(str)


    # 0. 检查文本列是否存在
    if text_col not in cleaned_df.columns:
        logger.error(f"错误：指定的文本列 '{text_col}' 不在 DataFrame 中。清洗中止。")
        # 即使中止，也返回带有初始化列的 df
        cleaned_df['is_valid'] = False # 标记所有行为无效，因为无法处理
        cleaned_df['invalidation_reason'] = 'missing_text_column'
        return cleaned_df


    # 1. 通过作者关键字筛选
    if author_col and author_col in cleaned_df.columns and author_blacklist:
        # 确保作者列是字符串
        cleaned_df[author_col] = cleaned_df[author_col].fillna('').astype(str)
        pattern = '|'.join([re.escape(keyword) for keyword in author_blacklist])
        mask = cleaned_df[author_col].str.contains(pattern, case=False, na=False)

        # 标记为无效
        cleaned_df.loc[mask & cleaned_df['is_valid'], 'invalidation_reason'] = 'author_blacklist' # 只记录首次失效原因
        cleaned_df.loc[mask, 'is_valid'] = False
        marked_count = mask.sum()
        if marked_count > 0:
            logger.info(f"步骤 1/6: 根据作者黑名单标记了 {marked_count} 行为无效。")
    elif author_col and author_col not in cleaned_df.columns:
         logger.warning(f"警告：指定的作者列 '{author_col}' 不在 DataFrame 中。跳过作者过滤。")
    elif not author_col:
        logger.info("步骤 1/6: 未指定作者列，跳过作者过滤。")
    elif not author_blacklist:
         logger.info("步骤 1/6: 未提供作者黑名单，跳过作者过滤。")


    # 2. 清洗微博转发 (修改 cleaned_text 列)
    cleaned_df['cleaned_text'] = cleaned_df['cleaned_text'].apply(lambda x: x.split('//')[0].strip() if '//' in x else x)
    # 检查是否因截断导致 cleaned_text 为空，并标记为无效
    empty_after_split_mask = cleaned_df['cleaned_text'].str.strip() == ''
    cleaned_df.loc[empty_after_split_mask & cleaned_df['is_valid'], 'invalidation_reason'] = 'empty_after_forward_clean'
    cleaned_df.loc[empty_after_split_mask, 'is_valid'] = False
    marked_count = (empty_after_split_mask & ~mask).sum() if 'mask' in locals() else empty_after_split_mask.sum() # 计算本次新增的无效标记
    if marked_count > 0:
         logger.info(f"步骤 2/6: 清理微博转发后，标记了 {marked_count} 行因内容为空而无效。")
    else:
        logger.info("步骤 2/6: 完成微博转发清理（或无需清理）。")

    # 3. 清洗微博标签 (如果启用)
    if clean_weibo_tags:
        # 改进的处理方式，处理以#开头到空格结束的标签格式
        def clean_all_weibo_tags(text):
            if not isinstance(text, str):
                return ""
            # 匹配所有以#开头、到空格或字符串结尾的内容
            cleaned_text = re.sub(r'#\S+\s*', '', text).strip()
            # 处理多余的空格
            cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
            return cleaned_text
            
        cleaned_df['cleaned_text'] = cleaned_df['cleaned_text'].apply(clean_all_weibo_tags)
        
        # 检查是否因清洗标签导致内容为空
        empty_after_tags_mask = cleaned_df['cleaned_text'].str.strip() == ''
        cleaned_df.loc[empty_after_tags_mask & cleaned_df['is_valid'], 'invalidation_reason'] = 'empty_after_tags_clean'
        cleaned_df.loc[empty_after_tags_mask, 'is_valid'] = False
        # 更新之前的掩码以计算新标记的条目
        previous_mask = mask if 'mask' in locals() else pd.Series(False, index=cleaned_df.index)
        previous_mask = previous_mask | empty_after_split_mask
        marked_count = (empty_after_tags_mask & ~previous_mask).sum()
        if marked_count > 0:
            logger.info(f"步骤 3/6: 清理微博标签后，标记了 {marked_count} 行因内容为空而无效。")
        else:
            logger.info("步骤 3/6: 完成微博标签清理（或无需清理）。")
    else:
        logger.info("步骤 3/6: 跳过微博标签清理（未启用）。")
        # 为了保持一致性，创建一个空的掩码
        empty_after_tags_mask = pd.Series(False, index=cleaned_df.index)


    # 4. 通用文本清洗 (URL, HTML, 多余空格等) - 应用到 cleaned_text
    try:
        cleaned_df['cleaned_text'] = cleaned_df['cleaned_text'].apply(clean_text)
        # 检查是否因为清洗变为空，并标记为无效
        empty_after_clean_mask = cleaned_df['cleaned_text'].str.strip() == ''
        cleaned_df.loc[empty_after_clean_mask & cleaned_df['is_valid'], 'invalidation_reason'] = 'empty_after_basic_clean'
        cleaned_df.loc[empty_after_clean_mask, 'is_valid'] = False
        # 更新之前的掩码
        previous_mask = previous_mask if 'previous_mask' in locals() else pd.Series(False, index=cleaned_df.index)
        previous_mask = previous_mask | empty_after_tags_mask
        marked_count = (empty_after_clean_mask & ~previous_mask).sum() # 计算本次新增的无效标记
        if marked_count > 0:
            logger.info(f"步骤 4/6: 应用基础文本清洗后，标记了 {marked_count} 行因内容为空而无效。")
        else:
             logger.info("步骤 4/6: 完成基础文本清洗。")
    except NameError:
         logger.error("错误：clean_text 函数未定义。跳过基础文本清洗。")


    # 5. 正则关键字清洗无意义内容 (基于 cleaned_text)
    if meaningless_patterns:
        combined_pattern = '|'.join(meaningless_patterns)
        try:
            meaningless_mask = cleaned_df['cleaned_text'].str.contains(combined_pattern, case=False, regex=True, na=False)
            # 标记为无效
            cleaned_df.loc[meaningless_mask & cleaned_df['is_valid'], 'invalidation_reason'] = 'meaningless_pattern'
            cleaned_df.loc[meaningless_mask, 'is_valid'] = False
            # 更新 previous_mask
            previous_mask = previous_mask | empty_after_clean_mask
            marked_count = (meaningless_mask & ~previous_mask).sum() # 计算本次新增的无效标记
            if marked_count > 0:
                logger.info(f"步骤 5/6: 根据无意义内容模式标记了 {marked_count} 行为无效。")
            else:
                 logger.info("步骤 5/6: 未发现匹配无意义内容模式的行。")
        except re.error as e:
            logger.error(f"错误：提供的无意义内容正则表达式无效：{e}。跳过此步骤。")
    else:
        logger.info("步骤 5/6: 未提供无意义内容模式，跳过此过滤。")
        # 为了保持一致性，创建一个空的掩码
        meaningless_mask = pd.Series(False, index=cleaned_df.index)

    # 6. 清洗后文本长度过滤 (基于 cleaned_text) - 注意这一步移到最后，保证在所有清洗后进行
    length_mask = cleaned_df['cleaned_text'].str.len() < min_text_length
    # 标记为无效
    cleaned_df.loc[length_mask & cleaned_df['is_valid'], 'invalidation_reason'] = f'text_length_<{min_text_length}'
    cleaned_df.loc[length_mask, 'is_valid'] = False
    # 更新 previous_mask
    previous_mask = previous_mask | meaningless_mask
    marked_count = (length_mask & ~previous_mask).sum() # 计算本次新增的无效标记
    if marked_count > 0:
        logger.info(f"步骤 6/6: 根据最小长度 ({min_text_length}) 标记了 {marked_count} 行为无效。")
    else:
        logger.info(f"步骤 6/6: 所有行均满足最小长度要求。")

    # --- 清理和总结 ---
    final_invalid_count = (~cleaned_df['is_valid']).sum()
    logger.info(f"通用社交媒体数据清洗（标记模式）完成。总行数: {original_count}, 标记无效行数: {final_invalid_count}")

    return cleaned_df 