#!/usr/bin/env python3
"""
文本预处理配置脚本
修改配置后运行: python preprocessor_config.py
"""

import asyncio
import logging
from batch_preprocessor import PreprocessorConfig, ProcessConfig, BatchPreprocessor

# 设置日志级别
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def main():
    # ========== 配置区域 ==========
    
    # 1. 预处理API配置
    api_config = PreprocessorConfig(
        # 🌐 预处理服务地址（确保服务已启动）
        base_url="http://localhost:8001",
        
        # 🚀 并发配置
        max_concurrent=100,          # 并发请求数（建议先用小值测试）
        timeout=30,                 # 请求超时时间（秒）
        retry_attempts=3,           # 重试次数
        retry_delay=1,              # 重试延迟（秒）
        
        # 📋 预处理选项配置
        remove_pii=True,            # 移除个人隐私信息
        emoji_convert=True,         # 将Emoji转换为文本
        emoji_remove=False,         # 直接移除Emoji（与emoji_convert互斥）
        remove_social_mentions=True,    # 移除@提及
        remove_weibo_reposts=True,      # 移除微博转发标记//
        remove_hashtags=True,           # 移除话题标签#
        enable_author_blacklist=False,  # 启用作者黑名单
        remove_ads=True,            # 移除广告信息
        remove_urls=True,           # 移除URL链接
        normalize_whitespace=True,  # 规范化空白字符
        normalize_unicode=True,     # Unicode规范化
        convert_fullwidth=True,     # 全角转半角
        detect_language=False,       # 检测语言（可选）
        split_sentences=True,      # 分句处理（可选）
        max_length=5000,          # 最大文本长度
        min_length=3,              # 最小文本长度
    )
    
    # 2. 数据处理配置
    process_config = ProcessConfig(
        # 📁 文件路径配置
        input_csv="data/raw/九号打标数据-0609.csv",              # 输入CSV文件路径
        output_csv="data/processed/九号打标数据-0609_cleaned.csv",     # 输出CSV文件路径
        
        # 📊 数据列配置
        text_column="正文",       # 待处理的文本列名
        author_column=None,         # 作者列名（如果有的话）
        id_column="序号",             # ID列名（如果有的话）
        
        # 🎯 数据筛选配置
        max_rows=None,              # 最大处理行数（None表示处理全部）
        random_sample_size=None,    # 随机抽样数量（测试时可用）
        random_seed=42,             # 随机种子（保证可重复性）
        
        # 🔍 条件筛选配置（可选）
        filter_column=None,         # 筛选字段名
        filter_values=None,         # 筛选值列表
        filter_condition="in",      # 筛选条件: 'in', 'not_in', 'equals', 'not_equals'
        
        # 💾 保存配置
        batch_size=1000,              # 每多少行保存一次进度
        jsonl_file=None,            # 进度保存文件（None表示自动生成）
    )
    
    # ========== 执行处理 ==========
    
    print("🚀 开始文本预处理任务...")
    print(f"📄 输入文件: {process_config.input_csv}")
    print(f"📄 输出文件: {process_config.output_csv}")
    print(f"🔧 并发数: {api_config.max_concurrent}")
    print(f"🎯 处理模式: {'随机抽样' if process_config.random_sample_size else '全量处理'}")
    if process_config.random_sample_size:
        print(f"📊 抽样数量: {process_config.random_sample_size}")
    print("=" * 50)
    
    try:
        async with BatchPreprocessor(api_config, process_config) as processor:
            result_df = await processor.process_batch()
            
            # 处理结果统计
            total_rows = len(result_df)
            success_rows = result_df['processing_success'].sum() if 'processing_success' in result_df.columns else 0
            failed_rows = total_rows - success_rows
            
            print("=" * 50)
            print("✅ 预处理任务完成！")
            print(f"📊 总处理行数: {total_rows}")
            print(f"✅ 成功处理: {success_rows}")
            print(f"❌ 处理失败: {failed_rows}")
            print(f"📄 结果已保存到: {process_config.output_csv}")
            
            if 'cleaned_text' in result_df.columns:
                # 清洗效果统计
                original_avg_len = result_df['original_length'].mean() if 'original_length' in result_df.columns else 0
                cleaned_avg_len = result_df['cleaned_length'].mean() if 'cleaned_length' in result_df.columns else 0
                avg_char_removed = result_df['char_removed'].mean() if 'char_removed' in result_df.columns else 0
                
                print(f"📏 平均原始长度: {original_avg_len:.1f} 字符")
                print(f"📏 平均清洗后长度: {cleaned_avg_len:.1f} 字符")
                print(f"🧹 平均移除字符: {avg_char_removed:.1f} 字符")
                
                if 'pii_count' in result_df.columns:
                    total_pii = result_df['pii_count'].sum()
                    total_emoji = result_df['emoji_count'].sum()
                    total_mentions = result_df['mentions_removed'].sum()
                    total_hashtags = result_df['hashtags_removed'].sum()
                    
                    print(f"🔒 移除PII信息: {total_pii} 处")
                    print(f"😀 处理Emoji: {total_emoji} 个")
                    print(f"@ 移除@提及: {total_mentions} 个")
                    print(f"# 移除话题标签: {total_hashtags} 个")
                
                # 句子切分统计
                if 'sentence_count' in result_df.columns:
                    total_sentences = result_df['sentence_count'].sum()
                    avg_sentences_per_text = result_df['sentence_count'].mean()
                    max_sentences = result_df['sentence_count'].max()
                    
                    print(f"📝 切分句子总数: {total_sentences} 个")
                    print(f"📝 平均每条文本句数: {avg_sentences_per_text:.1f} 句")
                    print(f"📝 单条文本最多句数: {max_sentences} 句")
            
    except KeyboardInterrupt:
        print("\n⚠️  任务被用户中断")
    except Exception as e:
        print(f"\n❌ 任务执行失败: {e}")
        logging.error(f"任务执行异常: {e}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(main())


# ========== 配置说明 ==========
"""
🔧 配置参数说明：

API配置 (PreprocessorConfig):
- base_url: 预处理服务地址
- max_concurrent: 并发请求数（建议10-50）
- timeout: 请求超时时间
- retry_attempts: 失败重试次数
- 各种预处理选项: 根据需求开启/关闭

处理配置 (ProcessConfig):
- input_csv/output_csv: 输入输出文件路径
- text_column: 文本内容列名
- author_column: 作者列名（可选）
- max_rows: 限制处理行数（测试用）
- random_sample_size: 随机抽样（测试用）
- filter_*: 条件筛选（可选）

📊 输出文件包含的新列：
- cleaned_text: 清洗后的文本
- original_length/cleaned_length: 原始/清洗后长度
- char_removed: 移除的字符数
- pii_count/emoji_count/mentions_removed等: 各种清洗统计
- processing_success: 是否处理成功
- processing_error: 错误信息（如果有）
- detected_language: 检测到的语言（如果启用）
- warnings: 警告信息（如果有）
- sentence_count: 切分后的句子数量（如果启用句子切分）
- sentences_text: 切分后的句子文本，用|||分隔（如果启用句子切分）
- sentences_detail: 句子详细信息的JSON格式（如果启用句子切分）

🚀 使用步骤：
1. 确保预处理服务已启动（端口8001）
2. 修改上述配置参数
3. 运行: python preprocessor_config.py
4. 查看输出文件和统计信息
""" 