"""
配置示例文件
"""

from batch_llm_api import APIConfig, ProcessConfig, LLMBatchProcessor
import asyncio

# 示例1: 情感分析
async def sentiment_analysis_example():
    """情感分析示例"""
    api_config = APIConfig(
        api_key="your-openrouter-api-key",  # 替换为您的API密钥
        model="openai/gpt-4o",
        max_concurrent=3,  # 并发数
        timeout=60,
        retry_attempts=3,
        system_prompt="""你是一个专业的情感分析师，专门分析用户评论的情感倾向。
你需要准确识别文本中的情感色彩，包括正面、负面和中性情感。
请严格按照指定的JSON格式返回分析结果，确保数据的一致性和准确性。"""
    )
    
    process_config = ProcessConfig(
        input_csv="data/reviews.csv",
        output_csv="results/sentiment_results.csv",
        input_column="review_text",
        prompt_template="""请分析以下评论的情感倾向：

评论内容：{input_text}

请返回JSON格式结果：
{{
    "sentiment": "positive/negative/neutral",
    "confidence": 0.85,
    "emotion": "喜悦/愤怒/悲伤/恐惧/惊讶/厌恶/中性",
    "keywords": ["关键词1", "关键词2"],
    "score": 4.2
}}""",
        output_json_fields=["sentiment", "confidence", "emotion", "keywords", "score"],
        max_rows=100  # 测试时可以限制行数
    )
    
    async with LLMBatchProcessor(api_config, process_config) as processor:
        result_df = await processor.process_batch()
        processor.save_results(result_df)

# 示例2: 文本分类
async def text_classification_example():
    """文本分类示例"""
    api_config = APIConfig(
        api_key="your-openrouter-api-key",
        model="anthropic/claude-3-haiku",  # 使用更便宜的模型
        max_concurrent=5,
        timeout=30,
        retry_attempts=2,
        system_prompt="""你是一个专业的文本分类专家，专门对新闻文章进行准确分类。
你熟悉各种新闻类别，能够准确识别文章的主题和内容领域。
请根据文章内容准确分类，并提供相关的标签和摘要。
返回结果必须严格遵循JSON格式。"""
    )
    
    process_config = ProcessConfig(
        input_csv="data/news_articles.csv",
        output_csv="results/classification_results.csv",
        input_column="article_content",
        prompt_template="""请对以下新闻文章进行分类：

文章内容：{input_text}

请返回JSON格式的分类结果：
{{
    "category": "科技/体育/政治/经济/娱乐/社会/其他",
    "subcategory": "具体子类别",
    "confidence": 0.92,
    "tags": ["标签1", "标签2", "标签3"],
    "summary": "一句话总结文章主要内容"
}}""",
        output_json_fields=["category", "subcategory", "confidence", "tags", "summary"],
        max_rows=None
    )
    
    async with LLMBatchProcessor(api_config, process_config) as processor:
        result_df = await processor.process_batch()
        processor.save_results(result_df)

# 示例3: 内容提取
async def content_extraction_example():
    """内容提取示例"""
    api_config = APIConfig(
        api_key="your-openrouter-api-key",
        model="openai/gpt-3.5-turbo",
        max_concurrent=8,
        timeout=45,
        retry_attempts=3,
        system_prompt="""你是一个专业的产品信息提取专家，专门从产品描述中提取关键信息。
你能够准确识别产品名称、品牌、价格、特性等重要信息。
请仔细分析产品描述，提取所有可用的关键信息。
如果某些信息在描述中不存在，请在对应字段中返回null。
输出必须严格遵循JSON格式。"""
    )
    
    process_config = ProcessConfig(
        input_csv="data/product_descriptions.csv",
        output_csv="results/extracted_features.csv",
        input_column="description",
        prompt_template="""请从以下产品描述中提取关键信息：

产品描述：{input_text}

请提取并返回JSON格式的信息：
{{
    "product_name": "产品名称",
    "brand": "品牌名",
    "price_range": "价格区间",
    "key_features": ["特性1", "特性2", "特性3"],
    "target_audience": "目标用户群体",
    "color_options": ["颜色1", "颜色2"],
    "size_info": "尺寸信息",
    "material": "材质信息"
}}""",
        output_json_fields=[
            "product_name", "brand", "price_range", "key_features", 
            "target_audience", "color_options", "size_info", "material"
        ],
        max_rows=None
    )
    
    async with LLMBatchProcessor(api_config, process_config) as processor:
        result_df = await processor.process_batch()
        processor.save_results(result_df)

# 运行示例
if __name__ == "__main__":
    # 选择要运行的示例
    print("选择要运行的示例:")
    print("1. 情感分析")
    print("2. 文本分类")
    print("3. 内容提取")
    
    choice = input("请输入选择 (1-3): ")
    
    if choice == "1":
        asyncio.run(sentiment_analysis_example())
    elif choice == "2":
        asyncio.run(text_classification_example())
    elif choice == "3":
        asyncio.run(content_extraction_example())
    else:
        print("无效选择") 