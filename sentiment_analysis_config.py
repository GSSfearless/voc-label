#!/usr/bin/env python3
"""
汽车工单情感分析专用配置
针对汽车维修服务场景优化的情感分析配置
"""

import asyncio
from batch_llm_api import APIConfig, ProcessConfig, LLMBatchProcessor

async def car_service_sentiment_analysis():
    """汽车服务工单情感分析"""
    
    # API配置 - 专门针对汽车服务场景
    api_config = APIConfig(
        api_key="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",
        model="google/gemini-2.5-flash-preview-05-20",
        max_concurrent=3,
        timeout=60,
        retry_attempts=2,
        system_prompt="""你是一个专业的汽车服务行业情感分析专家，专门分析汽车维修、保养、销售等服务场景中的客户反馈。

专业背景：
- 深度了解汽车行业术语和服务流程
- 熟悉客户在汽车服务中的常见关注点
- 能够准确识别服务质量、技术水平、价格合理性等方面的情感

分析要求：
1. 情感分类：positive(正面)/negative(负面)/neutral(中性)
2. 置信度：0.0-1.0，反映分析的确定性
3. 具体情绪：如满意、不满、担忧、赞赏、抱怨等
4. 关键词：提取反映情感的关键词汇
5. 评分：1-5分制（1=很不满意，2=不满意，3=一般，4=满意，5=很满意）
6. 服务维度：识别涉及的服务方面（如技术、态度、价格、时效等）

输出格式要求：
- 必须严格按照JSON格式返回
- 所有字段都要填写，如果无法确定则标注为null
- 关键词要具体且相关"""
    )
    
    # 处理配置
    process_config = ProcessConfig(
        input_csv="N7原始数据.csv",
        output_csv="汽车服务情感分析结果.csv",
        input_column="内容",
        prompt_template="""请分析以下汽车服务相关文本的情感倾向：

客户反馈内容：{input_text}

请从汽车服务行业的角度进行专业分析，返回以下JSON格式：

{{
    "sentiment": "positive/negative/neutral",
    "confidence": 0.85,
    "emotion": "具体情绪描述（如：满意、不满、担忧等）",
    "keywords": ["关键词1", "关键词2", "关键词3"],
    "score": 4,
    "service_dimension": ["技术质量", "服务态度", "价格", "时效性"],
    "main_concern": "客户主要关注的问题",
    "suggestion": "改进建议（如果是负面反馈）"
}}""",
        
        output_json_fields=[
            "sentiment", "confidence", "emotion", "keywords", 
            "score", "service_dimension", "main_concern", "suggestion"
        ],
        
        max_rows=50  # 测试用，可以调整为None处理全部数据
    )
    
    print("🚗 汽车服务情感分析开始...")
    print(f"📁 输入文件: {process_config.input_csv}")
    print(f"📁 输出文件: {process_config.output_csv}")
    print(f"🎯 分析维度: 情感、置信度、具体情绪、关键词、评分、服务维度")
    print("-" * 60)
    
    try:
        async with LLMBatchProcessor(api_config, process_config) as processor:
            result_df = await processor.process_batch()
            processor.save_results(result_df)
            
            # 详细统计分析
            total_rows = len(result_df)
            success_rows = len(result_df[result_df['parsing_success'] == True])
            success_rate = success_rows / total_rows * 100 if total_rows > 0 else 0
            
            print("-" * 60)
            print("✅ 汽车服务情感分析完成！")
            print(f"📊 总处理行数: {total_rows}")
            print(f"✅ 成功处理: {success_rows}")
            print(f"📈 成功率: {success_rate:.1f}%")
            
            # 情感分布统计
            if success_rows > 0:
                sentiment_counts = result_df['sentiment'].value_counts()
                print("\n📊 情感分布:")
                for sentiment, count in sentiment_counts.items():
                    percentage = count / success_rows * 100
                    print(f"  {sentiment}: {count}条 ({percentage:.1f}%)")
                
                # 平均评分
                avg_score = result_df['score'].dropna().mean()
                if not pd.isna(avg_score):
                    print(f"\n⭐ 平均满意度评分: {avg_score:.2f}/5.0")
                
                print(f"\n📁 详细结果已保存到: {process_config.output_csv}")
                
    except Exception as e:
        print(f"❌ 处理失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    import pandas as pd
    
    print("🚗 汽车服务情感分析专用工具")
    print("=" * 60)
    print("专门针对汽车维修、保养、销售等服务场景的情感分析")
    print("分析维度：情感倾向、置信度、具体情绪、关键词、评分、服务维度")
    print()
    
    # 检查输入文件
    import os
    if not os.path.exists("N7原始数据.csv"):
        print("❌ 找不到N7原始数据.csv文件")
        print("请确保文件在当前目录下")
        exit(1)
    
    # 预览数据
    try:
        df = pd.read_csv("N7原始数据.csv")
        print(f"📋 数据预览: 共{len(df)}行数据")
        if '内容' in df.columns:
            print("✅ 找到'内容'列")
            print("前3条数据内容:")
            for i, content in enumerate(df['内容'].head(3)):
                print(f"  {i+1}. {str(content)[:50]}...")
        else:
            print("❌ 找不到'内容'列，请检查CSV文件格式")
            print(f"当前列名: {list(df.columns)}")
            exit(1)
    except Exception as e:
        print(f"❌ 读取数据文件失败: {e}")
        exit(1)
    
    print("\n" + "="*60)
    input("按回车键开始分析...")
    
    # 运行分析
    asyncio.run(car_service_sentiment_analysis()) 