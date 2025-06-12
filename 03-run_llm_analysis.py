#!/usr/bin/env python3
"""
九号电动车社媒评论分析脚本
修改下面的配置，然后运行：python ninebot.py

数据采样方式：
1. 全部数据处理：max_rows=None, random_sample_size=None
2. 前N行处理：max_rows=1000, random_sample_size=None  
3. 随机抽样N行：max_rows=None, random_sample_size=500
4. 可重复随机抽样：设置 random_seed=42（固定种子确保每次抽样结果相同）

数据筛选功能（新增）：
可以根据某个字段的值来筛选需要处理的数据：
1. 筛选特定值：filter_column="is_processed", filter_values=[0], filter_condition="in"
2. 排除特定值：filter_column="is_processed", filter_values=[1], filter_condition="not_in"  
3. 等于某值：filter_column="status", filter_values=["pending"], filter_condition="equals"
4. 不等于某值：filter_column="status", filter_values=["done"], filter_condition="not_equals"

缓存功能（节省API成本）：
1. 智能缓存：相同的文本内容会自动使用缓存结果，避免重复的API调用
2. 缓存文件：默认保存在 data/cache/llm_analysis_cache.json
3. 缓存过期：可以设置缓存的有效期，默认7天
4. 成本节省：重复处理时会显示缓存命中率和节省的API调用次数

使用场景示例：
- 只处理未处理的数据：filter_column="processed", filter_values=[0, "否", "N"], filter_condition="in"
- 跳过已处理的数据：filter_column="processed", filter_values=[1, "是", "Y"], filter_condition="not_in"

注意：
- random_sample_size 优先级高于 max_rows
- 筛选条件会在采样之前应用
- 输出文件会保留所有原始数据，未处理的行对应字段留空
- 随机抽样适合大数据集的快速测试和验证
- 设置随机种子可以确保抽样结果的可重复性
- 缓存基于文本内容的哈希值，确保内容完全相同才会命中缓存
"""

import asyncio
import logging
import os
from dotenv import load_dotenv
from batch_llm_api import APIConfig, ProcessConfig, LLMBatchProcessor

# 加载环境变量
load_dotenv()

# 设置为INFO级别，避免过多调试信息
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def main():
    # ========== 配置区域 ==========
    
    # 1. API配置 - 从环境变量读取API密钥
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise ValueError("请设置环境变量 OPENROUTER_API_KEY，或在.env文件中配置")
    
    api_config = APIConfig(
        api_key=api_key,  # 🔑 从环境变量读取API密钥
        model="google/gemini-2.5-flash-preview-05-20",                   # 🤖 可选的模型
        max_concurrent=60,                        # 🚀 并发数（建议先用小值测试）
        timeout=60,                              # ⏰ 超时时间
        retry_attempts=1,                        # 🔄 重试次数
        system_prompt="""你是一个电动车行业分类专家，你遵循给定的层级标签体系进行文本分类，如果无法判断，请返回其他。
        以下是固定的一套标签体系：
        产品支持#产品体验#产品建议#产品建议
产品支持#产品体验#产品续航#产品续航
产品支持#产品体验#产品设计#产品设计
产品支持#产品体验#骑行体验#骑行体验
产品支持#产品故障#电池类#掉电快
产品支持#产品故障#异响类#刹车异响
产品支持#产品体验#产品性能#速度
产品支持#产品故障#外观类#外观不良
产品支持#产品故障#充电类#无法充电
产品支持#产品故障#显示类#显示屏显示异常
产品支持#产品故障#外观类#外观件断裂/脱落
产品支持#APP#设备首页#蓝牙连接
产品支持#APP#设备数据#骑行轨迹
产品支持#产品体验#产品性能#刹车
产品支持#产品故障#骑行类#骑行断电
产品支持#产品咨询#产品使用#产品使用
产品支持#产品故障#灯类#灯光异常
产品支持#产品体验#产品性能#减震
产品支持#APP#智能防盗#智能服务费
产品支持#产品故障#骑行类#骑行晃动/抖动
产品支持#产品故障#异响类#前/后轮异响
产品支持#产品咨询#产品改装#产品改装
产品支持#产品故障#异响类#减震异响
产品支持#产品故障#开关机类#无法开关机
产品支持#APP#设备数据#剩余里程（续航）
产品支持#APP#功能设置#氮气加速开关
产品支持#产品故障#油门/刹车类#油门/刹车失灵
产品支持#产品体验#产品性能#加速
产品支持#产品体验#产品性能#爬坡
产品支持#APP#设备数据#精准续航
产品支持#APP#功能设置#油门转把
产品支持#APP#智能防盗#异动报警
产品支持#APP#功能设置#能量回收
产品支持#产品故障#开关机类#自动开关机
服务#政策法规#地方政策#上牌/上路/携带/禁摩/限摩
服务#政策法规#三包政策#保修标准
服务#线下服务#服务店人员投诉#服务态度
销售#线上销售#线上销售页面#线上销售页面
销售#线下销售#销售门店#门店价格
销售#线下销售#销售门店#非新品/非官方
销售#线上销售#线上销售订单#线上销售订单
销售#线上销售#线上销售订单#降价
销售#线下销售#销售门店#门店上牌
销售#线下销售#销售门店#销售店人员投诉
销售#线下销售#销售门店#核销/交付
销售#线上销售#线上销售退款#线上销售退款
疑似危机#疑似危机#媒体/平台#微博/黑猫/抖音/小红书/社群/贴吧/消费保
疑似危机#疑似危机#摔车客诉#轻微擦伤或破皮
疑似危机#疑似危机#摔车客诉#四肢骨折等对于人身健康有重大的损坏
营销#营销活动#新品发布#新品发布

本品品牌常见为9号/Segway，车型常见为ZT3 Pro、Max G2

常见竞品品牌包括：Navee（S65C）
小米/xiaomi（4 Pro Max）
Kaabo （Mantis 10）
Dualtron（Mini）
        """,  # 🎭 系统提示词
        # 💾 缓存配置 - 节省API调用成本
        enable_cache=True,                       # 🔧 启用缓存功能
        cache_file="data/cache/llm_analysis_cache.json",  # 📁 缓存文件路径
        cache_ttl=7*24*3600,                     # ⏳ 缓存过期时间（7天），None表示永不过期
    )
    
    # 2. 处理配置
    process_config = ProcessConfig(
        input_csv="data/processed/境外汇总_20250609_sentences.csv",               # 📁 输入文件
        output_csv="data/results/境外汇总_20250609-cleaned-sentences-results.csv",                # 📁 输出文件
        input_column="sentence_text",                     # 📝 要处理的列名
        
        # 📋 Prompt模板 - 根据您的需求修改
        prompt_template="""请分析以下文本内容中涉及到的分类、观点、情感、意图、品牌、车型，并以JSON格式返回结果。

文本内容：{input_text}

分析要求：

1. 文本中可能涉及多个标签，多个标签分类都需要给出，以给定的标签体系为准，如果没有合适的标签，则可以按照标签框架生成合适标签，但是标记 is_fixed_tag 为 False

2. 情感返回正向、负向、中性，并给出置信度，置信度范围为0-1

3. 用户意图返回咨询、投诉、建议、赞扬、其他，不能够和情感发生冲突

4. 观点表达分为主体词和描述词，不要过长，不要重复，描述词不要超过10个字

5. 标准化规范化后的观点表达需要能够作为独立标签展示，因此含义要清晰

6. 请严格按照以下JSON格式返回，不要有任何其他解释文字， 以下是示例：

输入：昨天刚买的M95C 电瓶充电一直是绿灯 有没有大佬知道怎么解决啊
输出：
[{{
    "sentiment": "负向",
    "confidence": 0.8,
    "intent": "咨询",
    "aspect": "电瓶充电",
    "desc": "充电一直是绿灯", 
    "normalized_viewpoint": "咨询充电绿灯原因",
    "tag": "产品支持#产品故障#充电类#其他充电类",
    "is_fixed_tag": true,
    "brand": "九号",
    "model": "M95C"
}}]
""",
        
        # 📊 要提取的JSON字段
        output_json_fields=["sentiment", "confidence", "intent", "aspect", "desc", "normalized_viewpoint", "tag", "brand", "model", "is_fixed_tag"],
        
        # 🎲 数据采样配置（可选）
        max_rows=None,  # 🔢 按顺序取前N行，None表示不限制
        random_sample_size=None,  # 🎯 随机抽样N行进行处理，None表示不抽样
        random_seed=42,  # 🌱 随机种子，确保抽样结果可重复（可选）
        
        # 🔍 数据筛选配置（新增功能）
        # 示例1：只处理标记为需要处理的数据
        # filter_column="need_process",  # 📋 筛选字段名
        # filter_values=[1, "是", "Y", "yes"],  # 📝 筛选值列表（包含这些值的行会被处理）
        # filter_condition="in",  # 📍 筛选条件
        
        # 示例2：跳过已经处理过的数据  
        # filter_column="is_processed",
        # filter_values=[1, "已处理", "done"],
        # filter_condition="not_in",
        
        filter_column="is_valid",  # 📋 设置为None表示不使用筛选，处理所有数据
        filter_values=[1],  # 📝 筛选值列表
        filter_condition="in",  # 📍 筛选条件：'in'包含, 'not_in'不包含, 'equals'等于, 'not_equals'不等于
        
        jsonl_file="llm_results_progress.jsonl",  # 📝 阶段性保存的jsonl文件
        batch_size=60  # 🔄 每30行保存一次
    )
    
    # ========== 执行处理 ==========
    
    print("🚀 开始批量处理...")
    print(f"📁 输入文件: {process_config.input_csv}")
    print(f"📁 输出文件: {process_config.output_csv}")
    print(f"🤖 使用模型: {api_config.model}")
    print(f"🔄 最大并发: {api_config.max_concurrent}")
    
    # 显示缓存配置
    if api_config.enable_cache:
        print(f"💾 缓存功能: 已启用")
        print(f"📁 缓存文件: {api_config.cache_file}")
        if api_config.cache_ttl:
            print(f"⏳ 缓存期限: {api_config.cache_ttl//3600//24} 天")
        else:
            print(f"⏳ 缓存期限: 永不过期")
    else:
        print("💾 缓存功能: 已禁用")
    
    # 显示数据筛选配置
    if process_config.filter_column:
        print(f"🔍 数据筛选: {process_config.filter_column} {process_config.filter_condition} {process_config.filter_values}")
    else:
        print("🔍 数据筛选: 无筛选，处理所有数据")
    
    # 显示数据采样配置
    if process_config.random_sample_size:
        print(f"🎯 随机抽样: {process_config.random_sample_size} 行")
        if process_config.random_seed is not None:
            print(f"🌱 随机种子: {process_config.random_seed}")
    elif process_config.max_rows:
        print(f"🔢 限制行数: 前 {process_config.max_rows} 行")
    else:
        print("📊 处理模式: 全部数据")
    
    print("-" * 50)
    
    try:
        async with LLMBatchProcessor(api_config, process_config) as processor:
            result_df = await processor.process_batch()
            processor.save_results(result_df)
            
            # 显示统计信息
            total_rows = len(result_df)
            success_rows = len(result_df[result_df['parsing_success'] == True])
            success_rate = success_rows / total_rows * 100 if total_rows > 0 else 0
            
            print("-" * 50)
            print("✅ 处理完成！")
            print(f"📊 总处理行数: {total_rows}")
            print(f"✅ 成功处理: {success_rows}")
            print(f"📈 成功率: {success_rate:.1f}%")
            print(f"📁 结果已保存到: {process_config.output_csv}")
            print(f"📝 进度文件: {process_config.jsonl_file}")
            
            # 显示前几行结果预览
            if len(result_df) > 0:
                print("\n📋 结果预览:")
                for field in process_config.output_json_fields:
                    if field in result_df.columns:
                        print(f"  {field}: {result_df[field].dropna().head(3).tolist()}")
            
            # 询问是否清理jsonl文件
            print(f"\n💡 提示: 进度文件 {process_config.jsonl_file} 已完成使命")
            print("   您可以手动删除它，或保留作为备份")
            print("   如需重新处理，请先删除此文件以避免跳过已处理数据")
            
    except Exception as e:
        print(f"❌ 处理失败: {e}")
        print("💡 请检查:")
        print("  1. API密钥是否正确")
        print("  2. 输入文件是否存在")
        print("  3. 网络连接是否正常")
        print("  4. API配额是否充足")

if __name__ == "__main__":
    print("🎯 批量LLM API调用脚本")
    print("=" * 50)
    
    # # 检查是否需要修改配置
    # import os
    # if not os.path.exists("test_data.csv"):
    #     print("⚠️  测试数据文件不存在，请先创建test_data.csv")
    #     print("可以使用提供的test_data.csv作为示例")
    #     exit(1)
    
    # print("⚠️  请确保已修改quick_start.py中的API密钥！")
    # input("按回车键继续...")
    
    # 运行主程序
    asyncio.run(main()) 