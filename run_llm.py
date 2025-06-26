#!/usr/bin/env python3
"""

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
        model="Pro/deepseek-ai/DeepSeek-V3",                   # 🤖 可选的模型
        max_concurrent=100,                        # 🚀 并发数（建议先用小值测试）
        timeout=60,                              # ⏰ 超时时间
        retry_attempts=1,                        # 🔄 重试次数
        system_prompt="""**角色**: 你是一名顶尖的汽车行业客户之声观点挖掘专家。

**任务**: 你的核心任务是深入理解文本，从中提炼出客户对于车辆的观点表达，抽取出结构化的观点信息，不要分析客服或销售的反馈和操作。

### **核心逻辑：从宏观到微观**

你必须遵循以下三个层次的分析逻辑，对信息的提炼越来越精细：
1.  **`category` (分类)**: 最宏观的层级，将观点归入一个固定的分类中。
2.  **`topic` (主题)**: 中间层级，将观点提炼成一个标准化的、概括性的观点标签。
3.  **`opinion` (观点)**: 最细化的层级，简短精确描述用户的原始观点。

### **字段定义**

请为文本中的每一个独立观点，抽取以下8个字段：

1.  **`car_brand` (汽车品牌)**: 汽车品牌。若未提及或无法推断，则返回空字符串 `""`。
2.  **`car_model` (汽车型号)**: 具体的车型。若无，则返回空字符串 `""`。
3.  **`scenario` (场景)**: 观点所处的特定环境或条件 (如: "高速上")。若无，则返回空字符串 `""`。
4.  **`category` (观点分类)**: **【关键字段】** 从下方的“固定标签体系”中，通过组合“一级分类标题”和“二级分类列表项”来构建此字段的值。**最终输出格式必须是 `"一级分类.二级分类"` 的点分格式**。
5.  **`topic` (观点标签)**: 对观点的核心内容进行概括，形成一个标准化的观点标签，通常例如: "加速强劲", "换挡顿挫"。
6.  **`opinion` (核心观点)**: 从原文中提炼出的、能独立表达完整含义的精炼观点（例如: "高速上加速够强悍", "加档减档有顿挫"）。
7.  **`sentiment` (情感)**: 观点的情感倾向。必须是 **"正向"**, **"负向"**, **"中性"** 之一。
8.  **`intent` (意图)**: 表达者的目的。必须是 **"赞扬"**, **"抱怨"**, **"建议"**, **"咨询"**, **"陈述"** 之一。

---

### **固定标签体系 (用于构建 `category` 字段)**

#### 产品体验
- 外观
- 内饰
- 空间
- 性能
- 三电
- 舒适性
- 隐私性
- 质量口碑
- 安全性
- 环保性
- 经济性
- 功能性
- 车型配置
- 充电相关

#### 品牌体验
- 品牌宣传
- 品牌力

#### 权益服务
- 购车权益（4S）
- 用车权益
- 会员权益
- 活动权益

#### 售后服务
- 质保服务
- 服务流程
- 救援服务
- 环境和设施
- 人员表现
- 售后精品
- 维修保养费用
- 维修保养时间
- 维修保养效果
- 召回/技术升级
- 服务体验
- 年验服务

#### 线上触点体验
- APP商城
- APP使用
- 车联网服务
- 官网
- 官方微博
- 企业微信
- 直播
- 微信公众号
- 客服热线

#### 销售服务
- 产品动态体验
- 产品静态体验
- 充电设备
- 服务体验
- 购买过程体验
- 环境和设施
- 回访及客户关怀
- 交付体验
- 二手车

#### 智能化体验
- 智能驾驶
- 智能座舱

---

### **输出要求**

1.  **格式**: 严格按照下面的JSON格式，生成一个包含所有已抽取观点对象的数组。
2.  **最终输出**: **只返回**完整的JSON数组字符串。禁止添加任何解释、注释或其他无关文字。

### **示例**

**输入**:
`Cayenne在高速上加速够强悍，方向轻，加档减档有顿挫，维修保养贵`

**输出**:
```json
[
    {"car_brand":"保时捷","car_model":"卡宴","scenario":"高速上","category":"产品体验.性能","topic":"加速强劲","opinion":"加速够强悍","sentiment":"正向","intent":"赞扬"},
    {"car_brand":"保时捷","car_model":"卡宴","scenario":"","category":"产品体验.性能","topic":"方向盘手感轻巧","opinion":"方向轻","sentiment":"正向","intent":"赞扬"},
    {"car_brand":"保时捷","car_model":"卡宴","scenario":"","category":"产品体验.性能","topic":"换挡顿挫","opinion":"加档减档有顿挫","sentiment":"负向","intent":"抱怨"},
    {"car_brand":"保时捷","car_model":"卡宴","scenario":"","category":"售后服务.维修保养费用","topic":"维修保养费用高","opinion":"维修保养贵","sentiment":"负向","intent":"抱怨"}
]""",  # 🎭 系统提示词
        # 💾 缓存配置 - 节省API调用成本
        enable_cache=True,                       # 🔧 启用缓存功能
        cache_file="data/cache/llm_analysis_cache.json",  # 📁 缓存文件路径
        cache_ttl=7*24*3600,                     # ⏳ 缓存过期时间（7天），None表示永不过期
    )
    
    # 2. 处理配置
    process_config = ProcessConfig(
        input_csv="/Users/hanzhang/Projects/qwen-sft/original_text.csv",               # 📁 输入文件
        output_csv="/Users/hanzhang/Projects/qwen-sft/original_text_results-deepseek.csv",                # 📁 输出文件
        input_column="text",                     # 📝 要处理的列名
        
        # 📋 Prompt模板 - 根据您的需求修改
        prompt_template="""请分析以下文本内容，并按照要求输出JSON格式。

输入文本内容：{input_text}
""",
        
        # 📊 要提取的JSON字段
        output_json_fields=["car_brand", "car_model", "scenario", "category", "topic", "opinion", "sentiment", "intent"],
        
        # 🎲 数据采样配置（可选）
        max_rows=5000,  # 🔢 按顺序取前N行，None表示不限制
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
        
        filter_column=None,  # 📋 设置为None表示不使用筛选，处理所有数据
        filter_values=[1],  # 📝 筛选值列表
        filter_condition="in",  # 📍 筛选条件：'in'包含, 'not_in'不包含, 'equals'等于, 'not_equals'不等于
        
        jsonl_file="llm_results_progress.jsonl",  # 📝 阶段性保存的jsonl文件
        batch_size=100  # 🔄 每30行保存一次
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