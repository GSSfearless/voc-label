#!/usr/bin/env python3
"""
第4步：后处理分析脚本
主要功能：
1. 验证tag列是否存在于已有标签体系中
2. 判断brand和model是否属于关心的车型
"""

import pandas as pd
import os
import logging

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_predefined_tags():
    """获取预定义的标签体系"""
    predefined_tags = [
        "产品支持#产品体验#产品建议#产品建议",
"产品支持#产品体验#产品续航#产品续航",
"产品支持#产品体验#产品设计#产品设计",
"产品支持#产品体验#骑行体验#骑行体验",
"产品支持#产品故障#电池类#掉电快",
"产品支持#产品故障#异响类#刹车异响",
"产品支持#产品体验#产品性能#速度",
"产品支持#产品故障#外观类#外观不良",
"产品支持#产品故障#充电类#无法充电",
"产品支持#产品故障#显示类#显示屏显示异常",
"产品支持#产品故障#外观类#外观件断裂/脱落",
"产品支持#APP#设备首页#蓝牙连接",
"产品支持#APP#设备数据#骑行轨迹",
"产品支持#产品体验#产品性能#刹车",
"产品支持#产品故障#骑行类#骑行断电",
"产品支持#产品咨询#产品使用#产品使用",
"产品支持#产品故障#灯类#灯光异常",
"产品支持#产品体验#产品性能#减震",
"产品支持#APP#智能防盗#智能服务费",
"产品支持#产品故障#骑行类#骑行晃动/抖动",
"产品支持#产品故障#异响类#前/后轮异响",
"产品支持#产品咨询#产品改装#产品改装",
"产品支持#产品故障#异响类#减震异响",
"产品支持#产品故障#开关机类#无法开关机",
"产品支持#APP#设备数据#剩余里程（续航）",
"产品支持#APP#功能设置#氮气加速开关",
"产品支持#产品故障#油门/刹车类#油门/刹车失灵",
"产品支持#产品体验#产品性能#加速",
"产品支持#产品体验#产品性能#爬坡",
"产品支持#APP#设备数据#精准续航",
"产品支持#APP#功能设置#油门转把",
"产品支持#APP#智能防盗#异动报警",
"产品支持#APP#功能设置#能量回收",
"产品支持#产品故障#开关机类#自动开关机",
"服务#政策法规#地方政策#上牌/上路/携带/禁摩/限摩",
"服务#政策法规#三包政策#保修标准",
"服务#线下服务#服务店人员投诉#服务态度",
"销售#线上销售#线上销售页面#线上销售页面",
"销售#线下销售#销售门店#门店价格",
"销售#线下销售#销售门店#非新品/非官方",
"销售#线上销售#线上销售订单#线上销售订单",
"销售#线上销售#线上销售订单#降价",
"销售#线下销售#销售门店#门店上牌",
"销售#线下销售#销售门店#销售店人员投诉",
"销售#线下销售#销售门店#核销/交付",
"销售#线上销售#线上销售退款#线上销售退款",
"疑似危机#疑似危机#媒体/平台#微博/黑猫/抖音/小红书/社群/贴吧/消费保",
"疑似危机#疑似危机#摔车客诉#轻微擦伤或破皮",
"疑似危机#疑似危机#摔车客诉#四肢骨折等对于人身健康有重大的损坏",
"营销#营销活动#新品发布#新品发布",
    ]
    return set(predefined_tags)

def get_target_brands_models():
    """获取关心的品牌和车型信息，包含车型别名"""
    target_info = {
        # Segway本品
        "Segway": {
            "ZT3 Pro": ["ZT3 Pro", "ZT3Pro", "zt3 pro", "zt3pro", "ZT3", "zt3", "ZT3P", "zt3p"],
            "Max G2": ["Max G2", "MaxG2", "max g2", "maxg2", "MAX G2", "MAXG2", "G2", "g2"]
        },
        "segway": {
            "ZT3 Pro": ["ZT3 Pro", "ZT3Pro", "zt3 pro", "zt3pro", "ZT3", "zt3", "ZT3P", "zt3p"],
            "Max G2": ["Max G2", "MaxG2", "max g2", "maxg2", "MAX G2", "MAXG2", "G2", "g2"]
        },
        
        # 竞品Navee
        "Navee": {
            "S65C": ["S65C", "s65c", "S65", "s65", "65C", "65c"]
        },
        "navee": {
            "S65C": ["S65C", "s65c", "S65", "s65", "65C", "65c"]
        },
        
        # 竞品小米
        "小米": {
            "4 Pro Max": ["4 Pro Max", "4ProMax", "4 pro max", "4promax", "4PM", "4pm", "Pro Max", "pro max"]
        },
        "xiaomi": {
            "4 Pro Max": ["4 Pro Max", "4ProMax", "4 pro max", "4promax", "4PM", "4pm", "Pro Max", "pro max"]
        },
        
        # 竞品Kaabo
        "Kaabo": {
            "Mantis 10": ["Mantis 10", "Mantis10", "mantis 10", "mantis10", "Mantis", "mantis", "M10", "m10"]
        },
        "kaabo": {
            "Mantis 10": ["Mantis 10", "Mantis10", "mantis 10", "mantis10", "Mantis", "mantis", "M10", "m10"]
        },
        
        # 竞品Dualtron
        "Dualtron": {
            "Mini": ["Mini", "mini", "MINI", "Dualtron Mini", "dualtron mini"]
        },
        "dualtron": {
            "Mini": ["Mini", "mini", "MINI", "Dualtron Mini", "dualtron mini"]
        }
    }
    return target_info

def check_tag_in_predefined(tag, predefined_tags):
    """检查tag是否在预定义标签体系中"""
    if pd.isna(tag) or tag == "":
        return False
    return str(tag).strip() in predefined_tags

def normalize_brand_model(brand, model, target_info):
    """规范化品牌和车型名称"""
    if pd.isna(brand) or brand == "":
        return "其他", "其他"
    
    brand_str = str(brand).strip().lower()
    model_str = str(model).strip().lower() if not pd.isna(model) else ""
    
    # 品牌映射表
    brand_mapping = {
        "Segway": "Segway",
        "segway": "Segway",
        "九号": "Segway",
        "九号电动车": "Segway",
        "ninebot": "Segway",
        "Navee": "Navee",
        "navee": "Navee",
        "小米": "小米",
        "xiaomi": "小米",
        "Kaabo": "Kaabo",
        "kaabo": "Kaabo",
        "Dualtron": "Dualtron",
        "dualtron": "Dualtron"
    }
    
    # 检查品牌匹配并规范化
    normalized_brand = "其他"
    matched_model_dict = {}
    
    for target_brand, model_dict in target_info.items():
        if target_brand.lower() in brand_str or brand_str in target_brand.lower():
            normalized_brand = brand_mapping.get(target_brand, "其他")
            matched_model_dict = model_dict
            break
    
    # 检查车型匹配并规范化
    normalized_model = "其他"
    if normalized_brand != "其他" and model_str:
        for standard_model, aliases in matched_model_dict.items():
            for alias in aliases:
                # 检查完全匹配或包含关系
                if (alias.lower() == model_str or 
                    alias.lower() in model_str or 
                    model_str in alias.lower()):
                    normalized_model = standard_model
                    break
            if normalized_model != "其他":
                break
    
    return normalized_brand, normalized_model

def process_data(input_file, output_file):
    """处理数据，添加验证列"""
    print(f"🔄 开始处理文件: {input_file}")
    
    # 读取数据
    df = pd.read_csv(input_file)
    print(f"📊 读取数据行数: {len(df)}")
    
    # 获取预定义标签和目标品牌车型
    predefined_tags = get_predefined_tags()
    target_info = get_target_brands_models()
    
    print(f"📋 预定义标签数量: {len(predefined_tags)}")
    print(f"🎯 目标品牌数量: {len(target_info)}")
    
    # 1. 检查tag是否在预定义标签体系中
    print("🏷️  检查标签是否在预定义体系中...")
    df['is_tag_in_predefined'] = df['tag'].apply(
        lambda x: check_tag_in_predefined(x, predefined_tags)
    )
    
    # 2. 规范化品牌和车型名称
    print("🚗 规范化品牌和车型名称...")
    brand_model_results = df.apply(
        lambda row: normalize_brand_model(row['brand'], row['model'], target_info),
        axis=1
    )
    
    df['normalized_brand'] = [result[0] for result in brand_model_results]
    df['normalized_model'] = [result[1] for result in brand_model_results]
    
    # 统计信息
    total_rows = len(df)  # 原始数量（包含所有数据）
    valid_sentences = len(df[df['is_valid'] == 1])  # 有效句子数量
    invalid_sentences = len(df[df['is_valid'] == 0])  # 无效句子数量
    
    # 只在有效句子中统计标签和品牌车型
    valid_df = df[df['is_valid'] == 1]
    valid_tags = valid_df['is_tag_in_predefined'].sum() if len(valid_df) > 0 else 0
    target_brands = len(valid_df[valid_df['normalized_brand'] != '其他']) if len(valid_df) > 0 else 0
    target_models = len(valid_df[valid_df['normalized_model'] != '其他']) if len(valid_df) > 0 else 0
    
    print("\n📈 统计结果:")
    print(f"  原始数据总数: {total_rows}")
    print(f"  有效句子数量: {valid_sentences} ({valid_sentences/total_rows*100:.1f}%)")
    print(f"  无效句子数量: {invalid_sentences} ({invalid_sentences/total_rows*100:.1f}%)")
    print()
    print(f"  有效句子中的统计:")
    if valid_sentences > 0:
        print(f"    有效标签数: {valid_tags} ({valid_tags/valid_sentences*100:.1f}%)")
        print(f"    目标品牌数: {target_brands} ({target_brands/valid_sentences*100:.1f}%)")
        print(f"    目标车型数: {target_models} ({target_models/valid_sentences*100:.1f}%)")
    else:
        print(f"    无有效句子进行分析")
    
    # 品牌分布统计（仅统计有效句子）
    if valid_sentences > 0:
        brand_counts = valid_df['normalized_brand'].value_counts()
        print(f"\n📊 有效句子中的品牌分布:")
        for brand, count in brand_counts.items():
            print(f"  {brand}: {count} ({count/valid_sentences*100:.1f}%)")
        
        # 车型分布统计（仅显示非'其他'的车型）
        model_counts = valid_df[valid_df['normalized_model'] != '其他']['normalized_model'].value_counts()
        if len(model_counts) > 0:
            print(f"\n🚗 有效句子中的车型分布:")
            for model, count in model_counts.items():
                print(f"  {model}: {count} ({count/valid_sentences*100:.1f}%)")
        else:
            print(f"\n🚗 有效句子中无目标车型数据")
    else:
        print(f"\n📊 无有效句子进行品牌车型分布统计")
    
    # 保存结果
    print(f"\n💾 保存结果到: {output_file}")
    
    # 确保输出目录存在
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    df.to_csv(output_file, index=False)
    
    print("✅ 处理完成！")
    
    # 显示一些示例结果（仅针对有效句子）
    print("\n📋 结果预览:")
    
    if valid_sentences > 0:
        # 显示有效标签的例子
        valid_tag_examples = valid_df[valid_df['is_tag_in_predefined'] == True]['tag'].dropna().head(3).tolist()
        if valid_tag_examples:
            print(f"  有效标签示例: {valid_tag_examples}")
        
        # 显示规范化品牌车型的例子
        target_examples = valid_df[valid_df['normalized_brand'] != '其他'][['brand', 'model', 'normalized_brand', 'normalized_model']].dropna().head(3)
        if len(target_examples) > 0:
            print("  品牌车型规范化示例:")
            for idx, row in target_examples.iterrows():
                print(f"    原始: {row['brand']}/{row['model']} -> 规范化: {row['normalized_brand']}/{row['normalized_model']}")
        
        # 显示无效标签的例子
        invalid_tag_examples = valid_df[valid_df['is_tag_in_predefined'] == False]['tag'].dropna().head(3).tolist()
        if invalid_tag_examples:
            print(f"  无效标签示例: {invalid_tag_examples}")
    else:
        print("  无有效句子可供预览")
    
    return df

def main():
    """主函数"""
    print("🎯 第4步：后处理分析")
    print("=" * 50)
    
    # 配置文件路径
    input_file = "data/results/境外汇总_20250609-cleaned-sentences-results.csv"
    output_file = "data/results/境外汇总_20250609_cleaned-sentences-results-processed.csv"
    
    # 检查输入文件是否存在
    if not os.path.exists(input_file):
        print(f"❌ 输入文件不存在: {input_file}")
        print("💡 请确保第3步的分析已经完成并生成了结果文件")
        return
    
    try:
        # 处理数据
        df = process_data(input_file, output_file)
        
        print(f"\n🎉 所有处理完成！")
        print(f"📁 输入文件: {input_file}")
        print(f"📁 输出文件: {output_file}")
        print(f"📊 新增列:")
        print(f"   - is_tag_in_predefined: 标签是否在预定义体系中")
        print(f"   - normalized_brand: 规范化品牌名称（Segway/Navee/小米/Kaabo/Dualtron/其他）")
        print(f"   - normalized_model: 规范化车型名称（ZT3 Pro/Max G2/S65C/4 Pro Max/Mantis 10/Mini/其他）")
        
    except Exception as e:
        print(f"❌ 处理失败: {e}")
        print("💡 请检查:")
        print("  1. 输入文件格式是否正确")
        print("  2. 文件是否被其他程序占用")
        print("  3. 磁盘空间是否充足")

if __name__ == "__main__":
    main() 