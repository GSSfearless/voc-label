#!/usr/bin/env python3
"""
快速运行句子分析脚本
修改输入文件路径后运行: python run_sentence_analysis.py
"""

import asyncio
import logging
from sentence_analysis import SentenceAnalyzer
from pathlib import Path

# 设置日志级别
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    # ========== 配置区域 ==========
    
    # 📁 输入文件配置
    input_file = "data/processed/境外汇总_20250609_cleaned.csv"  # 预处理结果文件
    
    # 📁 输出文件配置
    output_sentences = "data/processed/境外汇总_20250609_sentences.csv"      # 句子表
    output_summary = "data/processed/境外汇总_20250609_sentence_report.md"   # 分析报告
    output_samples = "data/processed/境外汇总_20250609_sentence_samples.csv" # 句子样本
    
    # 🎯 分析配置
    sample_size = 50  # 样本数量
    
    # ========== 执行分析 ==========
    
    print("🚀 开始句子切分结果分析...")
    print(f"📄 输入文件: {input_file}")
    print("=" * 50)
    
    if not Path(input_file).exists():
        print(f"❌ 输入文件不存在: {input_file}")
        print("请先运行预处理脚本生成结果文件")
        return
    
    try:
        # 创建分析器
        analyzer = SentenceAnalyzer(input_file)
        
        # 加载数据
        df = analyzer.load_data()
        
        # 检查是否有句子切分结果
        if 'sentences_detail' not in df.columns:
            print("❌ 输入文件中没有句子切分结果")
            print("请确认预处理时启用了句子切分功能 (split_sentences=True)")
            return
        
        # 提取句子
        print("正在提取句子...")
        sentences_df = analyzer.extract_sentences()
        
        if len(sentences_df) == 0:
            print("⚠️  没有找到有效的句子切分结果")
            return
        
        # 分析句子
        print("正在分析句子统计信息...")
        analysis = analyzer.analyze_sentences(sentences_df)
        
        # 创建输出目录
        for output_path in [output_sentences, output_summary, output_samples]:
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        
        # 导出结果
        print("正在导出结果...")
        analyzer.export_sentences(output_sentences, sentences_df)
        analyzer.export_summary(output_summary, analysis)
        
        # 生成样本
        sample_df = analyzer.generate_sample_sentences(sentences_df, sample_size)
        if len(sample_df) > 0:
            sample_df.to_csv(output_samples, index=False, encoding='utf-8-sig')
            print(f"句子样本已导出到: {output_samples}")
        
        # 显示分析结果
        print("\n" + "="*50)
        print("✅ 句子切分分析完成！")
        print(f"📊 基本统计:")
        print(f"   - 总句子数: {analysis['total_sentences']:,}")
        print(f"   - 原始文本数: {analysis['total_original_texts']:,}")
        print(f"   - 平均每条文本句数: {analysis['avg_sentences_per_text']:.2f}")
        
        print(f"📏 句子长度统计:")
        length_stats = analysis['sentence_length_stats']
        print(f"   - 平均长度: {length_stats['mean']:.1f} 字符")
        print(f"   - 中位数长度: {length_stats['median']:.1f} 字符")
        print(f"   - 长度范围: {length_stats['min']} - {length_stats['max']} 字符")
        
        print(f"📄 输出文件:")
        print(f"   - 句子表: {output_sentences}")
        print(f"   - 分析报告: {output_summary}")
        print(f"   - 句子样本: {output_samples}")
        
        # 语言分布
        if 'language_distribution' in analysis and analysis['language_distribution']:
            print(f"🌐 语言分布:")
            for lang, count in analysis['language_distribution'].items():
                percentage = (count / analysis['total_sentences']) * 100
                print(f"   - {lang}: {count:,} 句子 ({percentage:.1f}%)")
        
    except Exception as e:
        print(f"\n❌ 分析失败: {e}")
        logging.error(f"分析异常: {e}", exc_info=True)

if __name__ == "__main__":
    main()


# ========== 使用说明 ==========
"""
🔧 使用步骤：

1. 📋 确保已运行预处理脚本且启用了句子切分功能
2. ✏️  修改上面的输入文件路径
3. 🚀 运行: python run_sentence_analysis.py
4. 📊 查看生成的分析结果

📄 输出文件说明：
- sentences.csv: 包含所有切分后的句子，每行一个句子
- sentence_report.md: 详细的分析报告
- sentence_samples.csv: 按长度分层采样的句子样本

📊 句子表包含的列：
- original_id: 原始文本ID
- sentence_index: 句子在原文中的索引
- sentence_text: 句子内容
- sentence_start/end: 句子在原文中的位置
- sentence_lang: 句子语言
- original_text: 原始文本
- cleaned_text: 清洗后文本
- sentence_length: 句子长度
""" 