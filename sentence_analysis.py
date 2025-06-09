#!/usr/bin/env python3
"""
句子切分结果分析脚本
用于分析预处理后的句子切分结果，并导出为独立的句子表
"""

import pandas as pd
import json
import argparse
import logging
from pathlib import Path
from typing import List, Dict, Any

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SentenceAnalyzer:
    """句子切分结果分析器"""
    
    def __init__(self, input_file: str):
        self.input_file = input_file
        self.df = None
    
    def load_data(self) -> pd.DataFrame:
        """加载预处理结果文件"""
        try:
            self.df = pd.read_csv(self.input_file)
            logger.info(f"成功加载数据，共 {len(self.df)} 行")
            return self.df
        except Exception as e:
            logger.error(f"加载数据失败: {e}")
            raise
    
    def extract_sentences(self) -> pd.DataFrame:
        """提取所有句子，生成句子表"""
        if self.df is None:
            self.load_data()
        
        sentences_data = []
        
        for idx, row in self.df.iterrows():
            if not row.get('processing_success', False):
                continue
            
            # 获取原始数据
            original_id = row.get('序号', idx)  # 使用序号或索引作为原始ID
            original_text = row.get('正文', '')
            cleaned_text = row.get('cleaned_text', '')
            
            # 解析句子详情
            sentences_detail = row.get('sentences_detail', '')
            if not sentences_detail:
                continue
            
            try:
                sentences = json.loads(sentences_detail)
                
                for i, sentence in enumerate(sentences):
                    sentences_data.append({
                        'original_id': original_id,
                        'sentence_index': i,
                        'sentence_text': sentence.get('text', ''),
                        'sentence_start': sentence.get('start', 0),
                        'sentence_end': sentence.get('end', 0),
                        'sentence_lang': sentence.get('lang', ''),
                        'original_text': original_text,
                        'cleaned_text': cleaned_text,
                        'original_length': len(original_text),
                        'sentence_length': len(sentence.get('text', '')),
                    })
            except json.JSONDecodeError as e:
                logger.warning(f"解析句子详情失败 (行 {idx}): {e}")
                continue
        
        sentences_df = pd.DataFrame(sentences_data)
        logger.info(f"提取到 {len(sentences_df)} 个句子")
        return sentences_df
    
    def analyze_sentences(self, sentences_df: pd.DataFrame) -> Dict[str, Any]:
        """分析句子统计信息"""
        analysis = {}
        
        # 基本统计
        analysis['total_sentences'] = len(sentences_df)
        analysis['total_original_texts'] = sentences_df['original_id'].nunique()
        analysis['avg_sentences_per_text'] = len(sentences_df) / analysis['total_original_texts']
        
        # 句子长度统计
        analysis['sentence_length_stats'] = {
            'mean': sentences_df['sentence_length'].mean(),
            'median': sentences_df['sentence_length'].median(),
            'min': sentences_df['sentence_length'].min(),
            'max': sentences_df['sentence_length'].max(),
            'std': sentences_df['sentence_length'].std()
        }
        
        # 语言分布
        if 'sentence_lang' in sentences_df.columns:
            lang_dist = sentences_df['sentence_lang'].value_counts().to_dict()
            analysis['language_distribution'] = lang_dist
        
        # 句子数量分布
        sentences_per_text = sentences_df.groupby('original_id').size()
        analysis['sentences_per_text_stats'] = {
            'mean': sentences_per_text.mean(),
            'median': sentences_per_text.median(),
            'min': sentences_per_text.min(),
            'max': sentences_per_text.max(),
            'std': sentences_per_text.std()
        }
        
        return analysis
    
    def export_sentences(self, output_file: str, sentences_df: pd.DataFrame = None):
        """导出句子表"""
        if sentences_df is None:
            sentences_df = self.extract_sentences()
        
        try:
            sentences_df.to_csv(output_file, index=False, encoding='utf-8-sig')
            logger.info(f"句子表已导出到: {output_file}")
        except Exception as e:
            logger.error(f"导出句子表失败: {e}")
            raise
    
    def export_summary(self, output_file: str, analysis: Dict[str, Any]):
        """导出分析摘要"""
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write("# 句子切分分析报告\n\n")
                
                # 基本统计
                f.write("## 基本统计\n")
                f.write(f"- 总句子数: {analysis['total_sentences']}\n")
                f.write(f"- 原始文本数: {analysis['total_original_texts']}\n")
                f.write(f"- 平均每条文本句数: {analysis['avg_sentences_per_text']:.2f}\n\n")
                
                # 句子长度统计
                f.write("## 句子长度统计\n")
                length_stats = analysis['sentence_length_stats']
                f.write(f"- 平均长度: {length_stats['mean']:.1f} 字符\n")
                f.write(f"- 中位数长度: {length_stats['median']:.1f} 字符\n")
                f.write(f"- 最短句子: {length_stats['min']} 字符\n")
                f.write(f"- 最长句子: {length_stats['max']} 字符\n")
                f.write(f"- 标准差: {length_stats['std']:.1f}\n\n")
                
                # 每条文本的句子数量统计
                f.write("## 每条文本句子数量统计\n")
                sent_stats = analysis['sentences_per_text_stats']
                f.write(f"- 平均句子数: {sent_stats['mean']:.1f}\n")
                f.write(f"- 中位数句子数: {sent_stats['median']:.1f}\n")
                f.write(f"- 最少句子数: {sent_stats['min']}\n")
                f.write(f"- 最多句子数: {sent_stats['max']}\n")
                f.write(f"- 标准差: {sent_stats['std']:.1f}\n\n")
                
                # 语言分布
                if 'language_distribution' in analysis:
                    f.write("## 语言分布\n")
                    for lang, count in analysis['language_distribution'].items():
                        f.write(f"- {lang}: {count} 句子\n")
                    f.write("\n")
            
            logger.info(f"分析摘要已导出到: {output_file}")
        except Exception as e:
            logger.error(f"导出分析摘要失败: {e}")
            raise
    
    def generate_sample_sentences(self, sentences_df: pd.DataFrame, num_samples: int = 10) -> pd.DataFrame:
        """生成句子样本"""
        # 按长度分层采样
        length_bins = pd.qcut(sentences_df['sentence_length'], q=4, labels=['短', '中短', '中长', '长'])
        samples = []
        
        for bin_name in length_bins.cat.categories:
            bin_data = sentences_df[length_bins == bin_name]
            if len(bin_data) > 0:
                sample_size = min(num_samples // 4, len(bin_data))
                samples.append(bin_data.sample(n=sample_size))
        
        sample_df = pd.concat(samples, ignore_index=True) if samples else pd.DataFrame()
        return sample_df

def main():
    parser = argparse.ArgumentParser(description='句子切分结果分析')
    parser.add_argument('input_file', help='预处理结果CSV文件路径')
    parser.add_argument('--output-sentences', '-s', default='sentences.csv', help='句子表输出文件')
    parser.add_argument('--output-summary', '-r', default='sentence_analysis_report.md', help='分析报告输出文件')
    parser.add_argument('--output-samples', default='sentence_samples.csv', help='句子样本输出文件')
    parser.add_argument('--sample-size', type=int, default=20, help='样本数量')
    
    args = parser.parse_args()
    
    if not Path(args.input_file).exists():
        logger.error(f"输入文件不存在: {args.input_file}")
        return
    
    try:
        # 创建分析器
        analyzer = SentenceAnalyzer(args.input_file)
        
        # 提取句子
        logger.info("正在提取句子...")
        sentences_df = analyzer.extract_sentences()
        
        if len(sentences_df) == 0:
            logger.warning("没有找到句子切分结果")
            return
        
        # 分析句子
        logger.info("正在分析句子统计信息...")
        analysis = analyzer.analyze_sentences(sentences_df)
        
        # 导出结果
        logger.info("正在导出结果...")
        analyzer.export_sentences(args.output_sentences, sentences_df)
        analyzer.export_summary(args.output_summary, analysis)
        
        # 生成样本
        sample_df = analyzer.generate_sample_sentences(sentences_df, args.sample_size)
        if len(sample_df) > 0:
            sample_df.to_csv(args.output_samples, index=False, encoding='utf-8-sig')
            logger.info(f"句子样本已导出到: {args.output_samples}")
        
        # 显示摘要
        print("\n" + "="*50)
        print("📊 句子切分分析完成")
        print(f"📝 总句子数: {analysis['total_sentences']}")
        print(f"📄 原始文本数: {analysis['total_original_texts']}")
        print(f"📏 平均每条文本句数: {analysis['avg_sentences_per_text']:.2f}")
        print(f"📏 句子平均长度: {analysis['sentence_length_stats']['mean']:.1f} 字符")
        print(f"📄 结果已导出到:")
        print(f"   - 句子表: {args.output_sentences}")
        print(f"   - 分析报告: {args.output_summary}")
        print(f"   - 句子样本: {args.output_samples}")
        
    except Exception as e:
        logger.error(f"分析失败: {e}")

if __name__ == "__main__":
    main() 