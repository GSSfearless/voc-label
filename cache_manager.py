#!/usr/bin/env python3
"""
LLM分析缓存管理工具

功能：
1. 查看缓存统计信息
2. 清理过期缓存
3. 清理全部缓存
4. 查看缓存详情

使用方法：
python cache_manager.py --action stats          # 查看缓存统计
python cache_manager.py --action clean_expired  # 清理过期缓存
python cache_manager.py --action clean_all      # 清理全部缓存
python cache_manager.py --action show_details   # 显示缓存详情
"""

import argparse
import json
import time
from pathlib import Path
from typing import Dict, Any
import hashlib

class CacheManager:
    """缓存管理器"""
    
    def __init__(self, cache_file: str = "data/cache/llm_analysis_cache.json"):
        self.cache_file = Path(cache_file)
        self.cache_data = {}
        self._load_cache()
    
    def _load_cache(self):
        """加载缓存数据"""
        if self.cache_file.exists():
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    self.cache_data = json.load(f)
                print(f"✅ 已加载缓存文件: {self.cache_file}")
            except Exception as e:
                print(f"❌ 加载缓存文件失败: {e}")
                self.cache_data = {}
        else:
            print(f"📂 缓存文件不存在: {self.cache_file}")
            self.cache_data = {}
    
    def _save_cache(self):
        """保存缓存数据"""
        try:
            # 确保目录存在
            self.cache_file.parent.mkdir(parents=True, exist_ok=True)
            
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.cache_data, f, ensure_ascii=False, indent=2)
            print(f"✅ 缓存已保存到: {self.cache_file}")
        except Exception as e:
            print(f"❌ 保存缓存文件失败: {e}")
    
    def get_stats(self, cache_ttl: int = None):
        """获取缓存统计信息"""
        if not self.cache_data:
            print("📊 缓存统计: 无缓存数据")
            return
        
        total_count = len(self.cache_data)
        current_time = time.time()
        expired_count = 0
        valid_count = 0
        
        oldest_time = float('inf')
        newest_time = 0
        
        for key, value in self.cache_data.items():
            timestamp = value.get('timestamp', 0)
            oldest_time = min(oldest_time, timestamp)
            newest_time = max(newest_time, timestamp)
            
            if cache_ttl and (current_time - timestamp) > cache_ttl:
                expired_count += 1
            else:
                valid_count += 1
        
        print("📊 缓存统计信息")
        print("-" * 40)
        print(f"总缓存条目: {total_count}")
        print(f"有效缓存: {valid_count}")
        if cache_ttl:
            print(f"过期缓存: {expired_count}")
        
        if oldest_time != float('inf'):
            oldest_date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(oldest_time))
            newest_date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(newest_time))
            print(f"最早缓存: {oldest_date}")
            print(f"最新缓存: {newest_date}")
        
        # 计算文件大小
        if self.cache_file.exists():
            file_size = self.cache_file.stat().st_size
            if file_size < 1024:
                size_str = f"{file_size} B"
            elif file_size < 1024 * 1024:
                size_str = f"{file_size / 1024:.1f} KB"
            else:
                size_str = f"{file_size / 1024 / 1024:.1f} MB"
            print(f"文件大小: {size_str}")
    
    def clean_expired(self, cache_ttl: int):
        """清理过期缓存"""
        if not self.cache_data:
            print("🧹 无缓存数据需要清理")
            return
        
        current_time = time.time()
        expired_keys = []
        
        for key, value in self.cache_data.items():
            timestamp = value.get('timestamp', 0)
            if (current_time - timestamp) > cache_ttl:
                expired_keys.append(key)
        
        if not expired_keys:
            print("🧹 没有发现过期缓存")
            return
        
        # 删除过期缓存
        for key in expired_keys:
            del self.cache_data[key]
        
        self._save_cache()
        print(f"🧹 已清理 {len(expired_keys)} 条过期缓存")
    
    def clean_all(self):
        """清理全部缓存"""
        if not self.cache_data:
            print("🧹 无缓存数据需要清理")
            return
        
        cache_count = len(self.cache_data)
        self.cache_data = {}
        self._save_cache()
        print(f"🧹 已清理全部 {cache_count} 条缓存")
    
    def show_details(self, limit: int = 10):
        """显示缓存详情"""
        if not self.cache_data:
            print("📋 无缓存数据")
            return
        
        print(f"📋 缓存详情 (显示前 {limit} 条)")
        print("-" * 80)
        
        sorted_items = sorted(
            self.cache_data.items(),
            key=lambda x: x[1].get('timestamp', 0),
            reverse=True
        )
        
        for i, (key, value) in enumerate(sorted_items[:limit]):
            timestamp = value.get('timestamp', 0)
            time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))
            
            result = value.get('result', '')
            if len(result) > 100:
                result_preview = result[:100] + "..."
            else:
                result_preview = result
            
            print(f"{i+1:2d}. 键值: {key[:16]}...")
            print(f"    时间: {time_str}")
            print(f"    结果: {result_preview}")
            print()

def main():
    parser = argparse.ArgumentParser(description="LLM分析缓存管理工具")
    parser.add_argument(
        "--action", 
        choices=['stats', 'clean_expired', 'clean_all', 'show_details'],
        required=True,
        help="要执行的操作"
    )
    parser.add_argument(
        "--cache-file", 
        default="data/cache/llm_analysis_cache.json",
        help="缓存文件路径"
    )
    parser.add_argument(
        "--cache-ttl", 
        type=int, 
        default=7*24*3600,
        help="缓存过期时间（秒），默认7天"
    )
    parser.add_argument(
        "--limit", 
        type=int, 
        default=10,
        help="显示详情时的条目限制"
    )
    
    args = parser.parse_args()
    
    # 创建缓存管理器
    cache_manager = CacheManager(args.cache_file)
    
    print("🔧 LLM分析缓存管理工具")
    print("=" * 50)
    
    if args.action == 'stats':
        cache_manager.get_stats(args.cache_ttl)
        
    elif args.action == 'clean_expired':
        cache_manager.clean_expired(args.cache_ttl)
        
    elif args.action == 'clean_all':
        confirm = input("⚠️  确认要清理全部缓存吗？(y/N): ")
        if confirm.lower() in ['y', 'yes']:
            cache_manager.clean_all()
        else:
            print("❌ 操作已取消")
            
    elif args.action == 'show_details':
        cache_manager.show_details(args.limit)

if __name__ == "__main__":
    main() 