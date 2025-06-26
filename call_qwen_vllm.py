#!/usr/bin/env python3
"""
调用Qwen3:1.7B vLLM服务的脚本
服务地址：36.103.199.82:8000

使用方法：
1. 单次调用：python call_qwen_vllm.py --prompt "你好，请介绍一下你自己"
2. 批量处理CSV文件：python call_qwen_vllm.py --batch --input data.csv --column text_column
3. 交互模式：python call_qwen_vllm.py --interactive

配置说明：
- 可以修改 VLLM_BASE_URL 来更改服务地址
- 可以调整 max_tokens、temperature 等参数
- 支持系统提示词和对话历史
"""

import aiohttp
import asyncio
import json
import pandas as pd
import argparse
import sys
from typing import List, Dict, Any, Optional
import logging
from pathlib import Path
import time

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# vLLM服务配置
VLLM_BASE_URL = "http://36.103.199.82:8000"
VLLM_MODEL = "Qwen3:1.7B"  # 模型名称，可能需要根据实际情况调整

class QwenVLLMClient:
    """Qwen vLLM客户端"""
    
    def __init__(self, base_url: str = VLLM_BASE_URL, model: str = VLLM_MODEL):
        self.base_url = base_url.rstrip('/')
        self.model = model
        self.session = None
        
    async def __aenter__(self):
        """异步上下文管理器入口"""
        timeout = aiohttp.ClientTimeout(total=120)
        self.session = aiohttp.ClientSession(timeout=timeout)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器退出"""
        if self.session:
            await self.session.close()
    
    async def call_api(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        max_tokens: int = 1024,
        temperature: float = 0.7,
        top_p: float = 0.9,
        stop: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        调用vLLM API
        
        Args:
            prompt: 用户输入的提示词
            system_prompt: 系统提示词
            max_tokens: 最大生成token数
            temperature: 温度参数，控制随机性
            top_p: top-p采样参数
            stop: 停止词列表
            
        Returns:
            API响应结果
        """
        
        # 构建消息列表
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})
        
        # 构建请求数据
        data = {
            "model": self.model,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "top_p": top_p,
            "stream": False  # 不使用流式输出
        }
        
        if stop:
            data["stop"] = stop
        
        try:
            async with self.session.post(
                f"{self.base_url}/v1/chat/completions",
                json=data,
                headers={"Content-Type": "application/json"}
            ) as response:
                
                if response.status == 200:
                    result = await response.json()
                    return {
                        "success": True,
                        "content": result["choices"][0]["message"]["content"],
                        "usage": result.get("usage", {}),
                        "response": result
                    }
                else:
                    error_text = await response.text()
                    logger.error(f"API调用失败: HTTP {response.status}, {error_text}")
                    return {
                        "success": False,
                        "error": f"HTTP {response.status}: {error_text}",
                        "content": ""
                    }
                    
        except asyncio.TimeoutError:
            logger.error("API调用超时")
            return {
                "success": False,
                "error": "请求超时",
                "content": ""
            }
        except Exception as e:
            logger.error(f"API调用异常: {e}")
            return {
                "success": False,
                "error": str(e),
                "content": ""
            }
    
    async def batch_process(
        self,
        texts: List[str],
        system_prompt: Optional[str] = None,
        max_concurrent: int = 5,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """
        批量处理文本列表
        
        Args:
            texts: 要处理的文本列表
            system_prompt: 系统提示词
            max_concurrent: 最大并发数
            **kwargs: 其他API参数
            
        Returns:
            处理结果列表
        """
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def process_one(text: str, index: int):
            async with semaphore:
                logger.info(f"处理第 {index + 1}/{len(texts)} 条文本")
                result = await self.call_api(text, system_prompt, **kwargs)
                result["index"] = index
                result["input_text"] = text
                return result
        
        tasks = [process_one(text, i) for i, text in enumerate(texts)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 处理异常结果
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"处理第 {i + 1} 条文本时发生异常: {result}")
                processed_results.append({
                    "success": False,
                    "error": str(result),
                    "content": "",
                    "index": i,
                    "input_text": texts[i] if i < len(texts) else ""
                })
            else:
                processed_results.append(result)
        
        return processed_results

async def single_call(prompt: str, system_prompt: Optional[str] = None):
    """单次调用示例"""
    async with QwenVLLMClient() as client:
        print(f"🤖 正在调用Qwen3:1.7B模型...")
        print(f"📝 输入: {prompt}")
        print("-" * 50)
        
        result = await client.call_api(
            prompt=prompt,
            system_prompt=system_prompt,
            max_tokens=1024,
            temperature=0.7
        )
        
        if result["success"]:
            print(f"✅ 响应: {result['content']}")
            if "usage" in result:
                usage = result["usage"]
                print(f"📊 使用统计: {usage}")
        else:
            print(f"❌ 调用失败: {result['error']}")

async def batch_process_csv(
    input_file: str,
    column_name: str,
    output_file: Optional[str] = None,
    system_prompt: Optional[str] = None,
    max_concurrent: int = 5
):
    """批量处理CSV文件"""
    
    # 读取CSV文件
    try:
        df = pd.read_csv(input_file)
        logger.info(f"成功读取CSV文件: {input_file}, 共 {len(df)} 行")
    except Exception as e:
        print(f"❌ 读取CSV文件失败: {e}")
        return
    
    if column_name not in df.columns:
        print(f"❌ 列 '{column_name}' 不存在于CSV文件中")
        print(f"可用列: {list(df.columns)}")
        return
    
    # 过滤掉空值
    valid_mask = df[column_name].notna() & (df[column_name] != "")
    texts = df[valid_mask][column_name].tolist()
    
    if not texts:
        print("❌ 没有找到有效的文本数据")
        return
    
    print(f"📊 准备处理 {len(texts)} 条文本")
    print(f"🚀 最大并发数: {max_concurrent}")
    
    # 批量处理
    async with QwenVLLMClient() as client:
        start_time = time.time()
        results = await client.batch_process(
            texts=texts,
            system_prompt=system_prompt,
            max_concurrent=max_concurrent
        )
        end_time = time.time()
        
        # 统计结果
        success_count = sum(1 for r in results if r["success"])
        failure_count = len(results) - success_count
        
        print(f"✅ 处理完成！")
        print(f"📊 成功: {success_count}, 失败: {failure_count}")
        print(f"⏱️ 耗时: {end_time - start_time:.2f} 秒")
        
        # 将结果添加到DataFrame
        df["qwen_response"] = ""
        df["qwen_success"] = False
        df["qwen_error"] = ""
        
        for result in results:
            idx = result["index"]
            original_idx = df[valid_mask].index[idx]
            df.loc[original_idx, "qwen_response"] = result["content"]
            df.loc[original_idx, "qwen_success"] = result["success"]
            if not result["success"]:
                df.loc[original_idx, "qwen_error"] = result["error"]
        
        # 保存结果
        if output_file is None:
            input_path = Path(input_file)
            output_file = input_path.stem + "_qwen_results" + input_path.suffix
        
        df.to_csv(output_file, index=False)
        print(f"📁 结果已保存到: {output_file}")
        
        # 显示前几个结果预览
        print("\n📋 结果预览:")
        for i, result in enumerate(results[:3]):
            if result["success"]:
                print(f"  {i+1}. 输入: {result['input_text'][:50]}...")
                print(f"     输出: {result['content'][:100]}...")
            else:
                print(f"  {i+1}. 错误: {result['error']}")

async def interactive_mode():
    """交互模式"""
    print("🤖 进入Qwen3:1.7B交互模式")
    print("输入 'quit' 或 'exit' 退出")
    print("输入 'system:你的系统提示词' 设置系统提示词")
    print("-" * 50)
    
    system_prompt = None
    
    async with QwenVLLMClient() as client:
        while True:
            try:
                user_input = input("\n💬 您: ").strip()
                
                if user_input.lower() in ['quit', 'exit', '退出']:
                    print("👋 再见！")
                    break
                
                if user_input.startswith('system:'):
                    system_prompt = user_input[7:].strip()
                    print(f"✅ 系统提示词已设置: {system_prompt}")
                    continue
                
                if not user_input:
                    continue
                
                print("🤖 Qwen: ", end="", flush=True)
                result = await client.call_api(
                    prompt=user_input,
                    system_prompt=system_prompt,
                    max_tokens=1024,
                    temperature=0.7
                )
                
                if result["success"]:
                    print(result["content"])
                else:
                    print(f"❌ 错误: {result['error']}")
                    
            except KeyboardInterrupt:
                print("\n👋 再见！")
                break
            except Exception as e:
                print(f"❌ 发生异常: {e}")

async def test_connection():
    """测试连接"""
    print("🔍 测试vLLM服务连接...")
    
    async with QwenVLLMClient() as client:
        result = await client.call_api(
            prompt="你好",
            max_tokens=50
        )
        
        if result["success"]:
            print("✅ 连接成功！")
            print(f"📤 测试输入: 你好")
            print(f"📥 测试输出: {result['content']}")
            return True
        else:
            print(f"❌ 连接失败: {result['error']}")
            return False

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="调用Qwen3:1.7B vLLM服务")
    parser.add_argument("--prompt", type=str, help="单次调用的提示词")
    parser.add_argument("--system", type=str, help="系统提示词")
    parser.add_argument("--batch", action="store_true", help="批量处理模式")
    parser.add_argument("--input", type=str, help="输入CSV文件路径")
    parser.add_argument("--column", type=str, help="要处理的列名")
    parser.add_argument("--output", type=str, help="输出文件路径")
    parser.add_argument("--concurrent", type=int, default=5, help="最大并发数")
    parser.add_argument("--interactive", action="store_true", help="交互模式")
    parser.add_argument("--test", action="store_true", help="测试连接")
    
    args = parser.parse_args()
    
    # 测试连接
    if args.test:
        asyncio.run(test_connection())
        return
    
    # 交互模式
    if args.interactive:
        asyncio.run(interactive_mode())
        return
    
    # 批量处理模式
    if args.batch:
        if not args.input or not args.column:
            print("❌ 批量处理模式需要指定 --input 和 --column 参数")
            return
        
        asyncio.run(batch_process_csv(
            input_file=args.input,
            column_name=args.column,
            output_file=args.output,
            system_prompt=args.system,
            max_concurrent=args.concurrent
        ))
        return
    
    # 单次调用模式
    if args.prompt:
        asyncio.run(single_call(args.prompt, args.system))
        return
    
    # 如果没有指定参数，显示使用说明
    print("🤖 Qwen3:1.7B vLLM调用脚本")
    print("=" * 50)
    print("使用示例:")
    print("1. 测试连接:")
    print("   python call_qwen_vllm.py --test")
    print("\n2. 单次调用:")
    print("   python call_qwen_vllm.py --prompt '请介绍一下量子计算'")
    print("\n3. 带系统提示词的调用:")
    print("   python call_qwen_vllm.py --prompt '分析这段文本的情感' --system '你是一个情感分析专家'")
    print("\n4. 批量处理CSV:")
    print("   python call_qwen_vllm.py --batch --input data.csv --column text_column")
    print("\n5. 交互模式:")
    print("   python call_qwen_vllm.py --interactive")
    print("\n6. 查看帮助:")
    print("   python call_qwen_vllm.py --help")

if __name__ == "__main__":
    main() 