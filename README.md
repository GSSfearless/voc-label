# 批量LLM API调用脚本

这是一个用于批量并发调用大模型API的Python脚本，支持读取CSV数据、并发处理、JSON结构解析和结果输出。

## 功能特性

- ✅ **批量处理**: 读取CSV文件中的数据进行批量处理
- ✅ **并发调用**: 支持异步并发调用，提高处理效率
- ✅ **多重重试**: 自动重试失败的请求，提高成功率
- ✅ **JSON解析**: 智能解析模型返回的JSON结构
- ✅ **灵活配置**: 支持自定义prompt模板和输出字段
- ✅ **错误处理**: 完善的错误处理和日志记录
- ✅ **结果合并**: 将处理结果与原始数据合并输出
- 🆕 **智能缓存**: 基于内容哈希的缓存机制，避免重复API调用，显著节省成本

## 安装依赖

```bash
pip install -r requirements.txt
```

## 快速开始

### 1. 基本配置

```python
from batch_llm_api import APIConfig, ProcessConfig, LLMBatchProcessor
import asyncio

# API配置
api_config = APIConfig(
    api_key="your-openrouter-api-key",  # 您的API密钥
    model="openai/gpt-4o",               # 模型名称
    max_concurrent=5,                    # 最大并发数
    timeout=30,                          # 超时时间(秒)
    retry_attempts=3                     # 重试次数
)

# 处理配置
process_config = ProcessConfig(
    input_csv="input.csv",               # 输入CSV文件
    output_csv="output.csv",             # 输出CSV文件
    input_column="text",                 # 要处理的列名
    prompt_template="分析以下文本: {input_text}",  # prompt模板
    output_json_fields=["sentiment", "confidence"],  # 要提取的JSON字段
    max_rows=None                        # 限制处理行数(可选)
)
```

### 2. 执行处理

```python
async def main():
    async with LLMBatchProcessor(api_config, process_config) as processor:
        result_df = await processor.process_batch()
        processor.save_results(result_df)

if __name__ == "__main__":
    asyncio.run(main())
```

## 详细使用说明

### API配置参数

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `base_url` | str | "https://openrouter.ai/api/v1" | API基础URL |
| `api_key` | str | "" | API密钥 |
| `model` | str | "openai/gpt-4o" | 模型名称 |
| `max_concurrent` | int | 10 | 最大并发请求数 |
| `timeout` | int | 30 | 请求超时时间(秒) |
| `retry_attempts` | int | 3 | 重试次数 |
| `retry_delay` | int | 1 | 重试延迟(秒) |
| `system_prompt` | Optional[str] | None | 系统提示词，用于设定AI行为 |
| `enable_cache` | bool | True | 是否启用缓存功能 |
| `cache_file` | str | "llm_cache.json" | 缓存文件路径 |
| `cache_ttl` | Optional[int] | None | 缓存过期时间(秒)，None表示永不过期 |

### 处理配置参数

| 参数 | 类型 | 说明 |
|------|------|------|
| `input_csv` | str | 输入CSV文件路径 |
| `output_csv` | str | 输出CSV文件路径 |
| `input_column` | str | 要处理的列名 |
| `prompt_template` | str | prompt模板，使用{input_text}占位符 |
| `output_json_fields` | List[str] | 要从JSON响应中提取的字段列表 |
| `max_rows` | Optional[int] | 限制处理的行数，None表示处理全部 |

### Prompt模板编写

prompt模板中使用`{input_text}`作为占位符，脚本会自动替换为CSV中对应列的值：

```python
prompt_template = """请分析以下文本的情感：

文本内容：{input_text}

请返回JSON格式：
{{
    "sentiment": "positive/negative/neutral",
    "confidence": 0.95,
    "keywords": ["关键词1", "关键词2"]
}}"""
```

### System Prompt配置

系统提示词用于设定AI的行为模式和输出格式，是提高结果一致性的重要工具：

```python
api_config = APIConfig(
    system_prompt="""你是一个专业的文本分析师。
要求：
1. 严格按照JSON格式返回结果
2. 分析要客观准确
3. 提供有价值的关键词"""
)
```

**System Prompt最佳实践：**
- 明确定义AI的角色和专业领域
- 具体说明输出格式要求
- 设定分析的标准和原则
- 提供必要的背景信息

### JSON响应解析

脚本支持多种JSON格式的自动解析：

1. **直接JSON**: 直接返回JSON对象
2. **代码块格式**: 包含在\`\`\`json代码块中的JSON
3. **嵌入式JSON**: 文本中包含的JSON对象

如果解析失败，会保留原始响应内容。

## 使用示例

### 示例1: 情感分析

```python
# 参见 config_example.py 中的 sentiment_analysis_example()
```

### 示例2: 文本分类

```python
# 参见 config_example.py 中的 text_classification_example()
```

### 示例3: 内容提取

```python
# 参见 config_example.py 中的 content_extraction_example()
```

## 输入CSV格式

输入CSV文件应包含要处理的文本列，例如：

```csv
id,text,category
1,"这是一个很好的产品",product_review
2,"服务质量有待提高",service_review
3,"总体来说还不错",general_review
```

## 输出CSV格式

输出CSV会包含原始数据以及处理结果：

```csv
row_index,id,text,category,sentiment,confidence,keywords,raw_response,parsing_success
0,1,"这是一个很好的产品",product_review,positive,0.95,"[""产品"", ""好""]","{""sentiment"": ""positive""...}",True
1,2,"服务质量有待提高",service_review,negative,0.80,"[""服务"", ""质量""]","{""sentiment"": ""negative""...}",True
```

## 性能优化建议

1. **合理设置并发数**: 根据API限制调整`max_concurrent`参数
2. **选择合适的模型**: 根据任务复杂度选择性价比最高的模型
3. **批量处理**: 对于大量数据，可以分批处理
4. **监控日志**: 关注日志输出，及时发现和解决问题

## 错误处理

脚本内置了完善的错误处理机制：

- **网络错误**: 自动重试失败的请求
- **API限制**: 通过并发控制避免超出API限制
- **解析错误**: JSON解析失败时保留原始内容
- **数据错误**: 详细记录处理失败的行

## 注意事项

1. 确保API密钥有效且有足够的配额
2. 根据API提供商的限制调整并发数和请求频率
3. 大批量处理时建议先小规模测试
4. 定期检查输出结果的质量
5. 注意保护敏感数据和API密钥

## 缓存功能

### 缓存机制

脚本内置了智能缓存功能，可以显著节省API调用成本：

1. **自动缓存**: 成功的API调用结果会自动缓存
2. **内容识别**: 基于文本内容和系统提示词的MD5哈希值作为缓存键
3. **过期管理**: 支持设置缓存过期时间，自动清理过期缓存
4. **成本节省**: 相同内容的重复调用直接返回缓存结果

### 缓存配置

```python
api_config = APIConfig(
    api_key="your-api-key",
    enable_cache=True,                           # 启用缓存
    cache_file="data/cache/llm_cache.json",      # 缓存文件路径
    cache_ttl=7*24*3600,                         # 缓存7天过期
)
```

### 缓存管理工具

提供了专门的缓存管理脚本 `cache_manager.py`：

```bash
# 查看缓存统计
python cache_manager.py --action stats

# 清理过期缓存
python cache_manager.py --action clean_expired

# 清理全部缓存
python cache_manager.py --action clean_all

# 查看缓存详情
python cache_manager.py --action show_details
```

### 缓存效果

- 🎯 **缓存命中率**: 处理完成后会显示缓存命中率和节省的API调用次数
- 💰 **成本节省**: 重复处理相同内容时，缓存命中率可达90%以上
- ⚡ **性能提升**: 缓存命中的请求响应时间接近0

### 缓存注意事项

1. **缓存键生成**: 基于输入文本+系统提示词的组合，确保内容完全相同才会命中
2. **缓存文件**: 缓存保存在JSON文件中，可以跨运行会话保持
3. **过期清理**: 定期运行缓存清理工具，避免缓存文件过大
4. **禁用缓存**: 如需每次都调用API获取最新结果，可设置 `enable_cache=False`

## 扩展功能

脚本支持以下扩展：

- 支持不同的API提供商（OpenAI、Anthropic等）
- 自定义请求头和参数
- 结果后处理和验证
- 数据预处理功能

## 故障排除

常见问题及解决方案：

1. **API密钥错误**: 检查密钥是否正确设置
2. **超时错误**: 增加timeout参数值
3. **并发过高**: 降低max_concurrent参数
4. **JSON解析失败**: 检查prompt模板是否明确要求JSON格式
5. **文件读取错误**: 确认CSV文件路径和格式正确

如有其他问题，请查看日志输出获取详细错误信息。 