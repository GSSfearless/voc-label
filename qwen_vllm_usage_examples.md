# Qwen3:1.7B vLLM 调用脚本使用指南

## 脚本功能

`call_qwen_vllm.py` 是一个用于调用部署在 `36.103.199.82:8000` 的 Qwen3:1.7B vLLM 服务的脚本。

## 使用方法

### 1. 测试连接

首先建议测试与vLLM服务的连接是否正常：

```bash
python call_qwen_vllm.py --test
```

### 2. 单次调用

进行单次文本生成：

```bash
# 基本调用
python call_qwen_vllm.py --prompt "你好，请介绍一下你自己"

# 带系统提示词的调用
python call_qwen_vllm.py --prompt "分析这段文本的情感：今天天气真好" --system "你是一个情感分析专家"
```

### 3. 交互模式

进入聊天模式，可以连续对话：

```bash
python call_qwen_vllm.py --interactive
```

交互模式中的特殊命令：
- `system:你的系统提示词` - 设置系统提示词
- `quit` 或 `exit` - 退出交互模式

### 4. 批量处理CSV文件

批量处理CSV文件中的文本数据：

```bash
# 基本批量处理
python call_qwen_vllm.py --batch --input data.csv --column text_column

# 指定输出文件和并发数
python call_qwen_vllm.py --batch --input data.csv --column text_column --output results.csv --concurrent 10

# 带系统提示词的批量处理
python call_qwen_vllm.py --batch --input data.csv --column text_column --system "你是一个文本摘要专家，请为每段文本生成简洁的摘要"
```

## 配置选项

### API参数调整

如果需要调整API参数，可以修改脚本中的以下配置：

```python
# 在脚本开头修改
VLLM_BASE_URL = "http://36.103.199.82:8000"  # 服务地址
VLLM_MODEL = "Qwen3:1.7B"  # 模型名称

# 在调用时调整参数
await client.call_api(
    prompt=prompt,
    system_prompt=system_prompt,
    max_tokens=1024,     # 最大生成token数
    temperature=0.7,     # 温度参数 (0.0-2.0)
    top_p=0.9           # top-p采样参数 (0.0-1.0)
)
```

### 并发控制

在批量处理时，可以调整并发数以平衡速度和服务器负载：

```bash
# 低并发（更稳定）
python call_qwen_vllm.py --batch --input data.csv --column text --concurrent 2

# 高并发（更快速）
python call_qwen_vllm.py --batch --input data.csv --column text --concurrent 20
```

## 输出格式

### 单次调用输出

```
🤖 正在调用Qwen3:1.7B模型...
📝 输入: 你好，请介绍一下你自己
--------------------------------------------------
✅ 响应: 你好！我是通义千问，由阿里云开发的大型语言模型...
📊 使用统计: {'prompt_tokens': 15, 'completion_tokens': 87, 'total_tokens': 102}
```

### 批量处理输出

CSV文件会增加以下列：
- `qwen_response`: 模型生成的响应
- `qwen_success`: 是否成功 (True/False)
- `qwen_error`: 错误信息（如果失败）

## 常见问题

### 1. 连接失败

如果测试连接失败，请检查：
- vLLM服务是否正在运行
- 网络连接是否正常
- 服务地址和端口是否正确

### 2. 模型名称错误

根据实际部署的模型名称，可能需要调整 `VLLM_MODEL` 变量。常见的可能值：
- `"qwen"`
- `"Qwen/Qwen3-1.5B-Instruct"`
- 或者服务器配置的其他名称

### 3. 内存或性能问题

如果遇到性能问题，可以：
- 降低并发数 `--concurrent`
- 减少 `max_tokens`
- 调整 `temperature` 参数

## 开发和扩展

脚本采用模块化设计，主要组件：

- `QwenVLLMClient`: 核心API客户端类
- `single_call()`: 单次调用函数
- `batch_process_csv()`: 批量处理函数
- `interactive_mode()`: 交互模式函数
- `test_connection()`: 连接测试函数

可以根据需要扩展更多功能。 