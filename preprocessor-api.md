# VOC NLP 预处理服务 API 文档

版本: v1.0.0  
基础路径: `/v1/nlp`  
服务端口: `8001`  

## 通用说明

### 请求头

所有请求都应包含以下HTTP头：

```http
Content-Type: application/json
Accept: application/json
User-Agent: {client-name}/{version}
```

### 统一响应格式

所有接口都遵循统一的响应格式：

```json
{
  "code": 200,
  "message": "success",
  "data": {},
  "request_id": "uuid4",
  "timestamp": "2024-01-15T10:30:00Z",
  "processing_time_ms": 45
}
```

### 响应字段说明

| 字段 | 类型 | 说明 |
|------|------|------|
| `code` | int | HTTP状态码 |
| `message` | string | 响应消息 |
| `data` | object | 响应数据 |
| `request_id` | string | 请求唯一标识 |
| `timestamp` | string | 响应时间戳 (ISO8601) |
| `processing_time_ms` | int | 处理时间（毫秒） |

## 文本处理接口

### 1. 完整文本预处理

**接口路径**: `POST /v1/nlp/preprocess`

**功能描述**: 提供完整的文本预处理功能，包括清洗、PII脱敏、Emoji转换、社交媒体清洗等。

#### 请求参数

```json
{
  "id": "string (可选)",
  "text": "string (必填)",
  "author": "string (可选)",
  "options": {
    "remove_pii": "boolean",
    "emoji_convert": "boolean", 
    "emoji_remove": "boolean",
    "remove_social_mentions": "boolean",
    "remove_weibo_reposts": "boolean",
    "remove_hashtags": "boolean",
    "enable_author_blacklist": "boolean",
    "remove_ads": "boolean",
    "remove_urls": "boolean",
    "normalize_whitespace": "boolean",
    "normalize_unicode": "boolean",
    "convert_fullwidth": "boolean",
    "detect_language": "boolean",
    "split_sentences": "boolean",
    "max_length": "integer",
    "min_length": "integer"
  }
}
```

#### 参数详细说明

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| `id` | string | 否 | null | 消息ID，用于跟踪请求 |
| `text` | string | 是 | - | 待处理的文本内容 (1-10000字符) |
| `author` | string | 否 | null | 内容作者名称，用于黑名单过滤 |
| `options.remove_pii` | boolean | 否 | true | 是否移除PII信息 |
| `options.emoji_convert` | boolean | 否 | true | 是否转换Emoji为文本 |
| `options.emoji_remove` | boolean | 否 | false | 是否直接移除Emoji |
| `options.remove_social_mentions` | boolean | 否 | false | 是否清理@提及 |
| `options.remove_weibo_reposts` | boolean | 否 | false | 是否清理微博转发// |
| `options.remove_hashtags` | boolean | 否 | false | 是否清理话题标签# |
| `options.enable_author_blacklist` | boolean | 否 | false | 是否启用作者黑名单 |
| `options.remove_ads` | boolean | 否 | true | 是否移除广告信息 |
| `options.remove_urls` | boolean | 否 | true | 是否移除URL链接 |
| `options.normalize_whitespace` | boolean | 否 | true | 是否规范化空白字符 |
| `options.normalize_unicode` | boolean | 否 | true | 是否进行Unicode规范化 |
| `options.convert_fullwidth` | boolean | 否 | true | 是否全角转半角 |
| `options.detect_language` | boolean | 否 | false | 是否进行语言检测 |
| `options.split_sentences` | boolean | 否 | false | 是否进行句子切分 |
| `options.max_length` | integer | 否 | 10000 | 最大文本长度 |
| `options.min_length` | integer | 否 | 1 | 最小文本长度 |

#### 请求示例

```json
{
  "id": "msg_123456",
  "text": "@DOU+上热门 @抖音小助手 忘记一个不喜欢我的人很难吗 #汽车# 我的手机号是13812345678😍",
  "author": "某个营销号",
  "options": {
    "remove_pii": true,
    "emoji_convert": true,
    "remove_social_mentions": true,
    "remove_hashtags": true,
    "enable_author_blacklist": true,
    "detect_language": true,
    "split_sentences": true
  }
}
```

#### 响应示例

```json
{
  "code": 200,
  "message": "success",
  "data": {
    "id": "msg_123456",
    "author": "某个营销号",
    "original_text": "@DOU+上热门 @抖音小助手 忘记一个不喜欢我的人很难吗 #汽车# 我的手机号是13812345678😍",
    "cleaned_text": "忘记一个不喜欢我的人很难吗 我的手机号是138****5678[开心表情]",
    "removed_elements": {
      "pii_count": 1,
      "emoji_count": 1,
      "mentions_removed": 2,
      "reposts_removed": 0,
      "hashtags_removed": 1,
      "blacklist_triggered": true,
      "ad_phrases": [],
      "control_chars": 0,
      "spam_issues": []
    },
    "language_detection": {
      "languages": [
        {
          "lang": "zh",
          "confidence": 0.95,
          "script": "Han"
        }
      ],
      "primary_lang": "zh",
      "is_multilingual": false
    },
    "sentence_splitting": {
      "sentences": [
        {
          "index": 0,
          "text": "忘记一个不喜欢我的人很难吗",
          "start": 0,
          "end": 14,
          "lang": "zh"
        },
        {
          "index": 1,
          "text": "我的手机号是138****5678[开心表情]",
          "start": 15,
          "end": 35,
          "lang": "zh"
        }
      ],
      "total_sentences": 2,
      "language": "zh"
    },
    "statistics": {
      "original_length": 58,
      "cleaned_length": 35,
      "char_removed": 23,
      "processing_time_ms": 15
    },
    "warnings": [
      "作者 '某个营销号' 触发黑名单关键词: 营销号"
    ],
    "processing_metadata": {
      "service": "preprocessor",
      "version": "0.1.0",
      "processing_time_ms": 15
    }
  },
  "request_id": "req_789012",
  "timestamp": "2024-01-15T10:30:00Z",
  "processing_time_ms": 15
}
```

#### 响应数据字段说明

| 字段 | 类型 | 说明 |
|------|------|------|
| `original_text` | string | 原始文本 |
| `cleaned_text` | string | 清洗后的文本 |
| `removed_elements` | object | 移除元素统计 |
| `removed_elements.pii_count` | integer | 脱敏的PII信息数量 |
| `removed_elements.emoji_count` | integer | 处理的Emoji数量 |
| `removed_elements.mentions_removed` | integer | 清理的@提及数量 |
| `removed_elements.reposts_removed` | integer | 清理的转发内容数量 |
| `removed_elements.hashtags_removed` | integer | 清理的话题标签数量 |
| `removed_elements.blacklist_triggered` | boolean | 是否触发作者黑名单 |
| `language_detection` | object | 语言检测结果（当启用时） |
| `language_detection.languages` | array | 检测到的语言列表 |
| `language_detection.primary_lang` | string | 主要语言代码 |
| `language_detection.is_multilingual` | boolean | 是否多语言文本 |
| `sentence_splitting` | object | 句子切分结果（当启用时） |
| `sentence_splitting.sentences` | array | 切分后的句子列表 |
| `sentence_splitting.total_sentences` | integer | 总句子数 |
| `sentence_splitting.language` | string | 整体文本的主要语言 |
| `statistics` | object | 处理统计信息 |
| `statistics.original_length` | integer | 原始文本长度 |
| `statistics.cleaned_length` | integer | 清洗后文本长度 |
| `statistics.char_removed` | integer | 移除的字符数 |
| `warnings` | array | 警告信息列表 |
| `processing_metadata` | object | 处理元数据 |