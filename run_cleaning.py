import argparse
import pandas as pd
import logging
import sys
import os

# 确保可以从 src 目录导入模块
# 获取当前脚本文件所在的目录 (src/stage0)
current_dir = os.path.dirname(os.path.abspath(__file__))
# 获取 src 目录的路径 (src/stage0 的上级目录)
src_dir = os.path.dirname(current_dir)
# 将 src 目录添加到 Python 路径中
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

# 现在可以导入 data_cleaner 了
try:
    from data_cleaner import clean_social_media_data, clean_text # 导入需要的函数
except ImportError as e:
    print(f"Error importing cleaning functions: {e}")
    print(f"Make sure 'src/data/data_cleaner.py' exists and {src_dir} is in the Python path.")
    sys.exit(1)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout) # 输出到控制台
        # 可以添加 FileHandler 输出到文件
        # logging.FileHandler('cleaning.log')
    ]
)
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description="通用社交媒体数据清洗脚本")

    parser.add_argument("--input-file", required=True, help="输入的原始数据文件路径 (CSV格式)")
    parser.add_argument("--output-file", required=True, help="清洗后数据的输出文件路径 (CSV格式)")
    parser.add_argument("--text-col", default="text_content", help="包含主要文本内容的列名")
    parser.add_argument("--author-col", default="author", help="包含作者信息的列名 (如果不需要作者过滤，可以不提供或提供一个不存在的列名)")
    parser.add_argument("--min-length", type=int, default=3, help="清洗后文本的最小长度")
    parser.add_argument("--clean-weibo-tags", action="store_true", help="是否清洗微博标签 (#标签内容#)")

    args = parser.parse_args()

    # --- 在这里硬编码参数 ---
    author_blacklist = [
        # 九号常见型号: A2z, A1z, C25, C40, C65, C80, M95C, M65, NxxC系列, Qxx系列等
        '九号', 'ZEEHO','极核', '小牛'

        # 通用销售/服务/官方/媒体账号类 (适用于各类车企)
        '4S店', '专营店', '体验中心', '服务中心', '授权经销商', '经销商',
        '销售顾问', '销售', '客服', '官方客服', '小助手', '小秘书', '机器人',
        '置换', '金融', '车贷', '优惠',
        '车行', '车商', '汽车城',
        '试驾', '车展', '车市',
        '推广', '运营', '营销', '广告', '商务合作',
        '品牌号', '官方账号', '官方', '官博', '官微',
        '认证号', 'V认证', '蓝V认证', '企业认证',
        '媒体', '资讯', '快报', '头条', '播报', '汽车之家', '易车', '懂车帝',
        '工作室', '工作号', '小号',
        '抽奖', '福利', '活动主办', '官方活动'
    ]
    meaningless_patterns = [
        r"抽奖|福利", r"^(打卡|签到)", r"转发微博", r"视频新闻",
        # 建议补充的模式
        r"^[\\U0001F300-\\U0001FAD6\\s]+$",  # 纯表情/符号
        r"^[^\w\\u4e00-\\u9fa5]+$",        # 纯标点/特殊字符
        r"(优惠|折扣|特价|促销|秒杀)",      # 常见营销短语 (不含'福利', 已在上面)
        r"(点赞|关注|转发|评论区见)",      # 求赞/求关注类
        r"(点击链接|扫码|二维码|详情见|戳这里)", # 推广链接/二维码提示
        r"(http|https)://\\S+",           # 网址
        r"//@.*:| ^回复 @.*:",             # 微博转发/回复标记
        r"#.*?活动#",                      # 特定活动标签
        r"^(好的|收到|嗯嗯|哈哈|嘻嘻|ooo)$", # 非常短的、无意义的回复
        r"\\\[图片\\\]|\\\[视频\\\]|\\\[链接\\\]",  # 系统提示 (注意义)
        r"CALL"
    ]

    logger.info("开始数据清洗流程...")
    logger.info(f"输入文件: {args.input_file}")
    logger.info(f"输出文件: {args.output_file}")
    logger.info(f"文本列: {args.text_col}")
    logger.info(f"作者列: {args.author_col}")
    logger.info(f"作者黑名单 (硬编码): {author_blacklist}") # Added log for hardcoded value
    logger.info(f"无意义内容模式 (硬编码): {meaningless_patterns}") # Added log for hardcoded value
    logger.info(f"最小文本长度: {args.min_length}")
    logger.info(f"清洗微博标签: {args.clean_weibo_tags}")

    # --- 数据加载 ---
    try:
        df = pd.read_csv(args.input_file)
        logger.info(f"成功加载数据，共 {len(df)} 行")
    except FileNotFoundError:
        logger.error(f"错误：输入文件未找到 {args.input_file}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"加载数据时出错: {e}")
        sys.exit(1)

    # --- 数据清洗 ---
    # 注意：确保 data_cleaner.py 中的 clean_social_media_data 函数存在且签名匹配
    try:
        cleaned_df = clean_social_media_data(
            df,
            text_col=args.text_col,
            author_col=args.author_col,
            author_blacklist=author_blacklist, # 使用硬编码变量
            meaningless_patterns=meaningless_patterns, # 使用硬编码变量
            min_text_length=args.min_length,
            clean_weibo_tags=args.clean_weibo_tags
        )
    except Exception as e:
        logger.error(f"数据清洗过程中发生错误: {e}")
        # 可以在这里打印更详细的 traceback
        import traceback
        traceback.print_exc()
        sys.exit(1)

    # --- 生成并打印清洗报告 ---
    logger.info("--- 清洗报告 ---")
    total_rows = len(cleaned_df)
    # 计算无效行数 (is_valid == False)
    invalid_rows_count = (~cleaned_df['is_valid']).sum()
    valid_rows_count = total_rows - invalid_rows_count

    logger.info(f"总处理行数: {total_rows}")
    logger.info(f"有效行数 (is_valid=True): {valid_rows_count}")
    logger.info(f"无效行数 (is_valid=False): {invalid_rows_count}")

    if invalid_rows_count > 0:
        logger.info("标记无效原因分布:")
        # 计算每种无效原因的数量
        reason_counts = cleaned_df[~cleaned_df['is_valid']]['invalidation_reason'].value_counts()
        for reason, count in reason_counts.items():
            logger.info(f"  - {reason}: {count} 行")
    logger.info("--- 报告结束 ---")

    # --- 保存结果 ---
    try:
        # 确保输出目录存在
        output_dir = os.path.dirname(args.output_file)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
            logger.info(f"已创建输出目录: {output_dir}")

        # 保存包含所有原始行和新增标记列的完整 DataFrame
        cleaned_df.to_csv(args.output_file, index=False, encoding='utf-8-sig') # 使用 utf-8-sig 避免 Excel 打开乱码
        logger.info(f"清洗完成，包含标记列的结果已保存到 {args.output_file}，总行数: {len(cleaned_df)}")
    except Exception as e:
        logger.error(f"保存结果时出错: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 