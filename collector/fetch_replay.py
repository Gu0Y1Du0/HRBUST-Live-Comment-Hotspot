from bilibili_api import video, sync, Credential
import asyncio
import time
import json
import xml.etree.ElementTree as ET
from typing import AsyncIterator, Dict, Any, Optional


def _safe_int(x: str, default: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return default


def parse_bilibili_danmaku_xml_to_messages(
    xml_text: str, room_id: int | str, platform: str = "bilibili"
) -> list[dict]:
    """
    把B站弹幕XML解析成message
    通常不会有用户名，只有user_hash
    """
    root = ET.fromstring(xml_text)
    out: list[dict] = []

    for d in root.findall(".//d"):
        p = d.attrib.get("p", "")
        text = (d.text or "").strip()
        if not text:
            continue

        parts = p.split(",")

        # 常见 p 字段含义（大多数情况下）：
        # 0: 出现时间(秒) 1: 模式 2: 字号 3: 颜色 4: 发送时间戳(秒)
        # 5: pool 6: user_hash 7: row_id
        appear_s = float(parts[0]) if len(parts) > 0 else 0.0
        # 这里保留原始发送时间供参考，但在重放时主要用 appear_s
        send_ts_s = parts[4] if len(parts) > 4 else "0"
        user_hash = parts[6] if len(parts) > 6 else "unknown"

        # 原始的绝对时间戳 (如果是历史弹幕，这个时间是很久以前的)
        original_ts_ms = (
            _safe_int(send_ts_s) * 1000
            if _safe_int(send_ts_s) > 0
            else int(time.time() * 1000)
        )

        msg = {
            "platform": platform,
            "room_id": room_id,  # 这里通常填 BVID
            "user": {
                "id": user_hash,
                "name": None,
            },  # B站XML弹幕不包含用户名，只包含hash
            "content": text,  # 字段名改为 content，与Flink一致
            "event_type": "danmaku",
            "ts": original_ts_ms,  # 原始时间
            "video_time": appear_s,  # 新增字段：视频内的相对秒数 (重放系统的核心依赖)
        }
        out.append(msg)

    # 按照视频出现的相对时间排序，模拟直播流必须有序
    out.sort(key=lambda x: x["video_time"])
    return out


async def main():
    # bvid = "BV1SwknYnEms"
    bvid = "BV1CF9DYWEco"
    v = video.Video(bvid=bvid)

    print(f"正在获取视频 {bvid} 的信息...")

    # 获取视频分集信息 (CID)
    pages = await v.get_pages()
    if not pages:
        print("未找到分集信息")
        return

    # print(pages)

    # 通常取第一个分集 (正片)
    cid = pages[0]["cid"]
    page_name = pages[0]["page"]
    print(f"找到分集: {len(pages)}, CID: {cid}")

    # 获取弹幕 XML (Bilibili API 会自动拼接所有历史弹幕包，可能很大)
    print("正在下载并解析弹幕，可能需要几秒钟...")
    xml_text = await v.get_danmaku_xml(cid=cid)

    # 解析转换
    messages = parse_bilibili_danmaku_xml_to_messages(xml_text=xml_text, room_id=bvid)

    count = len(messages)
    print(f"解析完成！共获取到 {count} 条弹幕。")
    print(f"第一条: {messages[0]}")
    print(f"最后一条: {messages[-1]}")

    # 保存为 JSON 文件
    filename = "video_danmaku.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(messages, f, ensure_ascii=False, indent=2)

    print(f"数据已保存到本地文件: {filename}")


if __name__ == "__main__":
    asyncio.run(main())
