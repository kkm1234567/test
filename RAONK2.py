import base64
import json
import re
from urllib.parse import urlparse, parse_qs, unquote


def _b64decode_with_padding(b64: str) -> bytes:
    # URL에 들어있는 base64는 padding(=)이 빠지는 경우가 많아서 보정
    b64 = b64.strip()
    b64 += "=" * (-len(b64) % 4)
    return base64.b64decode(b64)


def _to_printable(s: str) -> str:
    r"""
    사람이 보기 편하게 제어문자들을 \xNN 형태로 치환.
    """
    out = []
    for ch in s:
        o = ord(ch)
        if 32 <= o <= 126:
            out.append(ch)
        elif ch in ("\n", "\r", "\t"):
            out.append({"\\n": "\n", "\\r": "\r", "\\t": "\t"}[repr(ch)[1:-1]])
        else:
            out.append(f"\\x{o:02x}")
    return "".join(out)


def _parse_raonk_blob(raw: bytes) -> dict:
    r"""
    RAONK 토큰(blob) 내부에서
      k<digits> + 0x0C(FF) + value
    패턴을 최대한 뽑아낸다.
    """
    text = raw.decode("latin1", errors="replace")  # 바이트 보존용
    # key-value: k31\x0c<value>  (value는 다음 구분자(\x0b / \n / \r / \x00 등) 전까지)
    kv_pattern = re.compile(r"(?:^|[\x0b\r\n])(?P<k>k\d+)\x0c(?P<v>[^\r\n\x00]*)")

    fields = []
    for m in kv_pattern.finditer(text):
        k = m.group("k")
        v = m.group("v")
        fields.append({"key": k, "value": v})

    # key별로도 바로 접근 가능하게
    by_key = {}
    for item in fields:
        by_key.setdefault(item["key"], []).append(item["value"])

    # 사람이 보기 좋은 별칭(너가 관측한 패턴 기준)
    alias = {
        "k31": "filename",
        "k30": "path",
        "k21": "customValue",
        "k22": "token_or_hash",   # 32 hex로 보이는 값이 자주 여기 들어있음
        "k28": "maybe_index_or_flag",
    }

    human = {}
    for k, vs in by_key.items():
        name = alias.get(k, k)
        human[name] = vs[0] if len(vs) == 1 else vs

    return {
        "decoded_len": len(raw),
        "decoded_hex_head": raw[:80].hex(),
        "decoded_text_printable_head": _to_printable(text[:200]),
        "fields": fields,
        "human": human,
    }


def decode_raonkhandler_url(url: str) -> dict:
    """
    input: raonkhandler.jsp URL (k00 포함)
    output: 사람이 읽기 좋은 dict
    """
    u = urlparse(url)
    qs = parse_qs(u.query)

    if "k00" not in qs or not qs["k00"]:
        raise ValueError("URL querystring에 k00 파라미터가 없습니다.")

    # k00는 URL-encoding 되어있을 수 있어서 unquote
    k00 = unquote(qs["k00"][0])

    raw = _b64decode_with_padding(k00)
    parsed = _parse_raonk_blob(raw)

    # AddUploadedFile 비슷하게 요약해보기 (있을 때만)
    human = parsed.get("human", {})
    summary = {
        "filename": human.get("filename"),
        "path": human.get("path"),
        "customValue": human.get("customValue"),
        "token_or_hash": human.get("token_or_hash"),
    }

    return {
        "input_url": url,
        "host": u.netloc,
        "path": u.path,
        "k00": k00,
        "parsed": parsed,
        "summary_guess": summary,
    }


# ---------------------------
# 사용 예시
# ---------------------------
if __name__ == "__main__":
    test_url = "https://dw.vworld.kr/vwDnMng/raonkupload/handler/raonkhandler.jsp?k00=a2MMYzianzwEworC2swMQwxC2sxMgxlZTc5MTgyMjM2MzI0ZjJhOTM2NTEyMmY5MzA0ZWYzMwtrMjgMMTULazMxDEFMX0QxNTVfMTFfMjAyNTEyMDQuemlwC2syMQwyMDE3MTEyOERTMDAxNDh8MTI5MAtrMzAML2ZpbGVzdG9yZS9kb3duX3N0b3JlL2R0bmEvMjAyNTEyLzBmOGNhNTk2MGQzNzQ3MTZhNTkzOTJlMzI5MzNlNDc5LnppcAs="
    result = decode_raonkhandler_url(test_url)
    print(json.dumps(result, ensure_ascii=False, indent=2))
