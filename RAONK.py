import base64
from urllib.parse import urlparse, parse_qs
from typing import Any, Dict, List

FIELD_SEP = b"\x0b"  # \v (Vertical Tab)  : RAONK 필드 구분자로 자주 보임
KV_SEP    = b"\x0c"  # \f (Form Feed)     : RAONK 내부 구분자로 자주 보임


def _b64_decode_loose(s: str) -> bytes:
    """padding(=)이 빠진 base64도 최대한 복구해서 디코딩."""
    s = "".join((s or "").split())
    pad = (-len(s)) % 4
    if pad:
        s += "=" * pad

    try:
        return base64.b64decode(s, validate=False)
    except Exception:
        return base64.urlsafe_b64decode(s)


def _is_printable_ascii(b: bytes) -> bool:
    """완전한 printable ASCII(공백 포함)면 True."""
    if not b:
        return True
    for ch in b:
        if ch < 0x20 or ch > 0x7E:
            return False
    return True


def _escape_bytes(b: bytes) -> str:
    """바이트를 사람이 보기 좋게 \\x.., \\v, \\f 등으로 표시."""
    out = []
    for ch in b:
        if 0x20 <= ch <= 0x7E:
            out.append(chr(ch))
        elif ch == 0x0b:
            out.append("\\v")
        elif ch == 0x0c:
            out.append("\\f")
        elif ch == 0x0a:
            out.append("\\n")
        elif ch == 0x0d:
            out.append("\\r")
        elif ch == 0x09:
            out.append("\\t")
        else:
            out.append(f"\\x{ch:02x}")
    return "".join(out)


def _extract_ascii_fragments(b: bytes, min_len: int = 4) -> List[str]:
    """
    바이너리 안에서 '사람이 읽을만한' ASCII 조각을 찾아줌.
    (경로/파일명 같은 게 숨어있을 때 유용)
    """
    frags = []
    cur = []
    for ch in b:
        if 0x20 <= ch <= 0x7E:
            cur.append(ch)
        else:
            if len(cur) >= min_len:
                frags.append(bytes(cur).decode("ascii", errors="ignore"))
            cur = []
    if len(cur) >= min_len:
        frags.append(bytes(cur).decode("ascii", errors="ignore"))
    return frags


def _bytes_to_human(b: bytes, max_escape: int = 120) -> str:
    """
    바이트를 사람이 볼 수 있는 형태로 변환:
    - 전부 ASCII면 그대로
    - 아니면 hex + (가능하면 ASCII 조각들) + escaped(일부)
    """
    if _is_printable_ascii(b):
        return f"ASCII: {b.decode('ascii', errors='ignore')}"

    hex_str = b.hex()
    esc = _escape_bytes(b)
    if len(esc) > max_escape:
        esc = esc[:max_escape] + "..."

    frags = _extract_ascii_fragments(b, min_len=4)
    frag_txt = ", ".join(frags[:5])  # 너무 길어지면 앞부분만
    if frag_txt:
        return f"HEX: {hex_str} | ASCII_FRAGS: [{frag_txt}] | ESC: {esc}"
    return f"HEX: {hex_str} | ESC: {esc}"


def decode_raonk_k00_from_url(url: str) -> Dict[str, Any]:
    """
    URL에서 k00를 뽑아 base64 디코딩 후,
    1) \v 로 1차 세그먼트 분리
    2) 각 세그먼트를 \f 로 다시 분해
    해서 '사람이 볼 수 있는' 형태로 반환.
    """
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)

    if "k00" not in qs or not qs["k00"]:
        raise ValueError("URL에 k00 파라미터가 없습니다.")

    k00_b64 = qs["k00"][0]
    raw = _b64_decode_loose(k00_b64)

    # 세그먼트(\v) → 서브필드(\f)
    segments_raw = [p for p in raw.split(FIELD_SEP) if p]
    segments: List[Dict[str, Any]] = []

    for si, seg in enumerate(segments_raw):
        sub = seg.split(KV_SEP)
        segments.append({
            "segment_index": si,
            "segment_len": len(seg),
            "subfields_count": len(sub),
            "subfields": [
                {
                    "sub_index": i,
                    "len": len(x),
                    "human": _bytes_to_human(x),
                }
                for i, x in enumerate(sub)
            ],
        })

    return {
        "host": parsed.netloc,
        "path": parsed.path,
        "k00_base64": k00_b64,
        "raw_len": len(raw),
        "raw_hex_head": raw.hex()[:240] + ("..." if len(raw.hex()) > 240 else ""),
        "segments": segments,
    }


def print_decoded_k00(url: str) -> None:
    info = decode_raonk_k00_from_url(url)

    print("=== URL INFO ===")
    print(f"host: {info['host']}")
    print(f"path: {info['path']}")
    print(f"raw_len: {info['raw_len']}")
    print(f"raw_hex_head: {info['raw_hex_head']}\n")

    print("=== SEGMENTS (split by \\v=0x0b) ===")
    for seg in info["segments"]:
        print(f"\n[SEG {seg['segment_index']}] len={seg['segment_len']} subfields={seg['subfields_count']}")
        for sf in seg["subfields"]:
            print(f"  - sub[{sf['sub_index']}] len={sf['len']} :: {sf['human']}")


def _main() -> int:
    # ✅ 로직에서 URL을 직접 넣는 방식
    print("1: k00 디코딩 테스트")
    url = "https://dw.vworld.kr/vwDnMng/raonkupload/handler/raonkhandler.jsp?k00=a2MMYzianzwEworC2swMQwxC2sxMgxlZTc5MTgyMjM2MzI0ZjJhOTM2NTEyMmY5MzA0ZWYzMwtrMjgMMTULazMxDEFMX0QxNTVfMTFfMjAyNTEyMDQuemlwC2syMQwyMDE3MTEyOERTMDAxNDh8MTI5MAtrMzAML2ZpbGVzdG9yZS9kb3duX3N0b3JlL2R0bmEvMjAyNTEyLzBmOGNhNTk2MGQzNzQ3MTZhNTkzOTJlMzI5MzNlNDc5LnppcAs="
    print_decoded_k00(url)
    print("\n\n")
    # print("2: k00 디코딩 테스트")
    # url = "https://dw.vworld.kr/vwDnMng/raonkupload/handler/raonkhandler.jsp?k00=a2MMYzianzwEworC2swMQwxC2sxMgxjZDAxNDkzMGIzNzk0ZmIzYmUyNjQ1Yjc4MjE2NTU5YQtrMjgMMTULazMxDEFMX0QxNTVfNDNfMjAyNTExMDQuemlwC2syMQwyMDE3MTEyOERTMDAxNDh8MTI4MgtrMzAML2ZpbGVzdG9yZTIvZG93bl9zdG9yZS9kdG5hLzIwMjUxMS83M2M4N2JhMGE2ZjM0MGU2YjQwNGZkZDMwMmI1NTYwYy56aXAL"
    # print_decoded_k00(url)
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
