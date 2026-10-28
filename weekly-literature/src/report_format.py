"""Stable, evidence-preserving Notion format for weekly literature reports.

Pure rendering is separated from the Notion writer. No new AI analysis is
performed here; all scientific statements come from persisted AI decisions.
"""
import hashlib
import json
import re
import urllib.request
import urllib.error

from pipeline import request, TRIAGE_POLICY, BASIC_TERMS, AI_TERMS, in_scope

REPORT_STYLE_VERSION = "2026-10-14-v1"
GROUPS = (
    ("A. 临床重点研究、风险分层与指南", r"guideline|risk stratification|perineural invasion|lymph node ratio|methodological challenges|surgical randomized|clinical practice|指南|风险分层|神经侵犯|淋巴结比值"),
    ("B. 直肠癌新辅助治疗、器官保留与精准手术", r"neoadjuvant|organ.preserv|watch.and.wait|total neoadjuvant|restaging|tnt|immunotherapy.*rectal|rectal.*immunotherapy|新辅助|器官保留|观察等待|再分期"),
    ("C. 结直肠外科、NOSES与围术期并发症", r"natural orifice|nose[st]?\b|anastomos|colectomy|surgical site|hospital.associated infection|incisional hernia|perioperative|laparoscop|robotic surgery|operative technique|经自然腔道|吻合口|腹腔镜|机器人手术|围术期"),
    ("D. 术后功能、康复与患者报告结局", r"rehabilitation|postoperative nursing|dyadic nursing|functional outcome|low anterior resection syndrome|lars\b|bowel symptom|proctitis|fistula|patient.initiated follow.up|patient.reported|康复|低位前切除综合征|排便功能|放射性直肠炎|造口护理|患者报告"),
    ("E. 筛查、分子标志物与系统治疗", r"screening|early detection|ctdna|fruquintinib|chemotherap|drug toxicit|biomarker|molecular surgery|targeted therap|prevention|metastatic|筛查|循环肿瘤|呋喹替尼|化疗|靶向|转移性"),
    ("F. 结直肠癌预测模型与医学AI", r"artificial intelligence|machine learning|deep learning|language model|foundation model|radiomics|nomogram|prediction model|predictive model|ai.assist|digital twin|人工智能|机器学习|深度学习|影像组学|预测模型|列线图"),
)
FALLBACK = "G. 基础与转化研究"


def _text(value):
    """Notion rich text, segmented well below Notion's 2000-char limit."""
    content = str(value or "")
    return [{"type": "text", "text": {"content": content[i:i + 1500]}} for i in range(0, len(content), 1500)] or [
        {"type": "text", "text": {"content": ""}}
    ]


def _block(kind, content, **extra):
    body = {"rich_text": _text(content)}
    body.update(extra)
    return {"object": "block", "type": kind, kind: body}


def format_blocks(blocks):
    """Historical weekly style; change presentation only, never evidence text."""
    import copy
    result = copy.deepcopy(blocks)
    for block in result:
        kind = block['type']
        if kind == 'callout':
            block[kind]['color'] = 'blue_background'
        if kind == 'table_of_contents':
            block[kind]['color'] = 'gray'
        if kind not in ('bulleted_list_item', 'callout'):
            continue
        rich = block[kind].get('rich_text', [])
        if not rich or rich[0].get('type') != 'text':
            continue
        first = rich[0]['text']['content']
        match = re.match(r'^([^：:｜]{1,18}[：:｜])', first)
        if match:
            label = copy.deepcopy(rich[0])
            label['text']['content'] = match.group(1)
            label['annotations'] = {**label.get('annotations', {}), 'bold': True}
            remainder = copy.deepcopy(rich[0])
            remainder['text']['content'] = first[match.end():]
            block[kind]['rich_text'] = [label] + ([remainder] if remainder['text']['content'] else []) + rich[1:]
    return result


def _source_parts(article, evidence_key):
    parts = []
    pmid = str(article.get("pmid") or "")
    if re.fullmatch(r"\d{1,12}", pmid):
        parts.append(("PubMed", "https://pubmed.ncbi.nlm.nih.gov/" + pmid + "/"))
    doi = str(article.get("doi") or "").strip()
    doi = re.sub(r"^(?:https?://(?:dx\.)?doi\.org/|doi:\s*)", "", doi, flags=re.I).rstrip(".,;")
    if doi and re.fullmatch(r"10\.\S+/\S+", doi, re.I):
        parts.append(("DOI", "https://doi.org/" + doi))
    arxiv = str(article.get("arxiv_id") or "")
    if re.fullmatch(r"\d{4}\.\d{4,5}(?:v\d+)?", arxiv):
        parts.append(("arXiv预印本", "https://arxiv.org/abs/" + arxiv))
    if not parts:
        return _text(evidence_key)
    result = _text("原文与标识：" + evidence_key + "｜")
    for n, (label, url) in enumerate(parts):
        if n:
            result += _text("｜")
        result.append({"type": "text", "text": {"content": label, "link": {"url": url}}})
    return result


def _category(article):
    # Classify once by original title. Unknown items are kept in fallback,
    # never omitted or auto-excluded because a title lacks keywords.
    title = str(article.get("title") or "").lower()
    for name, pattern in GROUPS:
        if re.search(pattern, title):
            return name
    return FALLBACK


def digestive_category(article):
    text=str(article.get('title') or '')+' '+str(article.get('abstract') or '')
    if AI_TERMS.search(text): return 'F. 消化系统与盆底疾病相关AI'
    if re.search(r'enhanced recovery|eras\b|frailty|fecal incontinence|pelvic.floor|constipation|康复|盆底|便秘',text,re.I):
        return 'D. 消化系统术后功能、康复与盆底疾病'
    for name,pattern in GROUPS:
        if re.search(pattern,text,re.I): return name.replace('结直肠外科','消化系统外科')
    if re.search(r'endoscop|esd\b|emr\b|colonoscopy|gastroscopy|内镜',text,re.I):
        return 'C. 消化系统内镜、外科与围术期'
    if re.search(r'clinical trial|randomi[sz]|prospective|retrospective|cohort|临床试验|队列',text,re.I):
        return 'H. 其他消化系统与盆底临床研究'
    if BASIC_TERMS.search(text): return 'G. 消化系统疾病基础与转化研究'
    return 'H. 其他消化系统与盆底临床研究'


def display_title(article,analysis):
    match=re.search(r'^中文题名[：:]\s*(.+)$',str(analysis.get('summary') or ''),re.M)
    return str(article.get('display_title_zh') or (match[1] if match else '') or article.get('title') or '无题')


def _excerpt(summary):
    """One complete sentence copied from the saved analysis, no new claims."""
    source = str(summary or "").strip()
    match = re.search(r"(?:主要结果|核心发现|结论|临床意义)[：:]\s*(.+?)(?:。|；|\n|$)", source)
    value = match.group(1).strip() if match else source.split("。", 1)[0].strip()
    return value  # Never truncate a study outcome mid-sentence.


def build_report(state, rows):
    """Return native Notion blocks and the count of included articles."""
    if any(row.get("payload") is None for row in rows):
        raise ValueError("Missing AI decisions: refuse partial publication")

    papers = []
    for row in rows:
        value, article = json.loads(row["payload"]), json.loads(row["article"])
        if value.get("decision") == "include":
            papers.append({"key": row["evidence_key"], "analysis": value, "article": article})

    audit = json.loads(state.get("audit") or "{}")
    sources = audit.get("supplementary_sources", {})
    gaps = [name + "：" + str(info.get("status", "UNKNOWN"))
            for name, info in sources.items() if info.get("status") not in ("COMPLETE", "COMPLETE_FOR_QUERY")]
    if any(info.get("status") == "COMPLETE_FOR_QUERY" for info in sources.values()):
        gaps.append("COMPLETE_FOR_QUERY仅代表预设查询覆盖")
    page = []
    def para(value):
        page.append(_block("paragraph", value))
    def head(value, level=1):
        page.append(_block("heading_" + str(level), value))
    def bullet(value):
        page.append(_block("bulleted_list_item", value))

    page.append(_block("callout", (
        "正式归档｜" + state["run_key"] + "。本批复核" + str(len(rows)) + "篇；纳入" +
        str(len(papers)) + "篇，排除" + str(len(rows) - len(papers)) +
        "篇。研究解读来自既有AI分析，未经统一全文复核；未覆盖来源列于文末。"),
        icon={"type": "emoji", "emoji": "📚"}))
    page.append({"object": "block", "type": "table_of_contents", "table_of_contents": {}})

    head("本周核心结论")
    if papers:
        para("以下为已完成分析文献的阅读要点（直接节选已有分析，不额外推断研究结论）；完整结果及局限见各专题。")
        for row in papers[:5]:
            bullet(display_title(row["article"],row["analysis"]) + "｜" + _excerpt(row["analysis"].get("summary")) +
                   "（" + row["key"] + "）")
    else:
        para("本批已完成审核，但没有达到纳入标准的文献。")

    head("检索与纳入概况")
    bullet("时间窗：" + state["window_start"] + " 至 " + state["window_end"] +
           "（Asia/Shanghai）；批次：" + state["run_key"])
    bullet("原始检索命中：" + str(audit.get("raw_hits", "未报告")) +
           "；去重PubMed：" + str(audit.get("unique_pmids", "未报告")) +
           "；元数据成功：" + str(audit.get("metadata_success", "未报告")))
    bullet("规则适格：" + str(audit.get("eligible", "未报告")) +
           "；送审：" + str(len(rows)) + "；纳入：" + str(len(papers)) +
           "；排除：" + str(len(rows) - len(papers)) +
           "；尚未选入审阅的候选：" + str(audit.get("deferred", "未报告")))
    bullet("来源覆盖：" + ("；".join(gaps) if gaps else "已配置来源状态详见审计，不能推定所有数据库全覆盖"))

    grouped = {name: [] for name, _ in GROUPS}
    grouped[FALLBACK] = []
    scoped=audit.get('triage',{}).get('policy')==TRIAGE_POLICY
    for p in papers:
        category=digestive_category(p['article']) if scoped else _category(p['article'])
        grouped.setdefault(category,[]).append(p)
    rank = 0
    for name, group in grouped.items():
        if not group:
            continue
        head(name)
        for item in group:
            rank += 1
            a = item["article"]
            v = item["analysis"]
            head(str(rank) + ". " + display_title(a,v), 2)
            bullet("英文原题：" + str(a.get("title") or "未报告"))
            page.append({"object": "block", "type": "bulleted_list_item",
                         "bulleted_list_item": {"rich_text": _source_parts(a, item["key"])}})
            bullet("期刊／日期：" + str(a.get("journal") or "未报告") +
                   "；" + str(a.get("publication_date") or "未报告"))
            bullet("研究解读：" + str(v.get("summary") or "未报告"))
            bullet("科普候选／证据边界：" + str(v.get("popscience") or "未报告"))

    head("科普选题建议")
    priority = {"high": 0, "medium": 1, "low": 2}
    candidates = sorted(papers, key=lambda r: priority.get(r["analysis"].get("popscience_priority"), 2))
    if candidates:
        for p in candidates[:8]:
            bullet("[" + str(p["analysis"].get("popscience_priority", "low")).upper() + "] " +
                   display_title(p["article"],p['analysis']) + "｜" +
                   str(p["analysis"].get("popscience") or "需医学审核") + "（" + p["key"] + "）")
    else:
        para("本批无已纳入论文可供推荐。")

    head("检索审计与局限")
    for name, info in sources.items():
        bullet(name + "：" + str(info.get("status", "UNKNOWN")) +
               ("；窗口：" + str(info["window_start"]) + " 至 " + str(info["window_end"])
                if info.get("window_start") and info.get("window_end") else "") +
               ("；错误类别：" + str(info["error"]) if info.get("error") else ""))
    backfill=audit.get('scope',{}).get('backfill',{})
    if backfill:
        bullet('范围扩展补检：'+str(backfill.get('status','未完成'))+'；与原检索审计分开，不把原检索完成视为补检完成。')
        windows=backfill.get('sources') or backfill.get('source_windows',{})
        for name,info in windows.items():
            bullet('补检 '+name+'：'+str(info.get('status','未完成' if info.get('window_start') else 'NOT_ATTEMPTED'))+
                '；窗口：'+str(info.get('window_start','未检索'))+' 至 '+str(info.get('window_end','未检索')))
    para("PUBLISHED表示已送达Notion并读回验证，不等于全部来源覆盖完整；摘要分析不能替代全文审阅，也不构成患者诊疗建议。")
    para("稳定追溯键：" + state["run_key"] + "｜排版版本：" + REPORT_STYLE_VERSION)
    return format_blocks(page), len(papers)


def _canonical(block):
    kind = block.get("type")
    if kind == "table_of_contents":
        return {"type": kind}
    data = block.get(kind, {})
    if kind not in {"callout", "paragraph", "heading_1", "heading_2", "bulleted_list_item"}:
        return {"type": "unrecognized"}
    content = []
    for item in data.get("rich_text", []):
        txt = item.get("plain_text")
        if txt is None:
            txt = item.get("text", {}).get("content", "")
        link = item.get("href")
        if link is None:
            link = (item.get("text", {}).get("link") or {}).get("url")
        content.append([txt, link])
    result = {"type": kind, "content": content}
    if kind == "callout":
        result["emoji"] = (data.get("icon") or {}).get("emoji")
    return result


def publish_report_blocks(store, run, page, blocks, token, prefix=None):
    """Append missing native blocks in <=50-block batches with exact readback.

    Never blindly retry PATCH after an ambiguous response: reread blocks first.
    Existing manual edits cause an explicit conflict instead of an overwrite.
    """
    prefix = prefix or []
    expected = prefix + [_canonical(b) for b in blocks]
    content_hash = hashlib.sha256(json.dumps(
        {"style": REPORT_STYLE_VERSION, "blocks": expected}, ensure_ascii=False,
        sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    saved = store.sql("SELECT status,content_hash FROM wl_publications WHERE run_key=?", (run,))[0]
    if saved["status"] == "VERIFIED":
        if saved["content_hash"] == content_hash:
            return
        prefix_hash = hashlib.sha256(json.dumps(
            {"style": REPORT_STYLE_VERSION, "blocks": prefix}, ensure_ascii=False,
            sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        if not prefix or saved["content_hash"] != prefix_hash:
            raise ValueError("Verified artifact content conflict")
    headers = {"Authorization": "Bearer " + token, "Notion-Version": "2025-09-03",
               "Content-Type": "application/json"}
    base = "https://api.notion.com/v1/blocks/" + page + "/children"

    def read():
        items, cursor = [], None
        while True:
            url = base + ("?start_cursor=" + cursor if cursor else "")
            value = json.loads(request(url, headers=headers))
            items.extend(_canonical(b) for b in value["results"])
            if not value["has_more"]:
                return items
            cursor = value["next_cursor"]

    existing = read()
    if len(existing) > len(expected) or existing != expected[:len(existing)]:
        raise ValueError("Output page differs; preserve user edits and reconcile manually")
    if len(existing) < len(prefix):
        raise ValueError("Previously verified report blocks missing")
    for offset in range(len(existing)-len(prefix), len(blocks), 50):
        payload = {"children": blocks[offset:offset + 50]}
        req = urllib.request.Request(base, data=json.dumps(payload, ensure_ascii=False).encode(),
                                     headers=headers, method="PATCH")
        try:
            with urllib.request.urlopen(req, timeout=60) as response:
                response.read()
        except urllib.error.HTTPError as exc:
            # Expose only a schema path, never the API's quoted source content.
            if exc.code==400:
                try:
                    message=json.loads(exc.read()).get('message','')
                    field=re.search(r'\bbody(?:\.[a-z_]+|\[\d+\])+',message)
                    if field: exc.safe_field=field[0]
                except (ValueError,AttributeError): pass
            raise
    if read() != expected:
        raise ValueError("Notion native block readback mismatch")
    store.sql("UPDATE wl_publications SET status='VERIFIED',content_hash=? WHERE run_key=?",
              (content_hash, run))


def update_report_metadata(run, page, included, audit, token, data_source):
    """Idempotently make the database card resemble historical weekly pages."""
    headers = {"Authorization": "Bearer " + token, "Notion-Version": "2025-09-03",
               "Content-Type": "application/json"}
    schema = json.loads(request("https://api.notion.com/v1/data_sources/" + data_source, headers=headers))
    properties = schema.get("properties", {})
    date = run.split(":")[1]
    statuses = [v.get("status") for v in audit.get("supplementary_sources", {}).values()]
    partial = any(x not in ("COMPLETE", "COMPLETE_FOR_QUERY") for x in statuses)
    values = {}
    title = next((k for k, v in properties.items() if v.get("type") == "title"), None)
    if title:
        values[title] = {"title": _text(date + "｜每周文献（" + str(included) + "项）")}
    if properties.get("执行日期", {}).get("type") == "date":
        values["执行日期"] = {"date": {"start": date}}
    for field, number in (("原始总数", audit.get("raw_hits")), ("去重后文献数", included)):
        if properties.get(field, {}).get("type") == "number" and number is not None:
            values[field] = {"number": number}
    definition = properties.get("处理状态", {})
    label = "数据不完整" if partial else "完成"
    if definition.get("type") in ("select", "status") and label in {
        item["name"] for item in definition[definition["type"]]["options"]
    }:
        values["处理状态"] = {definition["type"]: {"name": label}}
    if properties.get("摘要", {}).get("type") == "rich_text":
        values["摘要"] = {"rich_text": _text(
            "复核" + str(audit.get("selected", "若干")) + "篇候选，纳入" + str(included) +
            "篇。按专题归档并保留检索来源缺口审计。")}
    body = {"properties": values, "icon": {"type": "emoji", "emoji": "📫"}}
    req = urllib.request.Request("https://api.notion.com/v1/pages/" + page,
                                 data=json.dumps(body, ensure_ascii=False).encode(),
                                 headers={**headers, "Content-Type": "application/json"}, method="PATCH")
    with urllib.request.urlopen(req, timeout=60) as response:
        response.read()
    actual = json.loads(request("https://api.notion.com/v1/pages/" + page, headers=headers))
    for name, expected in values.items():
        kind, wanted = next(iter(expected.items()))
        got = actual.get('properties', {}).get(name, {}).get(kind)
        if kind in ('title', 'rich_text'):
            flatten = lambda items: ''.join(x.get('plain_text', x.get('text', {}).get('content', '')) for x in (items or []))
            matches = flatten(got) == flatten(wanted)
        elif kind in ('status', 'select'):
            matches = (got or {}).get('name') == wanted['name']
        elif kind == 'date':
            matches = (got or {}).get('start') == wanted['start']
        else:
            matches = got == wanted
        if not matches:
            raise ValueError('Notion report metadata readback mismatch: '+name)




