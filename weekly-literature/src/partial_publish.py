"""Bounded, resumable Notion delivery. D1 remains the only publication ledger.

The processing index links immutable <=50-decision batches. Neither partial
delivery nor an empty delta commits wl_history or finalizes a literature run.
"""
import json
import os
from publish import provision_page, finalize_article_title, publish_draft
from report_format import build_report, publish_report_blocks, _block, _canonical, format_blocks

INDEX = 'notion:partial:index:v1'
ACTIVE = 'notion:partial:active:v1'
SHOWN = 'notion:partial:shown:v1:'
NOTION_ORIGIN = 'https://www.notion.so/'


def has_unsynced(store, run):
    return bool(store.sql(
        'SELECT 1 FROM wl_candidates c JOIN wl_analysis n '
        'ON n.run_key=c.run_key AND n.evidence_key=c.evidence_key '
        'WHERE c.run_key=? AND c.selected=1 AND NOT EXISTS '
        '(SELECT 1 FROM wl_checkpoints p WHERE p.run_key=c.run_key AND p.unit=?||c.evidence_key) LIMIT 1',
        (run, SHOWN)))


def final_index(state, rows, entries):
    """Finalize by linking verified batches, not duplicating thousands of summaries."""
    included = sum(json.loads(r['payload'])['decision']=='include' for r in rows)
    audit = json.loads(state['audit'])
    blocks = [_block('callout', '正式归档｜'+state['run_key']+'；复核'+str(len(rows))+
                     '篇，纳入'+str(included)+'篇，排除'+str(len(rows)-included)+
                     '篇。下列各批已读回核验，完整分析按批保存；科普稿仍待医学审核。',
                     icon={'type':'emoji','emoji':'📚'}),
              {'object':'block','type':'table_of_contents','table_of_contents':{}},
              _block('heading_1','本周核心结论'),
              _block('paragraph','已核验各批的核心结论和A—G专题正文见下列入口；此处不重新推理或改写医学结论。'),
              _block('heading_1','检索与纳入概况'),
              _block('bulleted_list_item','候选复核：'+str(len(rows))+'篇；纳入：'+str(included)+'篇；排除：'+str(len(rows)-included)+'篇。'),
              _block('heading_1','文献分析（分批阅读）')]
    for item in entries:
        blocks.append(_block('paragraph', '批次'+str(item['number'])+'｜复核'+str(item['count'])+
                             '、纳入'+str(item['included'])+'：'+NOTION_ORIGIN+item['page'].replace('-','')))
    blocks.append(_block('heading_1','科普选题建议'))
    blocks.append(_block('paragraph','各批保留选题优先级和证据边界，高优先级稿件单独保存，仍待医学审核。'))
    blocks.append(_block('heading_1','检索审计与局限'))
    blocks.append(_block('paragraph','检索窗口：'+state['window_start']+' 至 '+state['window_end']))
    for name, info in audit.get('supplementary_sources',{}).items():
        blocks.append(_block('paragraph',name+'：'+str(info.get('status','UNKNOWN'))+
                             '；窗口：'+str(info.get('window_start','未报告'))+' 至 '+str(info.get('window_end','未报告'))))
    blocks.append(_block('paragraph','COMPLETE_FOR_QUERY仅代表预设查询完成，不代表全部来源完整覆盖。分析基于既有证据材料，不替代全文医学审核或患者诊疗。'))
    return format_blocks(blocks), included


def cached(store, run, unit, default):
    rows = store.sql('SELECT payload FROM wl_checkpoints WHERE run_key=? AND unit=?', (run, unit))
    return json.loads(rows[0]['payload']) if rows else default


def legacy_progress_blocks(run, entries):
    blocks = [_block('heading_1', run.split(':')[1]+'｜每周文献（处理中）'),
              _block('paragraph', '以下仅为已分析内容的分批同步，不是最终周报。未纳入文献不展示正文；科普稿均待医学审核。最新进度见末尾批次，来源缺口见各批审计。')]
    for item in entries:
        p = item['progress']
        blocks.append(_block('paragraph',
            '批次'+str(item['number'])+'｜已分析 '+str(p['analyzed'])+'/'+str(p['selected'])+
            '；待分析 '+str(p['selected']-p['analyzed'])+'；本批复核 '+str(item['count'])+
            '、纳入 '+str(item['included'])+'。阅读：'+NOTION_ORIGIN+item['page'].replace('-', '')))
    return blocks


def progress_blocks(run, entries):
    blocks = [
        _block('callout', '处理中｜'+run.split(':')[1]+'批次。分批保存已分析文献，不是最终周报。医学结论不在排版时重新生成。',
               icon={'type':'emoji','emoji':'📚'}),
        {'object':'block','type':'table_of_contents','table_of_contents':{}},
        _block('heading_1','本周核心结论'),
        _block('paragraph','各批页面按专题列出已保存的核心结论与完整解读。批次尚未结束，不把阶段性结果表述为全周结论。'),
        _block('heading_1','检索与纳入概况'),
        _block('bulleted_list_item','批次日期：'+run.split(':')[1]+'；真实已分析、候选总数和待分析数量见下方各批进度。'),
        _block('heading_1','科普选题建议'),
        _block('paragraph','各批保留科普候选与证据边界；高优先级完整稿单独保存，均待医学审核，不视为对外发布。'),
        _block('heading_1','检索审计与局限'),
        _block('paragraph','来源状态和窗口保留在各批文末。COMPLETE_FOR_QUERY仅代表既定查询完成；PARTIAL及未检索来源不得写成完整。分析基于摘要时仍须保留限定。'),
        _block('heading_1','文献分析（分批阅读）'),
        _block('paragraph','各批沿用A—G专题分组：临床重点与指南；新辅助与器官保留；外科与围术期；康复与患者报告结局；筛查与系统治疗；医学AI；基础转化。每篇保留原题、来源、期刊日期、研究解读与科普边界。')]
    for item in entries:
        old = legacy_progress_blocks(run, [item])[-1]
        blocks.append(_block('bulleted_list_item', old['paragraph']['rich_text'][0]['text']['content']))
    return format_blocks(blocks)


def sync_partial(store, run, progress):
    """Sync at most 50 saved decisions; freeze the selection before Notion writes."""
    entries = cached(store, run, INDEX, [])
    if entries:
        from layout_upgrade import upgrade_progress
        upgrade_progress(store, run, entries)
    active = cached(store, run, ACTIVE, None)
    if active is None:
        rows = store.sql(
            'SELECT c.evidence_key FROM wl_candidates c JOIN wl_analysis n '
            'ON n.run_key=c.run_key AND n.evidence_key=c.evidence_key '
            'LEFT JOIN wl_checkpoints p ON p.run_key=c.run_key AND p.unit=?||c.evidence_key '
            'WHERE c.run_key=? AND c.selected=1 AND p.unit IS NULL '
            'ORDER BY c.evidence_key LIMIT 50', (SHOWN, run))
        if not rows:
            return False
        active = {'number': len(entries)+1, 'keys': [r['evidence_key'] for r in rows], 'progress': progress}
        store.sql('INSERT INTO wl_checkpoints VALUES(?,?,?)', (run, ACTIVE, json.dumps(active)))
    placeholders = ','.join('?' for _ in active['keys'])
    rows = store.sql(
        'SELECT c.evidence_key,n.payload,a.payload AS article FROM wl_candidates c '
        'JOIN wl_analysis n ON n.run_key=c.run_key AND n.evidence_key=c.evidence_key '
        'JOIN wl_articles a ON a.evidence_key=c.evidence_key '
        'WHERE c.run_key=? AND c.selected=1 AND c.evidence_key IN ('+placeholders+') '
        'ORDER BY c.evidence_key', (run, *active['keys']))
    if len(rows) != len(active['keys']):
        raise ValueError('Partial batch metadata or analysis missing')
    state = store.sql('SELECT * FROM wl_runs WHERE run_key=?', (run,))[0]
    if state['status'] != 'READY':
        raise ValueError('Partial sync requires READY run')
    artifact = 'partial:'+run+':'+str(active['number'])
    page = provision_page(store, artifact)
    blocks, included = build_report(state, rows)
    blocks[0] = _block('callout', '处理中｜分批同步，不是最终周报。本批复核'+str(len(rows))+
                       '篇、纳入'+str(included)+'篇。摘要分析，待专业审阅。',
                       icon={'type':'emoji','emoji':'📚'})
    blocks[-2] = _block('paragraph', '本批送达仅代表上述分析已读回核验，运行仍在处理中；来源覆盖和医学审核均不因此完成。')
    publish_report_blocks(store, artifact, page, format_blocks(blocks), os.environ['NOTION_TOKEN'])
    finalize_article_title(page, artifact, run.split(':')[1]+'｜处理中·文献批次'+str(active['number']))
    for row in rows:
        publish_draft(store, row)
    entry = dict(number=active['number'], page=page, count=len(rows), included=included, progress=active['progress'])
    index_artifact = 'progress:'+run
    index_page = provision_page(store, index_artifact)
    old = progress_blocks(run, entries)
    new = progress_blocks(run, entries+[entry])
    # Prefix hashes permit only verified extension; edited pages fail closed.
    publish_report_blocks(store, index_artifact, index_page, new[len(old):] if entries else new,
                          os.environ['NOTION_TOKEN'], prefix=[_canonical(b) for b in old] if entries else None)
    finalize_article_title(index_page, index_artifact, run.split(':')[1]+'｜每周文献（处理中·分批更新）')
    store.batch([('INSERT OR IGNORE INTO wl_checkpoints VALUES(?,?,?)', (run, SHOWN+key, artifact))
                 for key in active['keys']]+[
        ('INSERT OR REPLACE INTO wl_checkpoints VALUES(?,?,?)', (run, INDEX, json.dumps(entries+[entry]))),
        ('DELETE FROM wl_checkpoints WHERE run_key=? AND unit=?', (run, ACTIVE))])
    return True


def finish_progress(store, run):
    """Link the separately verified final report, without rewriting batch pages."""
    entries = cached(store, run, INDEX, [])
    if not entries:
        return
    from layout_upgrade import upgrade_progress
    upgrade_progress(store, run, entries)
    state = store.sql('SELECT status FROM wl_runs WHERE run_key=?', (run,))[0]
    if state['status'] != 'PUBLISHED':
        raise ValueError('Final run not published')
    target = store.sql('SELECT page_id FROM wl_publications WHERE run_key=?', (run,))[0]['page_id']
    artifact = 'progress:'+run
    page = provision_page(store, artifact)
    old = progress_blocks(run, entries)
    tail = [_block('heading_1', '本批分析已全部完成'),
            _block('paragraph', '最终周报已独立核验：'+NOTION_ORIGIN+target.replace('-', '')+
                   '。来源缺口仍以最终审计为准；科普文章仍待医学审核。')]
    publish_report_blocks(store, artifact, page, tail, os.environ['NOTION_TOKEN'],
                          prefix=[_canonical(b) for b in old])
    finalize_article_title(page, run.split(':')[1]+'｜每周文献（处理中·分批更新）',
                           run.split(':')[1]+'｜每周文献（已完成·分批索引）')
    store.sql('INSERT OR IGNORE INTO wl_checkpoints VALUES(?,?,?)',
              (run, 'notion:partial:finished:v1', 'true'))
