"""Layout and retry-safety regression tests; no external credentials required."""
import json
import sys
from pathlib import Path
import unittest
import copy
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import pipeline
import report_format as rf


def row(key, title, decision="include", priority="medium"):
    record = {"title": title, "pmid": key.split(":")[1] if key.startswith("PMID:") else "",
              "doi": "10.1234/" + key.split(":")[-1],
              "journal": "Example journal", "publication_date": "2026-10-10"}
    analysis = {"decision": decision, "summary": "【基于摘要】主要结果：有待进一步外部验证。不能推导因果。",
                "popscience": "人群与证据边界已标明。", "popscience_priority": priority}
    return {"evidence_key": key, "article": json.dumps(record), "payload": json.dumps(analysis)}


class LayoutTest(unittest.TestCase):
    def setUp(self):
        self.state = {"run_key": "weekly-literature:2026-10-12:Asia/Shanghai",
                      "window_start": "2026-09-29", "window_end": "2026-10-11",
                      "audit": json.dumps({"raw_hits": 5000, "unique_pmids": 2200,
                                           "selected": 5, "eligible": 900,
                                           "deferred": 895, "supplementary_sources": {
                                               "Europe PMC": {"status": "PARTIAL"},
                                               "arXiv": {"status": "COMPLETE_FOR_QUERY"},
                                               "Chinese journals": {"status": "NOT_ATTEMPTED"}}})}

    def test_sections_links_and_no_loss(self):
        rows = [row("PMID:111", "Rectal cancer clinical practice guideline"),
                row("PMID:222", "Neoadjuvant organ preservation in rectal cancer"),
                row("PMID:333", "Machine learning for colorectal diagnosis", priority="high"),
                row("PMID:444", "Unrelated work", decision="exclude"),
                row("PMID:555", "Extracellular vesicle effects in tumor biology")]
        blocks, total = rf.build_report(self.state, rows)
        self.assertEqual(total, 4)
        kinds = [b["type"] for b in blocks]
        self.assertEqual(kinds[0:2], ["callout", "table_of_contents"])
        headings = [rf._canonical(b)["content"][0][0] for b in blocks if b["type"]=="heading_1"]
        self.assertEqual(headings[0], "本周核心结论")
        self.assertIn("科普选题建议", headings)
        self.assertIn("检索审计与局限", headings)
        self.assertIn("A. 临床重点研究、风险分层与指南", headings)
        self.assertIn("B. 直肠癌新辅助治疗、器官保留与精准手术", headings)
        self.assertIn("F. 结直肠癌预测模型与医学AI", headings)
        self.assertIn("G. 基础与转化研究", headings)
        view = json.dumps([rf._canonical(b) for b in blocks], ensure_ascii=False)
        self.assertEqual(kinds.count("heading_2"), 4)
        self.assertNotIn("Unrelated work", view)
        self.assertIn("有待进一步外部验证", view)
        for pmid in ("111","222","333","555"):
            self.assertIn("https://pubmed.ncbi.nlm.nih.gov/"+pmid+"/", view)
        self.assertIn("NOT_ATTEMPTED", view)
        self.assertIn("PARTIAL", view)

    def test_empty_included_and_missing_analysis(self):
        blocks, count = rf.build_report(self.state, [row("PMID:1", "Other", decision="exclude")])
        self.assertEqual(count,0)
        self.assertEqual([b["type"] for b in blocks].count("heading_2"),0)
        with self.assertRaisesRegex(ValueError,"Missing AI decisions"):
            rf.build_report(self.state,[{"payload":None}])

    def test_text_splitting_preserves_long_analysis(self):
        x="多项研究" * 850
        segments=rf._text(x)
        self.assertEqual("".join(s["text"]["content"] for s in segments),x)
        self.assertTrue(all(len(s["text"]["content"]) <= 1500 for s in segments))

    def test_historical_style_preserves_words_links_and_original_title(self):
        blocks,_=rf.build_report(self.state,[row('PMID:111','Rectal cancer trial')])
        self.assertEqual(blocks[0]['callout']['color'],'blue_background')
        labels=[b['bulleted_list_item']['rich_text'] for b in blocks if b['type']=='bulleted_list_item']
        original=next(x for x in labels if x[0]['text']['content']=='英文原题：')
        self.assertTrue(original[0]['annotations']['bold'])
        self.assertEqual(''.join(x['text']['content'] for x in original),'英文原题：Rectal cancer trial')
        self.assertEqual(rf.format_blocks(blocks),blocks)


class LayoutUpgradeTest(unittest.TestCase):
    def setUp(self):
        import layout_upgrade as up
        import partial_publish as pp
        self.up,self.pp=up,pp
        self.store=pipeline.Store(':memory:');self.store.migrate();self.addCleanup(self.store.db.close)
        self.run='weekly-literature:2026-10-19:Asia/Shanghai'
        self.blocks=[];self.fail_after=None;self.writes=0
        self.entries=[{'number':1,'page':'synthetic-batch','count':50,'included':40,
                       'progress':{'selected':100,'analyzed':60}}]

    def init_page(self,artifact,blocks):
        self.blocks=[dict(copy.deepcopy(b),id='block'+str(i)) for i,b in enumerate(blocks)]
        self.store.sql('INSERT INTO wl_publications(run_key,page_id,status,content_hash) VALUES(?,?,?,?)',
                       (artifact,'page','VERIFIED',self.up.fingerprint([rf._canonical(b) for b in blocks])))

    def read(self,page): return copy.deepcopy(self.blocks)

    def patch(self,url,body):
        self.writes+=1
        if 'children' in body:
            self.blocks.extend(dict(copy.deepcopy(b),id='new'+str(self.writes)+'-'+str(i)) for i,b in enumerate(body['children']))
        elif body.get('archived'):
            self.blocks=[b for b in self.blocks if b['id']!=url.rsplit('/',1)[1]]
        else:
            block=next(b for b in self.blocks if b['id']==url.rsplit('/',1)[1]);block.update(copy.deepcopy(body))
        if self.writes==self.fail_after: raise TimeoutError('committed, response lost')

    def test_index_archive_timeout_preserves_entries_and_resumes_without_duplicate(self):
        artifact='progress:'+self.run
        self.init_page(artifact,self.pp.legacy_progress_blocks(self.run,self.entries))
        self.fail_after=1
        with patch.object(self.up,'read',self.read),patch.object(self.up,'patch',self.patch):
            with self.assertRaises(TimeoutError): self.up.upgrade_progress(self.store,self.run,self.entries)
            self.fail_after=None
            self.up.upgrade_progress(self.store,self.run,self.entries)
            count=self.writes;self.up.upgrade_progress(self.store,self.run,self.entries)
        self.assertEqual(count,self.writes)
        self.assertEqual([rf._canonical(b) for b in self.blocks],
                         [rf._canonical(b) for b in self.pp.progress_blocks(self.run,self.entries)])

    def test_batch_style_timeout_replays_and_keeps_science_unchanged(self):
        artifact='partial:'+self.run+':1'
        old=[rf._block('callout','处理中｜摘要',icon={'type':'emoji','emoji':'📚'}),
             rf._block('bulleted_list_item','研究解读：结果RR1.2，95%CI未报告。')]
        self.init_page(artifact,old);self.fail_after=1
        with patch.object(self.up,'read',self.read),patch.object(self.up,'patch',self.patch):
            with self.assertRaises(TimeoutError): self.up.restyle_batch(self.store,artifact)
            self.fail_after=None;self.up.restyle_batch(self.store,artifact)
        before=''.join(v[0] for b in old for v in rf._canonical(b).get('content',[]))
        after=''.join(v[0] for b in self.blocks for v in rf._canonical(b).get('content',[]))
        self.assertEqual(before,after)
        self.assertEqual(self.store.sql('SELECT content_hash FROM wl_publications')[0]['content_hash'],
                         self.up.fingerprint([rf._canonical(b) for b in self.blocks]))

    def test_unknown_edits_fail_before_archiving_or_styling(self):
        artifact='progress:'+self.run
        self.init_page(artifact,self.pp.legacy_progress_blocks(self.run,self.entries))
        self.blocks[0]=dict(rf._block('paragraph','manual notes'),id='block0')
        with patch.object(self.up,'read',self.read),patch.object(self.up,'patch',self.patch):
            with self.assertRaises(ValueError): self.up.upgrade_progress(self.store,self.run,self.entries)
        self.assertEqual(self.writes,0)


class BlockWriteTest(unittest.TestCase):
    def setUp(self):
        self.store=pipeline.Store(":memory:")
        self.store.migrate()
        self.addCleanup(self.store.db.close)
        self.store.sql("INSERT INTO wl_publications(run_key,page_id) VALUES(?,?)",("run","page"))
        self.blocks=[]
        self.calls=0
        self.raise_after_commit=False

    def read(self, url, **kwargs):
        start=int(url.split("start_cursor=")[1]) if "start_cursor=" in url else 0
        end=min(start+100,len(self.blocks))
        return json.dumps({"results":self.blocks[start:end],"has_more":end<len(self.blocks),
                           "next_cursor":str(end) if end<len(self.blocks) else None}).encode()

    def write(self,req,**kwargs):
        self.calls+=1
        inserted=json.loads(req.data)["children"]
        self.blocks.extend(inserted)
        if self.raise_after_commit:
            self.raise_after_commit=False
            raise TimeoutError("server committed, response lost")
        class Response:
            def __enter__(self): return self
            def __exit__(self,*a): pass
            def read(self): return b"{}"
        return Response()

    def test_paged_write_retries_without_duplicate_after_timeout(self):
        expected=[rf._block("paragraph","Line "+str(i)) for i in range(127)]
        self.raise_after_commit=True
        with patch.object(rf,"request",self.read),patch("urllib.request.urlopen",self.write):
            with self.assertRaises(TimeoutError):
                rf.publish_report_blocks(self.store,"run","page",expected,"token")
            self.assertEqual(len(self.blocks),50)
            rf.publish_report_blocks(self.store,"run","page",expected,"token")
            self.assertEqual(len(self.blocks),127)
            self.assertEqual(self.calls,3)
            rf.publish_report_blocks(self.store,"run","page",expected,"token")
            self.assertEqual(self.calls,3)
        self.assertEqual(self.store.sql("SELECT status FROM wl_publications")[0]["status"],"VERIFIED")

    def test_edited_page_is_not_overwritten(self):
        self.blocks=[rf._block("paragraph","Human edit")]
        with patch.object(rf,"request",self.read),patch("urllib.request.urlopen") as write:
            with self.assertRaisesRegex(ValueError,"preserve user edits"):
                rf.publish_report_blocks(self.store,"run","page",[rf._block("heading_1","Expected")],"token")
            write.assert_not_called()

    def test_verified_hash_prevents_format_change(self):
        blocks=[rf._block("paragraph","Original")]
        with patch.object(rf,"request",self.read),patch("urllib.request.urlopen",self.write):
            rf.publish_report_blocks(self.store,"run","page",blocks,"token")
            with self.assertRaisesRegex(ValueError,"content conflict"):
                rf.publish_report_blocks(self.store,"run","page",[rf._block("paragraph","changed")],"token")


class ChineseTitleTest(unittest.TestCase):
    def test_digestive_categories_never_use_basic_as_fallback(self):
        self.assertTrue(rf.digestive_category({'title':'Frailty and enhanced recovery after colorectal surgery'}).startswith('D.'))
        self.assertTrue(rf.digestive_category({'title':'Deep learning for gastric cancer'}).startswith('F.'))
        self.assertTrue(rf.digestive_category({'title':'Colorectal cancer molecular signaling pathway in mouse cells'}).startswith('G.'))
        self.assertTrue(rf.digestive_category({'title':'Small bowel clinical cohort'}).startswith('H.'))

    def test_adoption_updates_only_presentation_receipts(self):
        import layout_upgrade as up
        import partial_publish as pp
        for legacy in (False,True):
            with self.subTest(legacy=legacy):
                store=pipeline.Store(':memory:');store.migrate();self.addCleanup(store.db.close)
                state={'run_key':'synthetic-run','window_start':'2026-10-01','window_end':'2026-10-07','audit':'{}'}
                paper=row('PMID:111','Rectal trial');artifact='partial:synthetic-run:1'
                store.sql('INSERT INTO wl_runs VALUES(?,?,?,?,?,?)',('synthetic-run','hash',state['window_start'],state['window_end'],'READY','{}'))
                store.sql('INSERT INTO wl_candidates VALUES(?,?,?,?)',('synthetic-run','PMID:111',1,1))
                store.sql('INSERT INTO wl_articles VALUES(?,?)',('PMID:111',paper['article']))
                store.sql('INSERT INTO wl_analysis VALUES(?,?,?)',('synthetic-run','PMID:111',paper['payload']))
                store.sql('INSERT INTO wl_checkpoints VALUES(?,?,?)',('synthetic-run',pp.SHOWN+'PMID:111',artifact))
                blocks,included=rf.build_report(state,[paper])
                blocks[0]=rf._block('callout','处理中｜分批同步，不是最终周报。本批复核1篇、纳入1篇。摘要分析，待专业审阅。',icon={'type':'emoji','emoji':'📚'})
                blocks[-2]=rf._block('paragraph','本批送达仅代表上述分析已读回核验，运行仍在处理中；来源覆盖和医学审核均不因此完成。')
                blocks=rf.format_blocks(blocks)
                original=copy.deepcopy(blocks)
                if legacy:
                    original=[b for b in original if not up.block_text(b).startswith('英文原题：')]
                    for b in original:
                        for t in b[b['type']].get('rich_text',[]):
                            t['text']['content']=t['text']['content'].replace('科普候选／证据边界：','科普候选与证据边界：')
                translated=[]
                for b in copy.deepcopy(original):
                    text=up.block_text(b)
                    if text.startswith('英文原题：'): continue
                    if b['type']=='heading_2':
                        translated.extend([rf._block('heading_2','1. 直肠试验'),rf._block('bulleted_list_item','英文原题： Rectal trial')])
                    else:
                        if b['type']=='bulleted_list_item' and ('Rectal trial｜' in text):
                            b=rf._block('bulleted_list_item',text.replace('Rectal trial｜','直肠试验｜'))
                        translated.append(b)
                store.sql('INSERT INTO wl_publications VALUES(?,?,?,?)',(artifact,'synthetic-page','VERIFIED',up.fingerprint([rf._canonical(b) for b in original])))
                with patch.object(up,'read',return_value=translated) as read,patch.object(up,'patch') as write:
                    up.adopt_title_translations(store,'synthetic-run',{'number':1,'count':1})
                    up.adopt_title_translations(store,'synthetic-run',{'number':1,'count':1})
                    self.assertEqual(read.call_count,1);write.assert_not_called()
                self.assertEqual(store.sql('SELECT content_hash FROM wl_publications')[0]['content_hash'],up.fingerprint([rf._canonical(b) for b in translated]))
                self.assertEqual(store.sql('SELECT payload FROM wl_analysis')[0]['payload'],paper['payload'])
                self.assertEqual(store.sql('SELECT status FROM wl_runs')[0]['status'],'READY')
                self.assertEqual(store.sql('SELECT * FROM wl_history'),[])

    def sample(self):
        from layout_upgrade import validate_title_translation
        self.validate=validate_title_translation
        self.original=[rf._block('bulleted_list_item','Rectal trial｜RR1.2'),
            rf._block('heading_2','1. Rectal trial'),
            rf._block('bulleted_list_item','研究解读：RR1.2'),
            rf._block('bulleted_list_item','[LOW] Rectal trial｜边界不变')]
        self.translated=[rf._block('bulleted_list_item','直肠试验｜RR1.2'),
            rf._block('heading_2','1. 直肠试验'),
            rf._block('bulleted_list_item','英文原题： Rectal trial'),
            rf._block('bulleted_list_item','研究解读：RR1.2'),
            rf._block('bulleted_list_item','[LOW] 直肠试验｜边界不变')]

    def test_accepts_only_bound_title_translation(self):
        self.sample()
        self.assertEqual(self.validate(self.original,self.translated),{'Rectal trial':'直肠试验'})

    def test_rejects_changed_result(self):
        self.sample()
        self.translated[3]=rf._block('bulleted_list_item','研究解读：RR1.3')
        with self.assertRaisesRegex(ValueError,'Non-title content changed'):
            self.validate(self.original,self.translated)

    def test_rejects_wrong_or_missing_original(self):
        self.sample()
        self.translated[2]=rf._block('bulleted_list_item','英文原题： Other trial')
        with self.assertRaisesRegex(ValueError,'Original title binding changed'):
            self.validate(self.original,self.translated)
        del self.translated[2]
        with self.assertRaisesRegex(ValueError,'Original title binding changed'):
            self.validate(self.original,self.translated)

    def test_rejects_link_change(self):
        self.sample()
        self.original[2]['bulleted_list_item']['rich_text'][0]['text']['link']={'url':'https://example.org/evidence'}
        self.translated[3]['bulleted_list_item']['rich_text'][0]['text']['link']={'url':'https://example.org/other'}
        with self.assertRaisesRegex(ValueError,'Non-title content changed'):
            self.validate(self.original,self.translated)

    def test_rejects_markup_artifacts_and_nested_content(self):
        self.sample()
        self.translated[0]=rf._block('bulleted_list_item','直肠试验｜</b>RR1.2')
        with self.assertRaisesRegex(ValueError,'Non-title content changed'):
            self.validate(self.original,self.translated)
        self.sample();self.translated[0]['has_children']=True
        with self.assertRaisesRegex(ValueError,'Nested or unsupported content'):
            self.validate(self.original,self.translated)


class CardMetadataTest(unittest.TestCase):
    def test_updates_title_status_and_icon_without_raw_run_title(self):
        properties={
            "任务批次":{"type":"title"},
            "执行日期":{"type":"date"},
            "原始总数":{"type":"number"},
            "去重后文献数":{"type":"number"},
            "摘要":{"type":"rich_text"},
            "处理状态":{"type":"select","select":{"options":[{"name":"完成"},{"name":"数据不完整"}]}}
        }
        seen=[]
        def output(req,**kwargs):
            seen.append(json.loads(req.data))
            class Response:
                def __enter__(self):return self
                def __exit__(self,*args):pass
                def read(self):return b"{}"
            return Response()
        audit={"raw_hits":1000,"selected":100,"supplementary_sources":{"Europe PMC":{"status":"PARTIAL"}}}
        def read(url, **kwargs):
            return json.dumps({'properties':seen[-1]['properties'] if seen else properties}).encode()
        with patch.object(rf,"request",side_effect=read),patch("urllib.request.urlopen",output):
            rf.update_report_metadata("weekly-literature:2026-10-12:Asia/Shanghai","page",41,audit,"token","db")
        data=seen[0]
        self.assertEqual(data["properties"]["任务批次"]["title"][0]["text"]["content"],"2026-10-12｜每周文献（41项）")
        self.assertEqual(data["properties"]["处理状态"]["select"]["name"],"数据不完整")
        self.assertEqual(data["properties"]["去重后文献数"]["number"],41)
        self.assertEqual(data["icon"]["emoji"],"📫")

    def test_success_response_without_correct_count_fails_readback(self):
        schema={'properties':{'去重后文献数':{'type':'number'}}}
        response={'properties':{'去重后文献数':{'number':99}}}
        class Response:
            def __enter__(self):return self
            def __exit__(self,*args):pass
            def read(self):return b'{}'
        with patch.object(rf,'request',side_effect=[json.dumps(schema).encode(),json.dumps(response).encode()]),patch('urllib.request.urlopen',return_value=Response()):
            with self.assertRaisesRegex(ValueError,'metadata readback mismatch'):
                rf.update_report_metadata('weekly-literature:2026-10-19:Asia/Shanghai','page',96,{},'token','db')


if __name__=="__main__":
    unittest.main()




