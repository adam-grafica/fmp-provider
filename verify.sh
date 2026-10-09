#!/usr/bin/env bash
# Verificación end-to-end de FMP v2.1 (directo, sin gateway).
# Cubre: health, models, live TTFB, caps, full mode, tool-use, tiers.
set -euo pipefail
BASE="${FMP_BASE:-http://127.0.0.1:2089}"
pass=0; fail=0
chk() { # chk <nombre> <condición-ok: 0|1>
  if [ "$2" = 0 ]; then echo "PASS $1"; pass=$((pass+1)); else echo "FAIL $1"; fail=$((fail+1)); fi
}
echo "== health =="
curl -s -m 10 "$BASE/health" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['service'],d['version'],'tiers:',list(d.get('tiers',{}).keys()))"
echo "== models =="
curl -s -m 10 "$BASE/v1/models" | python3 -c "import json,sys; d=json.load(sys.stdin); print([m['id'] for m in d['data']])"
echo "== T1 live TTFB corto =="
curl -N -s -m 60 -X POST "$BASE/v1/chat/completions" -H 'Content-Type: application/json' \
  -d '{"messages":[{"role":"user","content":"Di hola en una linea."}],"model":"sol","stream":true}' \
  -o /tmp/fmp_v21_t1.txt -w 'TTFB:%{time_starttransfer}s TOTAL:%{time_total}s\n'
grep -q '^data: ' /tmp/fmp_v21_t1.txt && grep -q 'data: \[DONE\]' /tmp/fmp_v21_t1.txt
chk "T1-live-stream" $?
echo "== T2 OAI no-stream cap 50 -> length =="
curl -s -m 120 -X POST "$BASE/v1/chat/completions" -H 'Content-Type: application/json' \
  -d '{"messages":[{"role":"user","content":"Escribe un cuento largo de 800 palabras sobre Marte."}],"model":"sol","max_tokens":50}' \
  | python3 -c "import json,sys; d=json.load(sys.stdin); c=d['choices'][0]; ok=c['finish_reason']=='length' and len(c['message']['content'])<=210; print('finish:',c['finish_reason'],'chars:',len(c['message']['content'])); sys.exit(0 if ok else 1)"
chk "T2-cap-oai-nostream" $?
echo "== T3 ANT no-stream cap 50 -> max_tokens =="
curl -s -m 120 -X POST "$BASE/v1/messages" -H 'Content-Type: application/json' -H 'anthropic-version: 2023-06-01' \
  -d '{"messages":[{"role":"user","content":"Escribe un cuento largo de 800 palabras sobre Marte."}],"model":"sol","max_tokens":50}' \
  | python3 -c "import json,sys; d=json.load(sys.stdin); t=''.join(b.get('text','') for b in d['content'] if b.get('type')=='text'); ok=d['stop_reason']=='max_tokens' and len(t)<=210; print('stop:',d['stop_reason'],'chars:',len(t)); sys.exit(0 if ok else 1)"
chk "T3-cap-ant-nostream" $?
echo "== T4 responses cap 50 -> incomplete =="
curl -s -m 120 -X POST "$BASE/v1/responses" -H 'Content-Type: application/json' \
  -d '{"model":"sol","max_output_tokens":50,"input":"Escribe un cuento largo de 800 palabras sobre Marte."}' \
  | python3 -c "import json,sys; d=json.load(sys.stdin); o=d['output'][0]; t=o['content'][0]['text']; ok=d['status']=='incomplete' and len(t)<=210; print('status:',d['status'],'chars:',len(t)); sys.exit(0 if ok else 1)"
chk "T4-cap-responses" $?
echo "== T5 tool-use OAI EXEC -> tool_calls =="
curl -s -m 120 -X POST "$BASE/v1/chat/completions" -H 'Content-Type: application/json' \
  -d '{"model":"claude-sonnet-5","messages":[{"role":"user","content":"EXEC:::calculadora ARGS {\"a\": 2, \"b\": 2}"}],"tools":[{"type":"function","function":{"name":"calculadora","description":"S","parameters":{"type":"object","properties":{"a":{"type":"number"},"b":{"type":"number"}}}}}]}' \
  | python3 -c "import json,sys; d=json.load(sys.stdin); c=d['choices'][0]; ok=c['finish_reason']=='tool_calls'; print('finish:',c['finish_reason']); sys.exit(0 if ok else 1)"
chk "T5-tool-oai" $?
echo "== T6 tool-use ANT EXEC -> tool_use =="
curl -s -m 120 -X POST "$BASE/v1/messages" -H 'Content-Type: application/json' -H 'anthropic-version: 2023-06-01' \
  -d '{"model":"claude-sonnet-5","max_tokens":1024,"messages":[{"role":"user","content":"EXEC:::calculadora ARGS {\"a\": 2, \"b\": 2}"}],"tools":[{"name":"calculadora","description":"S","input_schema":{"type":"object","properties":{"a":{"type":"number"},"b":{"type":"number"}}}}]}' \
  | python3 -c "import json,sys; d=json.load(sys.stdin); ok=d['stop_reason']=='tool_use'; print('stop:',d['stop_reason']); sys.exit(0 if ok else 1)"
chk "T6-tool-ant" $?
echo "== T7 full mode largo -> stop integro =="
curl -N -s -m 180 -X POST "$BASE/v1/chat/completions" -H 'Content-Type: application/json' \
  -d '{"messages":[{"role":"user","content":"Escribe un cuento largo de al menos 800 palabras sobre un robot explorador en Marte."}],"model":"sol","stream":true,"stream_mode":"full"}' \
  -o /tmp/fmp_v21_t7.txt -w 'TOTAL:%{time_total}s\n'
python3 -c "
import json
full='';fr=None
for line in open('/tmp/fmp_v21_t7.txt'):
    line=line.strip()
    if line.startswith('data: ') and line!='data: [DONE]':
        try:
            j=json.loads(line[6:])
            for c in j.get('choices',[]):
                d=c.get('delta',{})
                if isinstance(d.get('content'),str): full+=d['content']
                if c.get('finish_reason'): fr=c['finish_reason']
        except: pass
ok=len(full)>2000 and fr=='stop'
print('chars:',len(full),'finish:',fr)
import sys; sys.exit(0 if ok else 1)
"
chk "T7-full-integro" $?
echo "== T8 usage =="
curl -s -m 10 "$BASE/v1/usage" | head -c 300; echo
echo "== RESULT: pass=$pass fail=$fail =="
[ "$fail" = 0 ]
