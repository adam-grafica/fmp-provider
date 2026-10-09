#!/usr/bin/env bash
# Verificación end-to-end de FMP directo (sin gateway).
set -euo pipefail
BASE="${FMP_BASE:-http://127.0.0.1:2089}"
echo "== health =="
curl -s -m 10 "$BASE/health"
echo; echo "== models =="
curl -s -m 10 "$BASE/v1/models" | python3 -c "import json,sys; d=json.load(sys.stdin); print([m['id'] for m in d['data']])"
echo "== stream OAI (cuento largo) =="
curl -N -s -m 120 -X POST "$BASE/v1/chat/completions" -H 'Content-Type: application/json' \
  -d '{"messages":[{"role":"user","content":"Escribe un cuento largo de al menos 800 palabras sobre un robot explorador en Marte."}],"model":"sol","stream":true}' \
  -o /tmp/fmp_verify_oai.txt -w 'HTTP:%{http_code} SIZE:%{size_download}\n'
python3 -c "
import json
full='';n=0;stop=False
for line in open('/tmp/fmp_verify_oai.txt'):
    line=line.strip()
    if line=='data: [DONE]': stop=True; continue
    if line.startswith('data: '):
        n+=1
        try:
            for c in json.loads(line[6:]).get('choices',[]):
                d=c.get('delta',{})
                if isinstance(d.get('content'),str): full+=d['content']
        except: pass
print('OAI chunks:',n,'chars:',len(full),'DONE:',stop,'INTEGRO:',len(full)>2000 and stop)
"
echo "== stream ANT (cuento largo) =="
curl -N -s -m 120 -X POST "$BASE/v1/messages" -H 'Content-Type: application/json' -H 'anthropic-version: 2023-06-01' \
  -d '{"messages":[{"role":"user","content":"Escribe un cuento largo de al menos 800 palabras sobre un robot explorador en Marte."}],"model":"claude-sonnet-5","max_tokens":4000,"stream":true}' \
  -o /tmp/fmp_verify_ant.txt -w 'HTTP:%{http_code} SIZE:%{size_download}\n'
python3 -c "
import json
full='';n=0;stop=False
for line in open('/tmp/fmp_verify_ant.txt'):
    line=line.strip()
    if line=='data: {\"type\": \"message_stop\"}': stop=True
    if line.startswith('data: '):
        try:
            j=json.loads(line[6:]); n+=1
            if isinstance(j.get('delta',{}).get('text'),str): full+=j['delta']['text']
        except: pass
print('ANT deltas:',n,'chars:',len(full),'STOP:',stop,'INTEGRO:',len(full)>2000 and stop)
"
echo "== no-stream OAI =="
curl -s -m 120 -X POST "$BASE/v1/chat/completions" -H 'Content-Type: application/json' \
  -d '{"messages":[{"role":"user","content":"Di hola mundo"}],"model":"sol"}' | head -c 400; echo
