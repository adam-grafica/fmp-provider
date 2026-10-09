# FMP — FreeModels Provider (v2.1.0)

Proveedor local OpenAI-compatible + Anthropic-compatible + Responses API
sobre el worker de `freemodels.pro`. Sin dependencias (solo Node 18+).

## Qué ofrece

- **Streaming en vivo real**: pass-through inmediato, TTFB ~3ms. Sin
  buffer total: ves los tokens llegar como en una API nativa.
- **Cierre honesto**: el worker upstream corta el SSE sin `data: [DONE]`
  de forma intermitente. FMP reintenta cuando nada se emitió y cierra
  con `length`/`max_tokens` cuando ya salió texto (sin Frankenstein).
- **`stream_mode: full`**: bufferiza íntegro vía pipeline con retry +
  fallback JSON y luego emite SSE (para rachas malas de upstream).
- **Límite `max_tokens` real**: el worker lo ignora; FMP trunca y avisa
  con `length`/`max_tokens`/`incomplete` (igual que API nativa).
  Default 4096 en chat, 1024 en messages ( Anthropic exige el campo).
- **Tool-use nativo** en las 3 rutas (puente `EXEC:::<tool> ARGS <json>`
  con 3 niveles: EXEC → bloque shell → reintento dirigido).
- **Plan premium**: tiers `free`/`pro` por API key, rate-limit
  (rpm + concurrencia), `429` con `retry-after`, métricas en
  `/health` y `GET /v1/usage`.
- **Thinking honesto**: el worker NO expone canal thinking separado
  (verificado en bundle + SSE). `thinking:true` se reenvía y el modelo
  razona paso a paso en el texto. FMP no fabrica bloques thinking.

## Uso

```bash
node server.js                    # 127.0.0.1:2089
PORT=2089 HOST=127.0.0.1 FMP_MAX_RETRIES=5 node server.js
# Premium: FMP_KEYS="sk-x:free,sk-y:pro" FMP_FREE_RPM=10 FMP_PRO_RPM=120
```

## Endpoints

| Método | Ruta | Notas |
|---|---|---|
| GET | `/health`, `/` | estado + `stats` + `tiers` + modelos |
| GET | `/v1/models`, `/models` | 8 modelos |
| GET | `/v1/usage`, `/usage` | métricas por tier |
| POST | `/v1/chat/completions` | OpenAI, `stream` + `stream_mode: full` |
| POST | `/v1/messages` | Anthropic, `stream` + `stream_mode: full` |
| POST | `/v1/responses` | Responses API (Codex), `stream` |

`model` acepta id pelado (`sol`) o con prefijo 9Router (`fmp-oai/sol`).

## Puente agéntico (tools)

Codex manda ~20 tools y espera `function_call`. El worker no habla
Responses nativo, así que FMP traduce:

1. Inyecta catálogo de tools como prompt sistema, protocolo `EXEC:::<tool> ARGS <json>`.
2. Convierte `EXEC:::` a `function_call` / `tool_calls` / `tool_use` nativos.
3. Fallbacks: primer bloque shell del texto → `exec_command`; reintento
   dirigido que pide SOLO el bloque.
4. Rondas `function_call_output` / `role: tool` / `tool_result` se mapean
   a mensajes del worker para continuar la conversación agéntica.

Límite conocido: con el contexto completo de Codex (20 tools +
environment con mención a sandbox), el worker a veces se niega a emitir
EXEC; el puente convierte cuando el worker coopera. Codex pide
aprobación antes de ejecutar: no hay ejecución ciega.

## Modelos (8)

`claude-opus-5.5`, `claude-sonnet-5`, `claude-fable-5`, `claude-fable-5.1`,
`sol`, `terra`, `glm-5.2`, `kimi-k3`

## Variables

| Var | Default | Qué hace |
|---|---|---|
| `PORT` / `HOST` | `2089` / `127.0.0.1` | bind |
| `FMP_UPSTREAM` | worker freemodels | override upstream |
| `FMP_TIMEOUT_MS` | `120000` | timeout por intento |
| `FMP_MAX_RETRIES` | `5` | reintentos (solo si nada emitido) |
| `FMP_RETRY_BASE_MS` | `1500` | backoff lineal base |
| `FMP_KEYS` | vacío (todo `local`, sin límite) | `key:tier,key2:tier` |
| `FMP_MAX_TOKENS_CEIL` | `8192` | techo: FMP nunca emite más aunque el cliente pida más |
| `FMP_FREE_RPM` / `FMP_FREE_CONC` | `10` / `2` | límite tier free |
| `FMP_PRO_RPM` / `FMP_PRO_CONC` | `120` / `16` | límite tier pro |
| `FMP_TRACE` (`SHIM_TRACE`) | vacío | archivo de trace (debug) |

## Registro en 9Router

```bash
./register-9router.sh   # crea nodos fmp-oai + fmp-ant si faltan, backup sqlite
```

## Verificación

```bash
./verify.sh   # health + models + live/stream/full + caps + tool-use + tiers
```
