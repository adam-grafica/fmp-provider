# FMP — FreeModels Provider

Proveedor local OpenAI-compatible + Anthropic-compatible sobre el worker
de `freemodels.pro`. Sin dependencias (solo Node 18+).

## Por qué existe

El worker upstream (`freemodels-chat.freemodels.workers.dev`) **corta el
SSE de forma intermitente** (~1 de cada 3 respuestas largas): cierra la
conexión a mitad de frase **sin** emitir `data: [DONE]`. Evidencia
2026-10-09: 3 pedidos idénticos → 6766 chars DONE, 184 chars SIN DONE,
5917 chars DONE.

FMP pide `stream:true` al worker, acumula en buffer hasta ver `[DONE]`;
si el socket muere sin DONE, reintenta con backoff (hasta `FMP_MAX_RETRIES`)
y fusiona sin duplicar (el worker reemite desde el inicio). Si el SSE sigue
incompleto, pide el JSON no-stream (estable, 3/3 verificado) y se queda con
el mejor. Solo entonces emite al cliente. `finish_reason` refleja la
realidad: `stop` si hubo DONE o rescate JSON, `length` si todo falló.
Validación 2026-10-09: 3/3 OAI + 1/1 ANT íntegros directo, 1/1 OAI + 1/1 ANT
íntegros vía gateway 9Router.

## Uso

```bash
node server.js                    # 127.0.0.1:2089
PORT=2089 HOST=127.0.0.1 FMP_MAX_RETRIES=3 node server.js
```

## Endpoints

| Método | Ruta | Notas |
|---|---|---|
| GET | `/health`, `/` | estado + `stats` + modelos |
| GET | `/v1/models`, `/models` | 8 modelos |
| POST | `/v1/chat/completions` | OpenAI, `stream: true/false` |
| POST | `/v1/messages` | Anthropic, `stream: true/false` |
| POST | `/v1/responses` | Responses API (Codex), `stream: true/false` |

`model` acepta id pelado (`sol`) o con prefijo 9Router (`fmp-oai/sol`).

## Puente agéntico (`/v1/responses` + tools)

Codex manda ~20 tools y espera `function_call`. El worker no habla
Responses nativo, así que FMP traduce:

1. Inyecta catálogo de tools como prompt sistema, protocolo `EXEC:::<tool> ARGS <json>`.
2. Convierte `EXEC:::` a `function_call` nativo (stream + no-stream).
3. Fallbacks: primer bloque shell del texto → `exec_command`; reintento
   dirigido que pide SOLO el bloque.
4. Rondas `function_call` / `function_call_output` se mapean a mensajes
   del worker para continuar la conversación agéntica.

Límite conocido: el worker a veces genera el comando impreciso
(ej. escribe la salida esperada en vez del `echo`) y a veces se niega;
el `finish_reason`/`status` refleja el resultado. Codex pide aprobación
antes de ejecutar: no hay ejecución ciega.

## Modelos (8)

`claude-opus-5.5`, `claude-sonnet-5`, `claude-fable-5`, `claude-fable-5.1`,
`sol`, `terra`, `glm-5.2`, `kimi-k3`

## Variables

| Var | Default | Qué hace |
|---|---|---|
| `PORT` / `HOST` | `2089` / `127.0.0.1` | bind |
| `FMP_UPSTREAM` | worker freemodels | override upstream |
| `FMP_TIMEOUT_MS` | `120000` | timeout por intento |
| `FMP_MAX_RETRIES` | `3` | reintentos anti-truncamiento |
| `FMP_RETRY_BASE_MS` | `1500` | backoff lineal base |
| `FMP_TRACE` (`SHIM_TRACE`) | vacío | archivo de trace |

## Registro en 9Router

```bash
./register-9router.sh   # crea nodos fmp-oai + fmp-ant si faltan, backup sqlite
```

## Verificación

```bash
./verify.sh   # health + models + stream OAI + stream ANT + no-stream
```
