# Sprint E2-02 — Registo durável, chunks e cifra no líder

Este relatório descreve o caminho de escrita local do líder e os ensaios dos critérios de aceitação. O formato binário do chunk e do `.hdr` está no [README](README.md#6-caminho-de-escrita-local-sprint-e2-02).

## Decisões de desenho

- **Um só caminho de escrita:** `/registar`, as mensagens em `#arquivo` e cada registo da `/rajada` chamam `Arquivo.registar(autor, canal, texto)`, que é `synchronized`. O temporizador de selagem também sincroniza no `Arquivo`, por isso o LSN, o `registos.log` e o chunk aberto nunca são alterados por duas threads ao mesmo tempo.
- **fsync antes da resposta:** a linha é escrita, `FileChannel.force(true)` é chamado e só depois o bot responde com o LSN. Se a escrita falhar, o `Arquivo` recusa novas escritas até reiniciar, porque não se sabe se a linha chegou ao disco e não se pode reutilizar o LSN.
- **O `/registar` funciona em qualquer canal** e guarda o canal de origem. A restrição a `#arquivo` aplica-se às **mensagens** normais; as de bots, de webhooks e de outros canais são ignoradas.
- **A `/rajada` corre numa thread própria**, para não bloquear a JDA, e responde logo; no fim publica no canal o intervalo de LSN e o tempo.
- **Selagem por tamanho:** se o registo seguinte fizesse o chunk passar de `CHUNK_MAX_BYTES`, o chunk é selado antes e o registo abre um chunk novo. Assim o chunk em claro nunca passa de 64 KB. Um registo sozinho maior do que o limite forma um chunk só dele.
- **Arranque:**
  - lê o `registos.log` e confirma que os LSN são 1, 2, 3…; uma última linha incompleta (nunca confirmada) é cortada;
  - abre todos os chunks de `selados/` para saber o último LSN selado. Se algum não abrir, recusa arrancar;
  - os registos com LSN maior do que o último selado voltam ao chunk aberto.
- **Paragem ordenada:** um *shutdown hook* sela o chunk aberto (`motivo=paragem`).
- **`master.key`:** gerada com `SecureRandom` e escrita com `CREATE_NEW`. No Windows, a ACL fica só com o dono (`R`, `D`, `WDAC`); em POSIX, `0400`.
- **Um líder por pasta:** o líder tranca `lider.lock` com `FileChannel.tryLock()`. Ver o ensaio 7.
- **Eventos sem texto:** `REGISTO` e `COMANDO` guardam o tamanho do texto, nunca o texto.

## Ensaios

Ambiente: Windows 11, JBR 25 (compilado com `--release 17`), Maven 3.9.9, JDA 5.2.1, servidor de teste «SDT 26/27».

### 1. Testes sem Discord (`--teste`)

- **21 casos** de validação dos comandos (E2-01): 0 falhas.
- **24 testes** de registo, chunk e cifra: 0 falhas. Incluem:
  - linha ↔ registo com `;` e mudanças de linha;
  - leitura inversa igual, padding múltiplo de 4096, chunk selado imutável, LSN não consecutivo recusado;
  - HKDF igual ao caso A.1 do RFC 5869 (primeiros 32 bytes `3cb25f25…c5bf`);
  - `open` falha com byte alterado no texto cifrado (hash), byte alterado com o id recalculado e byte alterado na tag (`AEADBadTagException`), outro `salt`, outro `nonce`, `keyVersion` 2, outra `master.key` e uma chave de 31 bytes;
  - a `keyVersion` faz parte da AAD.

### 2. No Discord (dados reais do líder)

| Ação | Resultado |
|---|---|
| `/registar ola mundo` em `#arquivo` | «Registado com **LSN 1**» |
| mensagem `teste` em `#arquivo` | «Registado com **LSN 2**» |
| mensagem em `#geral` | ignorada (nenhum evento) |
| 5 s depois | `SELADO … lsn=1..2;registos=2;tamanho=4096;cifrado=4112;motivo=tempo` |
| `/rajada 500 0` | `RAJADA;n=500;lsn=3..502;ms=1197` |
| | `lsn=3..188` e `189..374` com `tamanho=65536;motivo=tamanho` |
| | `lsn=375..502` com `tamanho=45056;motivo=tempo` |

- `registos.log`: 502 linhas, LSN 1..502 sem repetições nem saltos.
- Todos os tamanhos em claro (4096, 65536, 45056) são múltiplos de 4096. Cada `.bin` tem mais 16 bytes, que são a tag.
- 500 registos com fsync em 1,2 s, cerca de 2,4 ms por registo.

### 3. Ficheiros selados

- `sha256sum` de cada `.bin` = nome do ficheiro, nos 4 chunks.
- `grep -rl 'ola mundo' selados/`, `grep -rl 'teste' selados/` e `grep -rl 'registo sintético' selados/`: **0 ficheiros**.
- `--verificar`: os 4 chunks abrem. Os registos (LSN, instante, autor, canal, texto) são exatamente os do `registos.log`, e os LSN selados são 1..502, contíguos.

### 4. Reinício

Depois de parar e voltar a arrancar o líder:

```
ARQUIVO;ultimoLsn=502;chunksSelados=4;ultimoLsnSelado=502;porSelar=0
```

Ensaio sem Discord: depois de 501 registos, o registo seguinte recebeu o LSN 502.

### 5. Falhas provocadas (`--simular`, sem Discord)

| Falha | Resultado |
|---|---|
| 1 byte alterado num `.bin` | `--verificar`: «nome ≠ SHA-256 do conteúdo». No arranque: «hash do conteúdo diferente do chunkId… arranque recusado» |
| outra `master.key` de 32 bytes (`-Dtpf.masterKey`) | `open falhou: AEADBadTagException: Tag mismatch`; arranque recusado |
| `master.key` com 31 bytes | «tem 31 bytes (esperava 32): arranque recusado» |
| líder morto (`kill -9`) com 3 registos por selar | no reinício: `porSelar=3`, e 5 s depois `SELADO lsn=504..506;motivo=tempo` |
| última linha do `registos.log` cortada | `AVISO: última linha incompleta (42 bytes) descartada`; LSN 1..507 sem saltos |
| permissões | `icacls master.key` → `(R,D,WDAC)`, só o dono. Nem o dono consegue escrever por cima |

### 6. Selagem por tempo e por tamanho sem Discord

- **1 registo:** `REGISTO` em …0563 e `SELADO` em …5706, ou seja **5,14 s** depois.
- **500 registos com intervalo 0:** 2 chunks por tamanho (186 registos de ≈ 350 bytes, 65536 bytes) e 1 por tempo.

### 7. O que correu mal: três líderes ao mesmo tempo

Na primeira ronda de testes no Discord estavam ligados **três processos do bot** com o mesmo token:
- o líder da E2-01, que ficou a correr de uma sessão anterior;
- o líder novo, arrancado em segundo plano;
- o líder arrancado com o `executar.bat`.

O Discord entrega cada interação e cada mensagem **a todas as sessões**. Os dois líderes novos usavam a mesma pasta e escreveram cada registo duas vezes:
- o `registos.log` ficou com 1006 linhas e os LSN repetidos (1, 1, 2, 2…);
- cada chunk foi selado duas vezes (chunkIds diferentes, porque o salt e o nonce são aleatórios);
- só uma resposta chegou ao utilizador, porque o Discord só aceita uma resposta por interação.

**Correção:** o líder tranca `lider.lock` com `FileChannel.tryLock()` antes de abrir o arquivo. Um segundo líder na mesma pasta recusa arrancar:

```
ERRO;arranque recusado: já há um líder a correr nesta pasta (…\lider.lock está trancado). Fecha-o antes de arrancar outro.
```

O sistema operativo liberta o trinco quando o processo termina, mesmo depois de um `kill`. Os dados do ensaio duplicado foram guardados em `ensaio-duplicado/` (fora do git).

**Limite:** o trinco só protege a mesma pasta. Dois líderes em pastas ou máquinas diferentes, com o mesmo token, continuam a receber os mesmos comandos e a atribuir LSN independentes. Num sistema distribuído, isto pede uma eleição de líder ou um *lease*, como na COMINDIR05. Fica registado para os sprints seguintes.

## Critérios de aceitação

- [x] `/registar` responde com o LSN atribuído e a linha correspondente já está em `registos.log`: a resposta só sai depois do `force(true)`; LSN 1 = `ola mundo`.
- [x] As mensagens em `#arquivo` são registadas pelo mesmo caminho e as mensagens de bots ou de outros canais são ignoradas: LSN 2 = `teste`; a mensagem em `#geral` não gerou nenhum evento.
- [x] Depois de reiniciar o líder, os LSN continuam a partir do último valor em `registos.log`, sem repetições nem saltos: `ultimoLsn=502` no reinício; ensaio 4.
- [x] Um único registo dá origem a um chunk selado cerca de 5 s depois, e `/rajada 500 0` produz vários chunks selados por tamanho, todos com tamanho múltiplo de `PAD_BYTES`: ensaios 2 e 6.
- [x] Cada ficheiro em `selados/` tem como nome o SHA-256 do seu conteúdo e não contém texto em claro: ensaio 3.
- [x] O líder abre cada chunk selado e obtém exatamente os registos do intervalo indicado no evento `SELADO`: `--verificar`, ensaio 3.
- [x] Alterar um byte de um `.bin`, usar outro `salt`/`nonce`/`keyVersion` ou outra `master.key` faz `open` falhar com exceção: ensaios 1 e 5.
- [x] Os eventos `REGISTO` e `SELADO` aparecem em `eventos-lider.txt`, e o README descreve o formato binário do chunk e do `.hdr`.
