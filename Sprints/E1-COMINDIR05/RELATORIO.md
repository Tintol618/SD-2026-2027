# Sprint E1-COMINDIR05 — Eleição do Líder de Partição

## Estrutura

Três projetos, todas as classes no pacote `raft04`, tópicos com o prefixo `raft04`:

| Projeto | Classes |
|---|---|
| `raft04-produtor/src/raft04/`    | `IpfsPubSub`, `Partitioner`, `VectorClock`, `SensorRecord`, `Wire`, `Batch`, `HeartbeatMonitor`, **`Producer`** |
| `raft04-no/src/raft04/`          | `IpfsPubSub`, `Partitioner`, `VectorClock`, `SensorRecord`, `Wire`, `Batch`, **`VoteRequest`**, **`VoteReply`**, **`RaftState`**, **`PartitionNode`**, **`RaftCheck`**, `CausalCheck` |
| `raft04-processador/src/raft04/` | `IpfsPubSub`, `Partitioner`, `VectorClock`, `SensorRecord`, `Wire`, `Batch`, `HeartbeatMonitor`, **`AlarmProcessor`** |

- **`RaftState`**: está **como no enunciado**. Só tem os ganchos das falhas do ponto 10 (propriedade `raft04.erro`, que por omissão é 0 e não altera nada).
- **`PartitionNode`**: substitui o `PartitionLeader` e a `Replica` da ficha anterior. Os nove processos `PartitionNode <k> <i>` (k = 0..2, i = 1..3) são iguais e nenhum é configurado como líder.
- **`SensorRecord`**: ganhou o campo `termo`. A linha passa a ser `seq;termo;sensor;temperatura;instante;L;V`, com o instante ao milissegundo.
- **`Batch`**: o cabeçalho passa a ser `LOTE;<termo>;<lider>;<endereço>;<porto>;<hb>;<ultimoSeq>;<ultimoTermo>;<n>`. `decode(msg, n)` valida o número de linhas, e `decodeHeader` serve o produtor.
- **`VoteRequest`** e **`VoteReply`**: `PEDIDO_VOTO;<termo>;<candidato>;<ultimoSeq>;<ultimoTermo>` e `VOTO;<termo>;<eleitor>;<candidato>;<S|N>`, ambos com `encode()` e `decode(String)`.
- **`Wire`**:
  - deixa de ter o `inc` e tem `PERIODO_HB_MS` (`raft04.periodo`, 2000), `FATOR_TIMEOUT` (`raft04.fator`, 3), `VERIFICACAO_MS = 200`, `ELEICAO_MIN_MS`/`ELEICAO_MAX_MS` (`raft04.eleicaoMin`/`Max`, 3Δ e 6Δ) e `GRUPO = 3`;
  - `topicElection(k)` devolve `raft04-eleicao-p<k>`;
  - `nodePort(k, i)` é `7000 + 10k + i`;
  - `isolated(k, i)` é verdadeiro enquanto existir o ficheiro `isolar-p<k>-n<i>`.
- **`HeartbeatMonitor`** (produtor e processador): `onHeartbeat(term, leader, hb)` ignora termos inferiores, assinala `NOVO_LIDER` quando o termo sobe e `HB_PERDIDO` quando há um salto de `hb` dentro do mesmo termo.

### Execução

Em cada projeto: `javac --release 11 -encoding UTF-8 -d out src/raft04/*.java`. Todos os processos correm na **mesma pasta**. Os nós arrancam por qualquer ordem, depois o processador e por fim o produtor:

```bash
java -cp out raft04.PartitionNode 1 2            # nó n2 da partição p1 (k i [N]); os nove: k = 0..2, i = 1..3
java -cp out raft04.AlarmProcessor 3
java -cp out raft04.Producer 3                   # <sensor> <temp> | rajada <n> <ms> | sair
java -cp out raft04.RaftCheck 3 <pasta> [falhas.txt]   # escreve também <pasta>/raft-relatorio.txt
java -cp out raft04.CausalCheck 3 <pasta>
# opções (antes de -cp): -Draft04.eleicaoMin=<ms> -Draft04.eleicaoMax=<ms> -Draft04.periodo=<ms>
#                         -Draft04.inicio=<instante ms> (arranque sincronizado) -Draft04.erro=<1..6> (ponto 10)
```

- `executar.bat` faz isto de forma interativa: compila, garante o daemon IPFS e abre os 9 nós, o processador e o produtor em janelas próprias.
- `ensaios.sh` (Git Bash) corre todos os ensaios dos pontos 9 e 10, cada um em `execucao/<ensaio>/`, e termina com `RaftCheck` e `CausalCheck` (`verificacao.txt`).
- Para **isolar** um nó (partição de rede), cria-se o ficheiro `isolar-p<k>-n<i>` na pasta de execução, e apaga-se para reparar.

### Decisões de desenho

- **Nó** (`PartitionNode`): tem um monitor próprio (`lock`). Sempre que precisa dos dois, a ordem é **`lock` do nó → monitor do `RaftState`**. O `RaftState` nunca chama o nó, e o único *callback* (`event`) usa um terceiro lock (`evLock`) que não adquire mais nenhum. Não há, por isso, nenhuma ordem inversa possível.
- **Registos do nó:** estão em memória (`List<SensorRecord>`, índice = `seq − 1`), espelhados em `no-p<k>-n<i>.txt`. O `append` acrescenta uma linha, e a truncagem reescreve o ficheiro de forma atómica (temporário + `ATOMIC_MOVE`).
- **Thread de verificação:** de 200 em 200 ms, dentro do `lock` do nó, chama `tick()`. Se devolver um termo, lê o `ultimoSeq` e o `ultimoTermo` locais e, **fora** do lock, publica o `PEDIDO_VOTO`. O estado já foi gravado dentro do `tick()`.
- **Mensagens de eleição:**
  - por cada `PEDIDO_VOTO` de outro nó: `onVoteRequest` com o último registo local, dentro do `lock`, e publicação do `VOTO` (mesmo a recusar) fora do lock;
  - por cada `VOTO` dirigido a este nó: `onVote` e, se devolver `true`, `becomeLeader()`;
  - as mensagens que o próprio nó publicou são ignoradas, pelo candidato, pelo eleitor e pelo líder.
- **`becomeLeader()`** (dentro do `lock`): `seq` = `ultimoSeq` local, *L* e *V* a partir do último registo do ficheiro (ou zero, se estiver vazio), `hb = 0`, `pendentes` vazia, e `scheduleAtFixedRate(enviarLote, 0, Δ)`.
- **`onIn`:**
  - dentro do `lock`, `t = termIfLeader()`. Se for −1, assinala `IGNORADO`;
  - se não, aplica as regras da COMINDIR03 (Lamport, vetor, verificação `V[k] = seq`) e grava o registo com o termo `t`;
  - o tópico `raft04-in-p<k>` é subscrito pelos três nós, mas só o líder atribui `seq`.
- **`enviarLote()`:**
  - dentro do `lock`: se `termIfLeader()` for −1, cancela o agendamento e termina. Se não, troca `pendentes` e lê `ultimoSeq`, `ultimoTermo` e `++hb`;
  - fora do lock: publica e escreve `hb-p<k>-n<i>.txt`;
  - depois de qualquer chamada ao `RaftState`, `afterRaft()` cancela o agendamento se o nó deixou de ser líder.
- **Seguidor** (`onData` → `applyBatch`): por cada registo do lote:
  - **duplicado** (mesmo `seq` e mesmo termo): ignorado;
  - **divergência** (mesmo `seq`, outro termo);
  - **acrescento** (`seq` = último + 1);
  - **lacuna** (retido; acima de `LIMITE_RETIDOS` recupera por TCP).

  No fim compara com `ultimoSeq`/`ultimoTermo`. Se lhe faltam registos, recupera-os **pedindo também o seu último registo**, para confirmar que coincide com o do líder. Assim, uma divergência escondida atrás de uma falta também é apanhada.
- **Divergência** (`resolveDivergence`):
  1. pede ao líder os registos a partir do início do seu último bloco de termo;
  2. recua um bloco de cada vez enquanto o primeiro registo recebido não coincidir;
  3. trunca a partir do primeiro `seq` diferente e aplica os do líder;
  4. assinala `TRUNCADO;p<k>;de=…;descartados=…;termos=[…]`.

  Os registos descartados são guardados em `truncados-p<k>-n<i>.txt` (instante da truncagem e linha). É um ficheiro a mais do que o enunciado pede, para o `RaftCheck` poder dizer se as leituras truncadas existem no registo final com outro `seq`.
- **Recuperação TCP:** **qualquer** nó serve o seu ficheiro em `nodePort(k, i)`. O seguidor pede ao endereço e porto do cabeçalho do último `LOTE`.
- **Isolamento** (`Wire.isolated`):
  - não se publica nem se processa nada nos tópicos de dados e de eleição;
  - o servidor TCP fecha as ligações, e o cliente falha;
  - o tópico de entrada **não** é afetado: um líder isolado continua a receber leituras e a atribuir `seq`;
  - um seguidor isolado continua a fazer `tick()`, por isso o seu termo sobe a cada prazo.
- **Processador:**
  - ignora lotes de termo menor do que o maior já visto nessa partição;
  - trata cada registo pelo par (`seq`, `termo`);
  - avisa `AVISO: partição p<j> sem líder conhecido` antes de publicar um alarme para uma partição suspeita.
- **Produtor:** um `HeartbeatMonitor` por partição (só lê o cabeçalho), o mesmo `AVISO` antes de cada `IN` e `produtor.txt` com `instante;p<k>;sensor;temperatura`.
- **Eventos dos nós:**
  - `ARRANQUE` (a mais do que o enunciado pede: marca cada arranque, para medir o tempo até ao primeiro líder e verificar o termo depois de um reinício);
  - `CANDIDATO`, `VOTO`, `LIDER`, `NOVO_LIDER`, `TERMO` e `DEMISSAO`, vindos do `RaftState`;
  - `RECEBIDO`, `RECUPERACAO`, `TRUNCADO`, `IGNORADO` e `ERRO`.
- **`RaftCheck <N> <pasta> [falhas.txt]`:**
  - **termos:** candidatos, votos concedidos por candidato e líder de cada termo, ou «voto dividido» se houve mais de um candidato e nenhum líder;
  - **segurança:** termos com mais de um `LIDER`, nós cujo termo diminuiu, votos duplos no mesmo termo e `ERRO`. Mostra também os **líderes simultâneos**, ou seja, períodos em que dois nós se julgavam líderes (de `LIDER` até `DEMISSAO`, ou até ao último lote se o nó falhou);
  - **tempo até ao primeiro líder:** a partir do primeiro `ARRANQUE`;
  - **indisponibilidade:** de cada falha até ao primeiro `LOTE` de um termo superior, com as leituras enviadas nesse intervalo;
  - **leituras perdidas:** cada linha de `produtor.txt` é emparelhada com um registo do mesmo sensor e temperatura atribuído até 3 s depois, na união dos registos dos três nós;
  - **truncagens:** quantos dos registos descartados existem no registo final com outro `seq`;
  - **coincidência final:** os `seq` em que os três ficheiros diferem.

### Ambiente dos testes

- Windows 11, Kubo 0.43.1 (um nó, API 5001), todos os processos na mesma máquina.
- Java: JBR 25 do Android Studio, compilando com `--release 11`, porque o PC não tem outro JDK.
- 9 nós, o processador e o produtor, cada um num JVM com `-Xmx96m`.

**Recusas do daemon.** Como nas fichas anteriores, o Kubo recusa algumas publicações com `pub falhou (500): validation ignored` quando vários processos publicam ao mesmo tempo. Aqui isso fez perder, por exemplo, um `VOTO` concedido no ensaio 9(b) (`fixo-4`, p1: o termo 1 ficou sem líder, apesar de o n2 ter recebido um voto). Para o algoritmo, é uma **perda de mensagem**: o RAFT tolera-a, porque o prazo seguinte abre um termo novo.

---

## 2.2 · Diagnóstico inicial

**(a)**
- **Duas cópias:** a maioria de 2 é 2. Uma eleição por maioria **não tolera nenhuma falha**, porque o sobrevivente sozinho nunca tem 2 votos.
- **Três cópias:** a maioria é 2 e tolera-se **uma** falha.
- **Quatro nós:** a maioria é 3, logo continua a tolerar só **uma** falha. Além disso, com dois candidatos podem ficar empatados a 2–2. Há mais mensagens, mais cópias e mais hipóteses de voto dividido, sem tolerar mais falhas. Por isso usam-se grupos ímpares.

**(b)**
- **Dois candidatos não podem ter maioria no mesmo termo:** cada um precisaria de 2 votos, ou seja 4 votos no total, mas só há 3 eleitores e cada um vota **uma vez por termo** (o voto em si próprio conta). Duas maiorias de 3 nós partilham pelo menos um nó, e esse nó teria de ter votado duas vezes.
- **Dois nós que se julgam líderes em termos diferentes ao mesmo tempo:** **sim, podem existir.** No ensaio 9(d), o antigo líder isolado (termo 1) e o novo líder (termo 2) coexistiram **26 s**. O termo resolve isto quando a rede é reparada: o do termo 1 vê o termo 2 e demite-se.

**(c)**
- **Sem eleição:** o líder isolado continuaria a receber leituras do produtor e a atribuir `seq`, mas os outros dois nós deixavam de receber lotes. Ficariam a «suspeitar» e desatualizados para sempre.
- **Com eleição por maioria:** só o lado com **dois** nós (os dois seguidores) consegue eleger um líder, num termo superior. O antigo líder, sozinho, nunca é eleito, mas continua a julgar-se líder do termo antigo até receber uma mensagem do termo novo.
- **Relação com a partição de dados p1:** **nenhuma**. A partição de rede é uma falha de comunicação **dentro** do grupo que serve p1. A partição de dados p1 é só o subconjunto de sensores (S02, S05, S08) que esse grupo guarda. As partições de dados p0 e p2 não são afetadas.

---

## 4.1 · Estado RAFT, eleição e nó de partição

### Antes de avançar (ponto 1): `RaftState`

- **Porque `adoptTerm` repõe `votedFor` a 0:** o voto é **por termo**. Num termo novo ainda não se votou em ninguém. Sem esta reposição, o nó ficaria com o voto do termo anterior e só poderia votar no mesmo candidato. Se esse candidato não se voltasse a candidatar, o nó nunca mais votaria, e as eleições podiam não ter maioria (perda de vivacidade).
- **Porque se concede o voto a um candidato em quem já se votou neste termo:** é idempotente. O segundo pedido chega quando o primeiro `VOTO` se perdeu ou o pedido é repetido (no RAFT, o candidato retransmite o `RequestVote`), ou quando o PubSub o entrega duas vezes. Recusar seria incoerente com um voto já dado, e voltar a concedê-lo não dá mais poder ao candidato.
- **`ERRO` em `onBatch`:** acontece quando um **líder** recebe um `LOTE` de outro nó **do mesmo termo**, o que exige dois líderes no mesmo termo. É impossível porque a maioria exige 2 votos e cada nó vota uma vez por termo, com o voto gravado antes de ser publicado. Só apareceu com a falha 6 (maioria de 1 voto): 29 a 90 `ERRO` por partição.
- **Ficheiro temporário + `ATOMIC_MOVE`:** se o processo falhar a meio da escrita, o ficheiro de estado fica com o conteúdo antigo ou com o novo, **nunca meio escrito** (um `estado` corrompido impedia o nó de arrancar). **O que falta sem `fsync` (`FileChannel.force`):** a escrita pode ficar na *cache* do sistema operativo. Uma **falha de energia** pode perder o voto já publicado, e o nó, ao reiniciar, podia votar outra vez no mesmo termo. Uma falha só do processo (`kill -9`) não perde nada, porque o SO escreve a *cache*.

### Antes de avançar (ponto 3): candidatura e primeira eleição

- **Falha entre a gravação e a publicação do `PEDIDO_VOTO`:** no arranque seguinte, o nó tem o termo *t* e `votouEm = si próprio`. Arranca como seguidor (o papel não é persistente) e **não pode votar noutro candidato no termo *t***, porque já votou. É seguro: a eleição continua com os outros nós. No pior caso, esse termo fica sem líder e o prazo seguinte abre o termo *t* + 1.
- **Porque se responde mesmo quando se recusa:** o `VOTO` leva o termo do eleitor. Se for maior do que o do candidato, este **adota-o e desiste** logo (`onVote` → `adoptTerm`), em vez de esperar pelo fim do prazo. A recusa também é informação: o candidato sabe que aquele nó já votou ou que está mais atualizado.
- **Tempo até ao primeiro `LOTE`, com os três nós a arrancar no termo 0:**
  - **no mínimo** ≈ `ELEICAO_MIN_MS` = **6 s** (o primeiro prazo a expirar, mais uma ida e volta de mensagens);
  - **no máximo** ≈ `ELEICAO_MAX_MS` + `VERIFICACAO_MS` = **12,2 s**, se não houver voto dividido;
  - o valor esperado do mínimo de três sorteios uniformes é 6 + 6/4 = **7,5 s**;
  - medido em 9(b): **6,47 a 10,55 s, média 8,11 s** (30 partições). A média fica um pouco acima porque os nós arrancam desfasados até ≈ 3 s e o relógio conta desde o arranque do primeiro.

### Antes de avançar (ponto 4): o nó como líder

- **Ordem dos *locks*:**
  - todas as operações do nó adquirem **primeiro o `lock` do nó e depois** o monitor do `RaftState` (`tick`, `onVoteRequest`, `onVote`, `onBatch`, `termIfLeader`, todos chamados dentro de `synchronized (lock)`);
  - **nenhuma** adquire pela ordem inversa: o `RaftState` não conhece o nó, e o seu `log.accept(...)` só usa o `evLock`, que não adquire mais nada;
  - **se alguma usasse a ordem inversa**, uma thread com o `lock` do nó à espera do `RaftState` e outra com o `RaftState` à espera do `lock` do nó ficariam bloqueadas para sempre (*deadlock*). O nó deixava de responder, e os outros elegiam outro líder.
- ***L* e *V* a partir do último registo:** o novo líder continua a mesma sequência de eventos da partição. A componente *V*\[*k*\] tem de continuar a ser igual ao `seq`.
  - **A partir de zero** (falha 5), o primeiro `IN` daria *V*\[*k*\] = 1 com `seq` = 58. A verificação `V[k] = seq`, no líder e no `CausalCheck`, falharia.
  - *L* voltaria a valores já usados e quebraria a condição de relógio com os registos anteriores da mesma partição.
- **`IN` que o líder do termo 1 recebeu mas não publicou, recebido também pelo líder do termo 2:** o tópico de entrada chega aos três nós.
  - **Se, quando a leitura chegou, o nó do termo 2 ainda era seguidor:** assinalou `IGNORADO` e a leitura só existe no ficheiro do antigo líder. Será **truncada** e perde-se (9(a): `S02 20.4`).
  - **Se o nó do termo 2 já era líder** (o antigo isolado, 9(d)): os dois atribuem-lhe um `seq`, cada um no seu termo. A cópia do antigo é truncada, mas a leitura **sobrevive** com o `seq` do novo. Em 9(d), 49 dos 157 registos truncados existiam no registo final com outro `seq`.

### Antes de avançar (ponto 6): o processador e o par (`seq`, `termo`)

- **(`seq` 7, termo 3) depois de (`seq` 7, termo 2):** **sim**, deve gerar o alarme se a temperatura o justificar. É **outro registo**, e muito provavelmente outra leitura: o `seq` 7 do termo 2 foi truncado e o líder do termo 3 deu o 7 a outra leitura.
- **Consequências para a contagem de alarmes:**
  - a contagem **pode incluir alarmes de registos que deixaram de existir**: o alarme do (7, termo 2) já foi publicado e não se desfaz;
  - quando a mesma leitura é atribuída por dois líderes (9(d)), pode haver **dois alarmes** para a mesma leitura;
  - o `AlarmProcessor` conta quantos alarmes vêm de um `seq` já tratado noutro termo («de seq já tratado noutro termo») para tornar isto visível;
  - nos ensaios, o antigo líder isolado não publicava lotes, e o seu único lote depois da reparação foi **ignorado** pelo processador (`IGNORADO p1`, termo 1 < 2).

---

## 4.2 · Ensaios

### Ponto 9

**(a) Figura 1 em p1:**
1. arranque dos 9 nós;
2. o n1 foi eleito no termo 1 em **7,29 s**, com os votos `[1, 3]`;
3. `S02 20.1` e `S05 23.7` (seq 1 e 2), e depois um `LOTE`;
4. `S02 20.4`, e o n1 é terminado 0,3 s depois de ela ser publicada;
5. `S05 23.9` durante a eleição;
6. o n3 é eleito no termo 2, com os votos `[2, 3]`;
7. `S02 20.6`;
8. o n1 volta.

```
eventos-p1-n1.txt (regresso do antigo líder)
…524313;ARRANQUE;p1;n=1;termo=1;ultimoSeq=3;ultimoTermo=1     ← termo 1, lido do ficheiro de estado
…525474;NOVO_LIDER;p1;termo=2;lider=n3                       ← (com TERMO 1->2 antes)
…525479;RECEBIDO;p1;termo=2;lider=n3;hb=5;ultimoSeq=3;ultimoTermo=2;n=0
…525503;TRUNCADO;p1;de=3;descartados=1;termos=[1]
truncados-p1-n1.txt:  3;1;S02;20.4;…10:08:27.915;3;0,3,0
no-p1-n3.txt (final, igual nos três):  1;1;S02;20.1 · 2;1;S05;23.7 · 3;2;S02;20.6
```

| Observação da ficha | Medido |
|---|---|
| Primeira eleição no arranque, ganha por quem expira primeiro | n1, termo 1, 7,29 s, uma só candidatura |
| Indisponibilidade em \]4 s; 12,2 s\] | **8,93 s** (da falha ao primeiro `LOTE` do termo 2) |
| Leitura durante a eleição perde-se sem vestígio | `S05 23.9` perdida (nos três nós, só `IGNORADO`) |
| n1 vota no n3 porque o último registo do n3 é tão atualizado como o seu | n3 eleito com `[2, 3]`: o eleitor foi o n2, porque o n1 estava morto |
| O antigo líder regressa no termo 1, reconhece o novo e trunca | `TERMO 1->2`, `NOVO_LIDER n3`, `TRUNCADO de=3 descartados=1` |
| `S02 20.4` perde-se | perdida: o `seq` 3 é agora `S02 20.6` (termo 2) |

`RaftCheck`: 0 termos com dois líderes, coincidência final OK. `CausalCheck`: `V[1] = seq` OK.

**(b) Arranque**:

| | Prazo aleatório \[6, 12\] s (10 arranques, ordem baralhada, 30 partições) | Prazo fixo 6 s, arranque sincronizado (5 × 3 partições) |
|---|---|---|
| Tempo até ao primeiro líder (mín / médio / máx) | 6,47 / 8,11 / 10,55 s | 6,16 / 10,39 / **42,49 s** |
| Termos até ao primeiro líder (médio / máx) | **1,00** / 1 | **1,67** / 7 |
| Votos divididos | **0** | **10**, em 5 das 15 partições (até 6 seguidos na mesma partição) |
| Duração de uma eleição (1.º `CANDIDATO` → `LIDER`) | média 122 ms, máx 242 ms | 40 a 552 ms |

- **Assinatura de um voto dividido em `eventos-p<k>-n<i>.txt`:** dois ou três `CANDIDATO;…;termo=t` com poucos milissegundos de diferença, seguidos de `VOTO;…;termo=t;candidato=nX;recusado` em todos os nós (cada um já votou em si), **sem** `LIDER` no termo *t*. Depois, novos `CANDIDATO` no termo *t* + 1, outra vez quase simultâneos, porque o prazo é fixo.
- **Exemplo extremo:** `b/fixo-2`, p0, com os termos 1 a 6 divididos entre os três nós e o líder só no termo 7, ao fim de 42,5 s.
- **Primeira versão, sem sincronização:** os nove JVM arrancavam uns depois dos outros, com dezenas a centenas de ms de diferença. Esse desfasamento funcionou como um sorteio e houve **0 votos divididos**. Só com o arranque sincronizado (`-Draft04.inicio`) se observou o problema.

**(c) Três falhas do líder de p1 durante `rajada 300 10`**, com reinício 30 s depois:

| Falha | Termo do líder que falhou → novo | Indisponibilidade | Leituras enviadas durante a eleição | Registos truncados no antigo líder ao regressar |
|---|---|---|---|---|
| n1 | 1 → 2 (n3) | 7,77 s | 63 | 18 (termo 1) |
| n3 | 2 → 3 (n2) | 6,22 s | 37 | 19 (termo 2) |
| n2 | 3 → 4 (n3) | 6,68 s | 51 | 19 (termo 3) |

- **Indisponibilidades:** todas dentro de \]4 s; 12,2 s\], com média de 6,9 s, próxima dos ≈ 7 s esperados.
- **Leituras perdidas:** 187 das 249 enviadas para p1.
  - quase todas as que foram enviadas **durante a eleição**: a rajada dura ≈ 4 s e o líder morria 1,5 s depois do início, por isso o resto da rajada caía na indisponibilidade;
  - mais as **pendentes** do líder morto (18 a 19, menos de Δ = 2 s de leituras), que foram truncadas ao regressar. Destas, **0** existiam no registo final com outro `seq`.
- **Verificações:** coincidência final OK (64 registos nos três nós), 0 termos com dois líderes, `V[1] = seq` OK com registos dos termos 1, 2 e 3.

**(d) Isolamento do líder de p1 durante 30 s**, a meio de uma `rajada 300 10`, com uma segunda `rajada 150 10` 18 s depois do início do isolamento:
- **Dois líderes simultâneos:** o **n1 (termo 1)** isolado continuou líder e o **n3 (termo 2)** foi eleito pelo lado maioritário. Coexistiram **26,0 s**.
- **Leituras atribuídas pelo antigo líder durante o isolamento:** **157** (termo 1, `seq` 1..157). Recebeu as leituras das duas rajadas, porque o tópico de entrada não estava isolado. Nenhum destes registos chegou a ser publicado: o isolamento começou 1 s depois do início da rajada, antes do lote seguinte.
- **Novo líder:** atribuiu **49** registos no termo 2 (a segunda rajada). As leituras da primeira rajada chegaram quando n2 e n3 ainda eram seguidores (`IGNORADO`).
- **Depois da reparação:** o n1 recebeu um `LOTE` do termo 2, demitiu-se e **truncou os 157**. Destes, **49 existem no registo final com outro `seq`** (as leituras da 2.ª rajada, que os dois líderes receberam). Os outros **108 perderam-se**. No total, perderam-se 94 das 140 leituras enviadas para p1, contando também as que chegaram durante a eleição.
- **Processador:** `SUSPEITA` de p1 durante a eleição, `NOVO_LIDER` (termo 2) e `RETOMA`. Nunca viu os registos do antigo líder, que não publicou nada. Num primeiro ensaio igual, o lote que o antigo líder publicou logo a seguir à reparação foi **ignorado** (`IGNORADO p1`, termo 1 < termo 2).

**(d) Isolamento de um seguidor (n2) de p1 durante 60 s**, sem rajada (3 leituras pelo meio):
- **Termo atingido:** o n2 candidatou-se de prazo em prazo, sem ninguém o ouvir, e **chegou ao termo 8** (termos 2 a 8 «sem líder, nenhum voto concedido»).
- **Efeito do regresso:** o seu `PEDIDO_VOTO` do termo 8 fez o líder **n1 demitir-se** (`DEMISSAO;termo=1`). O voto foi recusado ao n2 pela restrição de voto, porque lhe faltavam os 3 registos. Depois de um novo prazo, o **n1 foi reeleito no termo 9**.
- **Resultado:** uma eleição desnecessária, com a partição sem líder durante esse prazo. Não se perdeu nenhuma leitura, porque nenhuma foi enviada nesse intervalo, e a coincidência final ficou OK.

**(e) Dois dos três nós de p1 terminados** (o líder n2 e o n3), com 10 leituras para p1 durante 30 s:
- **Sobrevivente n1:** candidatou-se nos termos **2, 3 e 4**, sempre sem votos, e as 10 leituras perderam-se (só `IGNORADO`).
- **Com o n3 de volta:** houve líder **ao fim de ≈ 2 s** (n1, termo 5, votos `[1, 3]`). O n3 arrancou no termo 1 e adotou logo o termo do primeiro `PEDIDO_VOTO` que recebeu.
- **Indisponibilidade total:** **33,8 s**.

### Ponto 10 — Falhas introduzidas

Ativadas com `-Draft04.erro=<n>` (código correto com 0). Cada uma correu num ensaio próprio (`execucao/f<n>`).

| Falha introduzida | Lado onde surgiu | Detetada por `RaftCheck` / `CausalCheck`? | O que permitiu concluir |
|---|---|---|---|
| Maioria calculada como `votes.size() >= groupSize / 2` | **Nenhum, à letra** (f1). Na variante f6, nos candidatos | **f1: nada a detetar** (0 líderes duplicados, OK). **f6: `RaftCheck` sim**: «DOIS OU MAIS LÍDERES: [1, 2, 3]» no termo 1, «LÍDERES SIMULTÂNEOS» durante ≈ 28 s, 29 a 90 `ERRO dois lideres no termo 1` e 6 a 12 `seq` divergentes na coincidência final, em 4 das 9 partições. **`CausalCheck`: não** (cada ficheiro é coerente consigo mesmo) | Com G = 3, `onVote` só corre ao receber o voto de **outro** nó, e nesse momento já há 2 votos. A fórmula errada (≥ 1) só pesaria com G par (com G = 4, ≥ 2 deixava dois candidatos ganharem com 2 votos cada). Na variante f6 a maioria é verificada **logo na candidatura**: cada candidato com o seu próprio voto é líder, e com o arranque sincronizado houve 2 ou 3 líderes no mesmo termo, cada um a dar o mesmo `seq` a leituras diferentes |
| Termo e `votedFor` só em memória; seguidor reiniciado logo após conceder um voto | Seguidor reiniciado | **`RaftCheck`: sim**, «TERMO DIMINUIU em n1: 2 → 0» e «em n2: 1 → 0» (ao `ARRANQUE`). Não se observou um voto duplo. **`CausalCheck`: não** | O nó volta ao termo 0 e esquece o voto. A sequência perigosa é: votar em A no termo *t*, falhar, voltar sem memória e receber o pedido de B no mesmo termo *t*, votando também em B. A e B ficam ambos com 2 votos, logo há dois líderes no termo *t*. Não aconteceu porque o `LOTE` do vencedor chega em milissegundos e B deixa de ser candidato, mas a garantia deixou de existir |
| Voto sem a verificação `upToDate`, com um nó atrasado candidato | Eleitores; perdas nos seguidores | **`RaftCheck`: sim**: o n1, isolado durante as escritas (registo vazio, termo 8), foi **eleito** com `[1, 2]`. Houve **2 `TRUNCADO`** (n2 e n3, 3 registos cada, de `seq` 1) e **3 de 4 leituras perdidas**. Com o código correto (9(d) seguidor), o voto foi recusado e perdeu-se 0. **`CausalCheck`: não** | A restrição de voto é o que impede que um nó sem os registos da maioria seja eleito. Sem ela, o líder novo é a referência e os outros apagam registos que já tinham |
| Seguidor que aplica lotes de termo inferior (9(d) repetido) | Seguidores **e o novo líder** | **`RaftCheck`: sim**: 2 `TRUNCADO`, em que n2 e n3 descartaram **51 registos do termo 2** (os do lado maioritário!) e aplicaram os 157 do termo 1 do antigo líder. A coincidência final está OK, mas o registo final tem **só o termo 1**, apesar de existir um líder do termo 2. **`CausalCheck`: não** (`V[1] = seq` OK) | As escritas da **minoria** substituíram as da **maioria**. O registo de referência deixou de ser o do líder atual, e o termo deixou de proteger contra o líder obsoleto. Neste ensaio não se perderam leituras porque as do termo 2 também tinham sido recebidas pelo antigo líder, mas noutra ordem e com outros `seq` |
| Novo líder que inicializa *L* e *V* a zero | Novo líder | **`RaftCheck`: sim, indiretamente**: **36 de 82** leituras para p1 perdidas, e **43 `ERRO;p1;recusado V[1]=1 diferente de seq=58`** nos eventos do novo líder. **`CausalCheck`: não** (nada de errado foi escrito: OK) | A verificação `V[k] = seq` do líder recusa todos os eventos. Sem ela, os registos seriam escritos com *V*\[1\] = 1, 2, … e `seq` 58, 59, …, e o `CausalCheck` assinalaria `V[1] = seq: ERRO` em todas as linhas novas e violações da condição de relógio |

---

## 4.3 · Comparação e reflexão crítica

### Ponto 11 — Valores medidos

| Métrica | Valor |
|---|---|
| Tempo até ao primeiro líder no arranque (mínimo / médio / máximo, s) | **6,47 / 8,11 / 10,55** (9(b), prazo aleatório, 30 partições) · 9(a): 7,29 · 9(c): 8,20 |
| Termos até ao primeiro líder: prazo aleatório / prazo fixo | **1,00 / 1,67** (máximo 7) |
| Votos divididos: prazo aleatório / prazo fixo | **0** em 30 partições / **10** em 15 partições (1 deles por um `VOTO` perdido no daemon) |
| Indisponibilidade após falha do líder (mínimo / médio / máximo, s) | **6,22 / 7,40 / 8,93** (9(a) e 3 × 9(c)) |
| Leituras perdidas por falha: durante a eleição / por truncagem | **38 / 14** em média (9(a): 1 / 1 · 9(c): 63, 37, 51 / 18, 19, 19) |
| Registos truncados ao regressar o antigo líder | 1 (9(a)) · 18, 19, 19 (9(c)) |
| Termos com mais de um líder | **0** em todos os ensaios com o código correto |
| `seq` divergentes na coincidência final | **0** em todos os ensaios com o código correto |

| Cenário (ensaio) | Lado maioritário aceita escritas? | Lado minoritário aceita escritas? | Escritas do lado minoritário após a reparação | Termos envolvidos | Comportamento: C ou A? |
|---|---|---|---|---|---|
| Falha do líder (c) | **Sim**, depois de 6 a 8 s de eleição | Não (o nó falhou). As pendentes ficaram no ficheiro | Truncadas ao regressar (18 a 19 por falha) | 1 → 2 → 3 → 4 | **C**: indisponível durante a eleição, sem conflitos |
| Isolamento do líder (d) | **Sim** (n3, termo 2: 49 registos) | **Sim**: o antigo líder aceitou **157** no termo 1 | **Truncadas** (157), das quais 49 sobreviveram com outro `seq` e 108 perderam-se | 1 e 2, ao mesmo tempo | **A** durante a partição (os dois lados aceitam) e **C** depois (o termo maior ganha). Em rigor **não é CP** |
| Isolamento de um seguidor (d) | **Sim** (o líder continua), até ao regresso do isolado | Não (o isolado nunca tem maioria; termos 2 a 8 sem votos) | Nenhuma. O regresso provoca a demissão do líder e uma eleição desnecessária (termo 9) | 1 → 9 | **C** |
| Falha de dois nós (e) | Não há maioria | Não: o sobrevivente candidata-se nos termos 2 a 4 sem votos | — (10 leituras perdidas) | 1 → 5 | **C**: 33,8 s indisponível |

### Ponto 12 — Questões

**Eleição no arranque.**
- **Porque os prazos aleatórios evitam votos divididos:** espalham as expirações por 6 s. O primeiro nó a expirar pede os votos e recebe-os em ≈ 120 ms (média medida), muito antes de o segundo expirar. Em 30 partições houve **0** votos divididos.
- **Com prazo fixo e arranque sincronizado:** **10** votos divididos em 15 partições, até 6 seguidos (42,5 s sem líder). Cada nó volta a sortear 6 s exatos a partir de instantes quase iguais.
- **A ordem de arranque deixou de importar:** ninguém é configurado como líder. Quem arranca primeiro só tem o prazo a contar mais cedo. Nas 10 ordens baralhadas houve sempre líder no termo 1.
- **Relação entre os tempos:** Δ ≪ `ELEICAO_MIN_MS` < `ELEICAO_MAX_MS`, com `ELEICAO_MIN_MS` igual a vários Δ (aqui 3Δ), para que um ou dois heartbeats atrasados não provoquem uma eleição. O intervalo de sorteio deve ser largo face ao tempo de uma votação (aqui 6 s contra ≈ 0,1 s).
- **Se `ELEICAO_MIN_MS` fosse inferior a Δ:** os seguidores expirariam **entre dois heartbeats** de um líder saudável. Haveria eleições contínuas, demissões de líderes vivos e termos a subir sem parar (o mesmo efeito do seguidor isolado de 9(d), mas permanente).

**O termo.**
- **Demonstração:** para ser líder no termo *t*, um nó precisa de 2 votos do termo *t*. Dois líderes no termo *t* precisariam de 4 votos, mas há 3 nós e cada um só concede **um** voto por termo (`votedFor` é gravado antes de o `VOTO` sair). Duas maiorias têm um nó em comum, e esse nó teria votado duas vezes. Logo, no máximo um líder por termo: 0 termos com dois líderes em todos os ensaios corretos.
- **Isso não impede dois nós de se julgarem líderes ao mesmo tempo:** em 9(d), o n1 (termo 1) e o n3 (termo 2) coexistiram 26 s. A garantia é **por termo**, não por instante. O líder antigo só sabe que foi substituído quando recebe uma mensagem do termo novo.
- **Papel do termo na reparação:** é o critério de desempate. O n1 recebe o `LOTE` do termo 2, demite-se e trunca, e o processador ignora o lote do termo 1.
- **Comparação com Lamport:** como o relógio de Lamport, o termo só cresce, viaja em todas as mensagens e quem recebe um maior adota-o. Ao contrário dele, **não conta eventos nem ordena registos**: identifica uma **época de liderança**, e um termo maior **invalida** o líder anterior, coisa que um *L* maior nunca faz.

**CAP e particionamento.**
- **Classificação:** ver a segunda tabela.
  - falha do líder, isolamento de um seguidor e falha de dois nós: **C**, ou seja, indisponibilidade em vez de conflito;
  - isolamento do líder: **A** durante a partição (dois líderes aceitaram escritas) e C depois da reparação.
- **Porque o lado minoritário não elege líder:** precisa de 2 votos e só tem o seu. O seguidor isolado chegou ao termo 8 sem um único voto.
- **O sistema desta ficha não é, em rigor, CP:** o antigo líder isolado aceitou e escreveu 157 leituras que depois foram truncadas. Quem as lesse no ficheiro dele veria dados que desapareceram.
- **Mecanismo que o tornaria CP:** o ***commit* por maioria** (`AppendEntries` com resposta e índice de *commit*). Um registo só conta como aceite quando está em 2 dos 3 nós. O líder isolado nunca conseguiria confirmar nada, e as escritas ficavam **pendentes**, nunca aceites.
- **Custo do *commit*:**
  - **latência:** mais uma ida e volta antes de cada confirmação;
  - **disponibilidade:** o lado minoritário passa a recusar ou a não confirmar escritas, em vez de as aceitar;
  - as leituras linearizáveis exigiriam também falar com a maioria.

**Restrição de voto e truncagem.**
- **O que protege:** um nó atrasado não é eleito. Na falha 3, sem ela, o n1 com o registo vazio foi eleito e os outros dois apagaram 3 registos. Com ela (9(d) seguidor), foi recusado e nada se perdeu.
- **O que não protege:** os registos que **só o líder antigo** tinha. Em 9(a), `S02 20.4` estava só no ficheiro do n2 (líder, pendente). O n3 e o n1 estavam tão atualizados um como o outro e elegeram o n3, que nunca viu essa leitura.
- **Porque, sem *commit*, uma leitura escrita pelo líder se pode perder:** escrevê-la no ficheiro do líder não a torna conhecida da maioria. Se o líder falhar antes do lote seguinte, a maioria elege outro que não a tem e dá o mesmo `seq` a outra leitura. Ao regressar, o antigo líder trunca-a (9(c): 18 a 19 por falha). Só a confirmação por maioria antes de responder o evitaria.

**Persistência.**
- **Termo e voto têm de ser persistentes:** na falha 2, o nó reiniciado voltou ao termo 0 (`TERMO DIMINUIU 2 → 0`) e esqueceu o voto. Um nó que esquece o voto pode votar duas vezes no mesmo termo, e com dois votos «duplicados» há dois líderes num termo. Um nó que esquece o termo aceita mensagens de líderes obsoletos.
- **Os registos também são persistentes:** a restrição de voto e a eleição baseiam-se no último registo de cada nó. Se um nó perdesse registos ao reiniciar, podia votar num candidato atrasado ou ser ele próprio um candidato «aparentemente atualizado». Além disso, o líder novo inicializa `seq`, *L* e *V* a partir do ficheiro.
- **Na ficha anterior:** era a **incarnação `inc`** (o instante de arranque) que permitia distinguir um reinício do líder fixo de uma retoma. Era local ao processo, e não um número partilhado pelo grupo.

**Detetar, suspeitar e eleger.**
- **Custo de uma falsa suspeita:**
  - na ficha anterior, uma falsa suspeita só dava um `AVISO`;
  - agora, uma pausa longa do líder (como os 30 s de isolamento de 9(d)) faz o resto do grupo **eleger outro líder**;
  - o antigo, ao retomar, ainda se julga líder, aceita escritas e acaba por as ver **truncadas** (157 em 9(d));
  - a partição fica indisponível durante a eleição (6 a 9 s).
- **Porque o seguidor isolado provoca uma eleição desnecessária quando regressa:** sobe o termo a cada prazo, porque não ouve ninguém, e o seu `PEDIDO_VOTO` com termo 8 obriga um líder saudável a demitir-se (`DEMISSAO;termo=1`). Não ganha, porque está atrasado, mas a partição fica sem líder até à eleição seguinte (termo 9).
- **O que faria uma fase de *Pre-Vote*:** antes de incrementar o termo, o nó pergunta «votarias em mim?». Os outros, que ainda recebem heartbeats do líder, respondem que não. O isolado nunca chega a aumentar o termo nem perturba o grupo ao regressar.

**Limites.**
- **O que acontece às leituras enviadas a uma partição sem líder:** perdem-se sem vestígio. Os três nós assinalam `IGNORADO`, e o produtor só avisa se já suspeitar (em 9(c), a rajada acabava antes da suspeita). Exemplos: 9(c), 63, 37 e 51 por falha; 9(e), as 10 leituras de 30 s.
- **O que seria preciso para nenhuma leitura aceite se perder, sem duplicados:**
  1. o produtor **retém e reenvia** cada leitura até receber uma **confirmação**;
  2. a confirmação só é dada depois do ***commit* por maioria**, para que uma leitura confirmada nunca seja truncada;
  3. cada leitura leva um **identificador único** (produtor + número de sequência do produtor), e o líder guarda os identificadores já aceites. Assim, um reenvio de uma leitura que afinal já tinha sido aceite (com a confirmação perdida) não cria um duplicado.
- **Cabe no âmbito desta ficha? Não.** A ficha exclui a confirmação por maioria e a retenção ou reenvio pelo produtor. **A solução desta ficha não resolve esse problema.**

---

## Critérios de aceitação

### CA1 · Fundamentos

- **Termo:** número inteiro, persistente e crescente, que identifica uma época de liderança. Cada termo tem no máximo um líder.
- **Maioria:** ⌊G/2⌋ + 1 = 2 votos em G = 3, contando o voto do próprio candidato.
- **Restrição de voto:** só se vota num candidato cujo último registo seja pelo menos tão atualizado como o nosso (termo maior, ou igual com `seq` maior ou igual).
- **CAP:**
  - **C:** o sistema comporta-se como se houvesse uma só cópia;
  - **A:** todo o pedido a um nó vivo tem uma resposta que não é erro;
  - **P:** o sistema funciona apesar de perdas arbitrárias de mensagens.

  Durante uma partição de rede não se tem C e A ao mesmo tempo.
- **Porque o termo garante no máximo um líder por termo, mas não um líder ativo por instante:** a garantia vem dos votos, e os votos são **por termo**. Um líder antigo que não recebe mensagens não sabe que houve um termo novo. Em 9(d), houve dois líderes durante 26 s, em termos diferentes.

Perguntas de verificação:
- **Se o n1 tivesse falhado em *t* = 15:** **não**, o n3 não podia ser eleito no termo 2. Sozinho tem 1 voto em 3.
  - A partição p1 ficaria **indisponível** para escritas até um dos outros voltar. As leituras para p1 perdiam-se (9(e): 33,8 s sem líder e 10 leituras perdidas).
  - Isso traduz-se em **C** no CAP: abdica-se de A para não arriscar dois líderes.
- **Porque o n2, ao regressar, não volta a ser líder, apesar de ter registos até ao `seq` 3:**
  - volta como **seguidor** no termo 1, e o primeiro `LOTE` do termo 2 faz-lhe adotar o termo 2 e reconhecer o n3;
  - mesmo que se candidatasse, o seu último registo é (`seq` 3, termo 1). O do n3 é (`seq` 3, termo 2), com um **termo maior**, por isso os outros recusar-lhe-iam o voto pela restrição de voto.
  - **Se tivesse regressado em *t* = 16, antes de qualquer eleição:** **sim**, podia ser eleito no termo 2. O seu registo (3, termo 1) era **mais** atualizado do que o do n1 e o do n3 (2, termo 1), e esse eleitor concedia-lhe o voto. Nesse caso `S02 20.4` sobreviveria.
- **Partição de dados p1 contra partição de rede que isola o n2:** p1 é o conjunto de sensores (S02, S05, S08) servido por um grupo de três nós. A partição de rede é uma falha de comunicação que separa o n2 desse grupo, com o n2 vivo.
  - **O termo de p1 é independente do de p0:** cada partição de dados tem o seu grupo, o seu tópico de eleição, os seus ficheiros de estado e a sua sequência de termos. Em 9(c), p1 chegou ao termo 4 enquanto p0 e p2 continuaram no termo 1.

### CA2 · Implementação

- **Onde se gravam o termo e o voto:** `RaftState.persist()`, chamada em `tick()` (voto em si), `onVoteRequest` (voto concedido) e `adoptTerm` (termo novo), sempre **antes** de o método devolver. A mensagem só é publicada pelo `PartitionNode` depois disso.
- **Condições de voto e de maioria:** `RaftState.onVoteRequest` (`upToDate`, `cTerm == term`, `votedFor`) e `RaftState.onVote` (`votes.size() <= groupSize / 2`).
- **Demissão:** `RaftState.adoptTerm` (evento `DEMISSAO`), chamada por `onBatch`, `onVoteRequest` e `onVote`. No nó, `PartitionNode.afterRaft()` cancela o agendamento dos lotes quando `termIfLeader()` deixa de ser o termo da liderança.
- **Onde o novo líder inicializa `seq`, *L* e *V*:** `PartitionNode.becomeLeader()`, com o último registo do ficheiro.
- **Secções sincronizadas:** o `lock` do nó protege os registos, os retidos, `pendentes`, `hb`, *L*, *V* e o agendamento. O `RaftState` protege o termo, o voto, o papel e o prazo. A ordem é sempre nó → `RaftState` (ver «Antes de avançar» 4).

Perguntas de verificação:
- **Porque se grava antes de publicar o `VOTO` ou o `PEDIDO_VOTO`:** se o voto saísse e o nó falhasse antes de o gravar, voltava com `votedFor = 0`.
  - **Sequência que produz dois líderes no mesmo termo:** os candidatos A e B estão no termo *t*. C vota em A (A ganha com A + C) e falha antes de gravar. C volta, recebe o pedido de B no termo *t* e vota em B, que ganha com B + C. Há dois líderes no termo *t*.
  - Na falha 2, o `RaftCheck` mostrou o sintoma: o termo voltou a 0 depois do reinício.
- **Um `IN` no instante da demissão:** o `onIn` obtém `termIfLeader()` **dentro do `lock` do nó**, e a demissão (`onBatch`/`onVoteRequest`/`onVote` → `adoptTerm`) também acontece dentro desse `lock`. As duas operações estão **serializadas**:
  - ou o `IN` é tratado **antes**, e recebe um `seq` com o termo antigo, legítimo naquele momento (será truncado se divergir);
  - ou é tratado **depois**, e `termIfLeader()` devolve −1, o que dá `IGNORADO`.

  Nunca há um `seq` com o termo antigo depois de o nó adotar o novo.
- **Porque o prazo é sorteado a cada eleição e a cada voto concedido:**
  - **a cada eleição:** dois candidatos que empataram voltam a sortear, e o empate desfaz-se na eleição seguinte. Com um só sorteio no arranque, quem empatou uma vez empataria sempre (é o que o prazo fixo mostra: até 6 votos divididos seguidos);
  - **a cada voto concedido:** o eleitor dá tempo ao candidato em quem votou para ganhar e enviar o primeiro `LOTE`, em vez de se candidatar logo a seguir e competir com ele.

### CA3 · Análise

Com `RaftCheck`, em todos os ensaios com o código correto:
- **segurança:** **0 termos com mais de um líder**, 0 nós com termo a diminuir, 0 votos duplos;
- **primeiro líder no arranque:** 6,47 a 10,55 s;
- **indisponibilidade após falha:** 6,22 a 8,93 s;
- **leituras perdidas e truncagens:** contadas por falha (ponto 11);
- **coincidência final:** **0 `seq` divergentes**.

Com `CausalCheck`: **`V[k] = seq` OK** em todas as partições de todos os ensaios, incluindo os registos de vários termos (9(c): termos 1, 2 e 3 no mesmo ficheiro). As mudanças de líder não quebram o invariante, porque o novo líder parte do último registo.

Perguntas de verificação:
- **As indisponibilidades de 9(c) cabem no intervalo previsto? Sim:** 7,77, 6,22 e 6,68 s, em \]4 s; 12,2 s\], com média de 6,9 s (esperados ≈ 7 s).
  - **Leituras perdidas:** **63, 37 e 51** durante a eleição, mais **18, 19 e 19** truncadas.
  - **Relação com a indisponibilidade:** o número perdido durante a eleição é ≈ ritmo de envio para p1 × (parte da indisponibilidade em que ainda havia rajada). A rajada acabava ≈ 2,5 s depois da falha, por isso as perdas dependem mais da rajada do que da duração total da eleição. Com envio contínuo, seriam proporcionais à indisponibilidade.
- **Termos até haver líder em 9(b) com prazo fixo:** **1,67 em média e até 7** (`b/fixo-2`, p0: 6 votos divididos seguidos, 42,5 s).
  - **O que caracteriza um voto dividido nos eventos:** vários `CANDIDATO` no **mesmo termo** com milissegundos de diferença, `VOTO … recusado` de cada nó (cada um já votou em si) e **nenhum `LIDER`** nesse termo. A seguir vem um novo `CANDIDATO` no termo + 1.
- **Registos truncados no antigo líder em 9(d):** **157**. **49** existem no registo do novo líder com **outro `seq`**: as leituras da 2.ª rajada chegaram aos dois líderes, porque o tópico de entrada não estava isolado, e cada um deu-lhes o seu número. Os outros **108 perderam-se**: chegaram quando o lado maioritário ainda não tinha líder, e os seguidores n2 e n3 só as assinalaram como `IGNORADO`.

### CA4 · Limites

Perguntas de verificação:
- **Quem leu o ficheiro do antigo líder em 9(d) viu dados que depois desapareceram:** o mecanismo que o impede no RAFT completo é o ***commit* por maioria**, ou seja, `AppendEntries` com resposta e o índice de *commit*. Só os registos replicados em 2 dos 3 nós são confirmados e visíveis. O isolado nunca chegaria a confirmar nada, e o sistema ficaria do lado **CP**: indisponível para escritas no lado minoritário, sem nunca mostrar dados que desaparecem.
- **O seguidor isolado sobe o termo a cada prazo** porque não ouve heartbeats e se candidata sem que ninguém o ouça (chegou ao termo 8). Ao regressar, o termo 8 faz o líder saudável demitir-se. O mecanismo do RAFT que evita isto é a **fase *Pre-Vote***: o nó só aumenta o termo se uma maioria disser que votaria nele, e nenhum nó que ouve o líder diz que sim.
- **Se o produtor reenviasse as leituras até serem confirmadas:** surgiriam **duplicados**. Uma leitura aceite cuja confirmação se perdeu seria reenviada e atribuída outra vez, com outro `seq`. Resolve-se com um **identificador único por leitura** (produtor + contador) e com o líder a guardar os identificadores já confirmados, ignorando os repetidos (idempotência). A confirmação teria de vir depois do *commit*.

**Limitações que se mantêm, e o que faria falta:**
1. **Sem *commit* por maioria, um líder obsoleto aceita escritas que depois são truncadas** (157 em 9(d); 18 a 19 por falha em 9(c)). Faria falta o `AppendEntries` com resposta e o índice de *commit*.
2. **As leituras enviadas durante a eleição perdem-se** (até 63 por falha em 9(c); 10 em 9(e)). Faria falta a retenção e o reenvio no produtor com identificadores únicos.
3. **Um seguidor isolado perturba o grupo ao regressar** (demissão de um líder saudável em 9(d)). Faria falta a fase *Pre-Vote*.
4. **A gravação sem `fsync`** não sobrevive a uma falha de energia, e o voto podia ser esquecido. Faria falta `FileChannel.force(true)` em `persist()`.
5. **A eleição depende do PubSub do daemon**, que perde mensagens sob carga (um `VOTO` perdido deixou um termo sem líder). O RAFT tolera-o, mas a eleição pode demorar mais um prazo.
