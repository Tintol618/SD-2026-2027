# Sprint E1-COMINDIR04 — Deteção de Falhas do Líder por Heartbeat e Envio em Lote

## Estrutura

Quatro projetos separados, todas as classes no pacote `rep04`, tópicos com o prefixo `rep04`:

| Projeto | Classes |
|---|---|
| `rep04-produtor/src/rep04/`    | `SensorRecord`, `RecordFile`, `IpfsPubSub`, `Wire`, `Partitioner`, `VectorClock`, **`Batch`**, **`HeartbeatMonitor`**, **`Producer`** |
| `rep04-lider/src/rep04/`       | `SensorRecord`, `RecordFile`, `IpfsPubSub`, `Wire`, `Partitioner`, `VectorClock`, **`Batch`**, **`HeartbeatMonitor`**, **`PartitionLeader`** |
| `rep04-replica/src/rep04/`     | `SensorRecord`, `RecordFile`, `IpfsPubSub`, `Wire`, `Partitioner`, `VectorClock`, **`Batch`**, **`HeartbeatMonitor`**, **`PartitionReplica`**, `PartitionCheck`, `CausalCheck`, **`HeartbeatCheck`** |
| `rep04-processador/src/rep04/` | `SensorRecord`, `IpfsPubSub`, `Wire`, `Partitioner`, `VectorClock`, **`Batch`**, **`HeartbeatMonitor`**, **`AlarmProcessor`** |

- `HeartbeatMonitor` está **exatamente** como no enunciado. `Batch`, `HeartbeatMonitor`, `Wire` e `SensorRecord` são idênticos nos quatro projetos.
- `Wire`: retirados `reg(...)` e `regParts(...)`. Acrescentados `PERIODO_HB_MS` (propriedade `rep04.periodo`, 5000 por omissão), `FATOR_TIMEOUT` (`rep04.fator`, 3 por omissão) e `VERIFICACAO_MS = 1000`.
- `SensorRecord.now(...)`: o `instante` passa a ter **milissegundos** (`2026-11-04T11:19:24.762`). O formato da linha é o mesmo (`seq;sensor;temperatura;instante;L;V`), mas com segundos inteiros a latência de lote teria um erro de até 1 s.
- `CausalCheck`: só mudou o cálculo do tamanho de um `REG`. Como `Wire.reg` foi retirado, a mensagem é reconstruída localmente (`"REG;127.0.0.1;" + (7000 + k) + ";" + linha`). `PartitionCheck`, `IpfsPubSub`, `Partitioner`, `VectorClock` e `RecordFile` estão inalterados (só o pacote mudou).

### Execução

Em cada projeto: `javac --release 11 -encoding UTF-8 -d out src/rep04/*.java`. Depois, cada programa no seu terminal e na **mesma pasta**, por esta ordem: **réplicas → líderes → processador → produtor**.

```bash
java -cp out rep04.PartitionReplica 1 0,1      # e 2 1,2 / 3 2,0
java -cp out rep04.PartitionLeader 0 3         # e 1 3 / 2 3
java -cp out rep04.AlarmProcessor 3
java -cp out rep04.Producer 3                  # <sensor> <temp> | rajada <n> <ms> | sair
java -cp out rep04.HeartbeatCheck 3 <pasta> [falhas.txt]   # escreve também <pasta>/heartbeat-relatorio.txt
java -cp out rep04.CausalCheck 3 <pasta>
java -cp out rep04.PartitionCheck 3 <pasta>
# opções (antes de -cp): -Drep04.periodo=<ms> -Drep04.fator=<f>; só no líder: -Drep04.erro=<1..4> (ponto 9)
```

- `executar.bat` faz o mesmo de forma interativa (com os modos «rajada» e «Figura 1»).
- `ensaios.sh` (Git Bash) corre **todos os ensaios** dos pontos 8 e 9, cada um em `execucao/<ensaio>/`, e termina com `HeartbeatCheck`, `CausalCheck` e `PartitionCheck` (`execucao/<ensaio>/verificacao.txt`).
- `kill -9` é feito com `taskkill /F`. `kill -STOP`/`kill -CONT` não existem no Windows: `suspender.ps1` usa `NtSuspendProcess`/`NtResumeProcess`, que suspendem todas as threads do processo e têm o mesmo efeito.

### Decisões de desenho

- **Líder** (`PartitionLeader`):
  - no arranque: `inc = System.currentTimeMillis()` e `hb = 0`. *L*, *V* e `seq` vêm da última linha de `lider-p<k>.txt`, como na COMINDIR03;
  - `onIn`: a **mesma secção sincronizada** da COMINDIR03 (relógios, `seq`, verificação `V[k] = seq`, `append`) acrescenta, no fim, `pendentes.add(r)`. Já não se publica nenhum `REG`;
  - `enviarLote()` corre num `ScheduledExecutorService` com `scheduleAtFixedRate(…, 0, PERIODO_HB_MS)`:
    1. dentro de `synchronized (lock)`, o mesmo monitor de `onIn`: troca `pendentes` por uma lista nova, lê `ultimoSeq = seq` e faz `++hb`;
    2. fora do `lock`: `Batch.encode()`, `publish`, a linha `inc;hb;instante;ultimoSeq;n;bytes` em `hb-p<k>.txt` e `HEARTBEAT: hb=… n=… ultimoSeq=…`;
  - `instante` em `hb-p<k>.txt` é o instante **imediatamente antes** do `publish`. Só se escreve a linha se a publicação correr bem: o ficheiro tem os lotes **publicados**;
  - se o `publish` falhar, regista `PUBLICAÇÃO FALHOU (hb …)` e **não** volta a publicar. Os registos chegam aos participantes pelo `ultimoSeq` do lote seguinte;
  - `enviarLote()` apanha qualquer `RuntimeException`. Com `scheduleAtFixedRate`, uma exceção não apanhada **cancela silenciosamente** todas as execuções seguintes, e o líder ficaria vivo mas sem heartbeats, ou seja, uma falsa «falha»;
  - o servidor TCP de recuperação está inalterado.
- **`Batch`**: campos `address`, `port`, `inc`, `hb`, `lastSeq` e `records`. Métodos:
  - `encode()`: o cabeçalho e as *n* linhas, separados por `\n`;
  - `decode(msg, n)`: aqui *n* é a **dimensão dos vetores**, como em `SensorRecord.fromLine(linha, n)`, e o número de registos vem do cabeçalho. Lança exceção se o número de linhas não coincidir com o *n* do cabeçalho. A réplica, que não conhece N, passa `0` (aceita a dimensão que vier);
  - `decodeHeader(msg)` e `headerCount(msg)` servem o produtor, que só interpreta o cabeçalho.
- **Réplica** (`PartitionReplica.Partition.onLote`):
  1. `monitor.onHeartbeat(inc, hb)` e o evento `RECEBIDO;p<k>;inc=…;hb=…;ultimoSeq=…;n=…;seq=<a>..<b>` (`seq=-` se o lote vier vazio);
  2. cada registo passa pela regra da COMINDIR02 (`apply`: duplicados ignorados, retidos em `pending`, recuperação TCP se houver mais de `LIMITE_RETIDOS` retidos);
  3. no fim, se `last < ultimoSeq`, faz `recover(last + 1, ultimoSeq)` por TCP para o endereço e porto do cabeçalho;
  - `RECUPERACAO;p<k>;de=<a>;ate=<b>` é registado em **todas** as recuperações TCP, também nas provocadas por lacunas;
  - os registos de um `LOTE` de incarnação antiga **não** são descartados: o monitor ignora-o, mas os registos são aplicados com a regra normal (ver «Antes de avançar» 1).
  - a thread `verificacao` chama, a cada `VERIFICACAO_MS`, `check()` em **todos** os monitores e só depois os resumos. O monitor tem o seu próprio lock, por isso uma recuperação TCP lenta numa partição não atrasa a suspeita.
- **Eventos dos participantes**: `event(String)` é `static synchronized` em cada classe principal. Escreve na consola e em `monitor-<id>.txt` a linha `System.currentTimeMillis() + ";" + evento`, ou seja `instante;evento;p<k>;detalhe`. É também o `Consumer<String>` passado a cada `HeartbeatMonitor`.
- **Processador**: uma thread por partição, `Batch.decode(msg, N)`, um monitor por partição e `RECEBIDO` em `monitor-processador.txt`. Cada registo do lote passa por `onReg`, inalterado. Antes de publicar um alarme para *j*: `AVISO: líder p<j> suspeito; alarme … pode perder-se`.
- **Produtor**: uma thread de subscrição (daemon) por partição, que só lê o cabeçalho, mais a thread de verificação. Em `send`, antes de publicar um `IN`, escreve `AVISO: líder p<k> suspeito; leitura … pode perder-se` se for o caso, e envia na mesma.
  - `rajada <n> <ms>`: *n* leituras de sensores aleatórios S01..S10 com *ms* de intervalo entre envios. Na COMINDIR03, o segundo argumento era o número de sensores. Aqui segue o diagrama de classes (`rajada(n, ms)`) e a pergunta do CA3 («300 leituras com intervalo de 10 ms»).
- **`HeartbeatCheck <N> <pasta> [falhas.txt]`**:
  - **por partição e incarnação:** número de lotes, `hb` em falta no ficheiro, proporção de vazios, registos por lote (média, máximo e média dos não vazios), intervalo entre heartbeats (média e máximo);
  - **suspeitas por participante:** a incarnação observada é o `inc` do último `RECEBIDO` antes da suspeita. É **falsa suspeita** se a `RETOMA` seguinte tiver o mesmo `inc`, e **falha real** se a `RETOMA` trouxer outro `inc` ou não chegar. Se o participante nunca tinha recebido um lote dessa partição, a suspeita aparece como «sem líder observado». Para cada suspeita indica também o silêncio desde o último heartbeat **publicado** (de `hb-p<k>.txt`);
  - **tempo de deteção:** para cada linha `p<k>;t` de `falhas.txt`, a primeira `SUSPEITA` de cada participante que acompanha p<k> depois de *t*. No fim, compara com o intervalo previsto \](fator−1)Δ, fator·Δ + 1 s\];
  - **latência de lote:** para cada `RECEBIDO` com `seq=a..b` (réplicas e processador), o instante do `RECEBIDO` menos o `instante` do registo. Mostra também a «espera no líder» (instante de publicação do lote menos o instante do registo);
  - **contagens:** `HB_PERDIDO`, `RECUPERACAO` e `REINICIO`;
  - **custo do transporte:** bytes por registo em `LOTE` (bytes dos lotes não vazios ÷ registos), contra o `REG` equivalente, e número de mensagens.
- **Falhas do ponto 9:** são ativadas com `-Drep04.erro=1..4` no líder (falha 5: `-Drep04.fator=1` nos participantes). Por omissão (`0`), o código é o correto. Assim não há cópias de código, e «repor o código correto» é não passar a propriedade.

### Ambiente dos testes

- Windows 11, Kubo 0.43.1, um único nó IPFS (API 5001), todos os processos na mesma máquina.
- Java: o JDK Temurin que estava instalado desapareceu durante o trabalho, por isso foi usado o JBR 25 do Android Studio (`D:\androidstudio\jbr`), compilando com `--release 11`.

**Recusas do daemon.** Como nas fichas anteriores, o Kubo recusa algumas publicações com `pub falhou (500): validation ignored`:
- **nos líderes:** 1 a 4 lotes por execução, que ficam fora de `hb-p<k>.txt`. São esses os `hb … em falta no ficheiro` e os `HB_PERDIDO` dos participantes;
- **no produtor:** 0 a 2 leituras por rajada.

Estas recusas são, nesta arquitetura, a causa «natural» de heartbeats perdidos.

---

## 2.2 · Diagnóstico inicial

**(a)** **Não.** Na COMINDIR03, o líder só publica quando há registos. Um minuto de silêncio é compatível com «não houve leituras de sensores de p2» e com «o líder de p2 morreu». Faltam à réplica duas coisas:
- um sinal **periódico**, emitido mesmo sem dados, cuja ausência signifique alguma coisa;
- o número de sequência mais recente do líder, para saber se o silêncio esconde registos.

**(b)** O último heartbeat antes da falha foi emitido em *t*<sub>hb</sub> ∈ \]*t*<sub>f</sub> − 5, *t*<sub>f</sub>\]. A suspeita surge entre *t*<sub>hb</sub> + 15 e *t*<sub>hb</sub> + 16, porque a verificação é feita de segundo a segundo.
- **Menor:** a falha ocorre mesmo antes do heartbeat seguinte (*t*<sub>hb</sub> → *t*<sub>f</sub> − 5). Deteção a pouco mais de **10 s** da falha.
- **Maior:** a falha ocorre logo a seguir a um heartbeat (*t*<sub>hb</sub> = *t*<sub>f</sub>) e a verificação acabou de passar. Deteção a **16 s**.

Logo, o tempo de deteção está em \]10 s, 16 s\].

**(c)** A ausência de heartbeats durante `TIMEOUT` não prova que o líder falhou: num sistema assíncrono, um líder lento ou um canal lento são indistinguíveis de um líder morto. Dois exemplos medidos nesta arquitetura:
1. **Pausa do processo líder** (GC longo, máquina sobrecarregada, `kill -STOP`). No ensaio 8(d), o líder de p2 esteve suspenso 21 s e foi suspeito por **todos** os participantes, apesar de estar correto (4 falsas suspeitas).
2. **O daemon IPFS recusa ou atrasa a publicação.** `validation ignored` fez perder heartbeats com o líder vivo (intervalos de **10 s** em `hb-p<k>.txt`, ensaio 8(b) com o produtor parado). Com `fator = 1` (falha 5), um só heartbeat recusado deu 4 falsas suspeitas.

Um atraso do lado do **participante** (a sua thread de subscrição bloqueada) produz o mesmo efeito.

---

## 4.1 · Heartbeat, envio em lote e detetor de falhas

### Antes de avançar (ponto 1): concorrência no `HeartbeatMonitor`

- **Threads que acedem ao estado:**
  - a(s) thread(s) de **receção** (`onHeartbeat`), uma por partição na réplica e no processador;
  - a thread de **verificação** (`check`);
  - no produtor e no processador, também a thread que vai **publicar** (`state()`).
- **Sem `synchronized`:**
  - **visibilidade:** a thread de verificação podia nunca ver o `lastNanos` atualizado (não há *happens-before*) e suspeitar de um líder vivo;
  - **intercalação:** `check` lê `elapsed > timeout`, entretanto `onHeartbeat` põe `ATIVO` e atualiza `lastNanos`, e depois `check` escreve `SUSPEITO`. O líder fica suspeito **depois** de um heartbeat recebido, e a `RETOMA` só aparece no heartbeat seguinte. Também se podia ler `inc` de uma incarnação e `hb` de outra, e gerar `HB_PERDIDO` ou `REINICIO` falsos.
- **Porque se ignora um `LOTE` com `inc` inferior:** vem de uma incarnação **anterior**, por exemplo:
  - um lote do líder antigo ainda em trânsito no PubSub, quando o novo já publicou. Os dois publicam por ligações diferentes e não há ordem garantida entre eles;
  - um líder «zombie» que estava suspenso e retoma depois de outro ter arrancado.

  Aceitá-lo faria o monitor voltar à incarnação antiga e assinalar um `REINICIO` para trás.
- **Os registos que ele transporta não devem ser descartados.** O `seq` continua de incarnação para incarnação, porque o novo líder parte do ficheiro. Um registo de uma incarnação antiga é um registo válido da partição, com o mesmo `seq` e o mesmo conteúdo que o ficheiro do líder tem. A regra de aplicação (duplicados ignorados por `seq`) já trata a repetição. Descartá-lo só obrigaria a uma recuperação TCP desnecessária. Por isso, a réplica aplica os registos mesmo quando o monitor ignora o heartbeat.

### Antes de avançar (ponto 3): `scheduleAtFixedRate` vs `scheduleWithFixedDelay`

- **`scheduleAtFixedRate` com o líder suspenso 20 s:** as execuções em atraso não se perdem nem se sobrepõem. Ficam **em fila** e correm **de seguida** logo que o processo retoma, e depois o ritmo volta à fase original. Medido no ensaio 8(d), em `hb-p2.txt`:

  ```
  …;3;1793791330460;0;0;39        ← último antes da pausa
  …;4;1793791356519;3;3;168       ← 26,1 s depois (pausa de 21 s): lote com as 3 leituras da pausa
  …;5;1793791356528;3;0;39        ┐
  …;6;1793791356539;3;0;39        │ 4 heartbeats em atraso em 40 ms
  …;7;1793791356546;3;0;39        │
  …;8;1793791356558;3;0;39        ┘
  …;9;1793791360460;3;0;39        ← de volta à fase original (330460 + 6 × 5000)
  ```

  Os participantes veem uma **rajada de heartbeats**: `RETOMA` no hb 4, e os seguintes renovam o temporizador sem mais nada.
- **`scheduleWithFixedDelay` com publicações de 2 s:** o atraso conta a partir do **fim** de cada execução, por isso o período efetivo passa a 5 + 2 = **7 s** e vai derivando. O `TIMEOUT` de 15 s continua a tolerar dois heartbeats atrasados, mas a margem diminui. Com `scheduleAtFixedRate`, uma publicação de 2 s não altera o período (5 s entre inícios).

### Antes de avançar (ponto 5): latência de um alarme

- **Latência máxima entre a receção da leitura pelo líder de origem e a aplicação do alarme numa réplica:**
  - a leitura espera até **Δ** no líder de origem;
  - o processador recebe o lote e publica o `IN` do alarme (milissegundos);
  - o líder de destino atribui-lhe `seq` logo à receção, mas o alarme espera até **Δ** pelo lote seguinte;
  - a réplica aplica-o ao receber esse lote.

  Total: ≈ **2Δ = 10 s**, mais os atrasos de publicação. Se o líder de destino estiver suspeito, o alarme pode perder-se (é o `AVISO`).
- **Os carimbos *L* e *V* do alarme:** a regra é **a mesma** (`Lp = max`, `Vp.merge`, cópia dentro do `lock`) e os heartbeats não lhes tocam. **Os valores mudam**, porque o processador passa a ver o mundo em degraus de Δ. Quando trata um registo de um lote, ainda não viu os registos das outras partições que estão pendentes nos respetivos líderes. Na COMINDIR03, já os teria recebido. O alarme fica com **menos dependências causais**, e a proporção de pares causais entre partições desce: ≈ 33 % na rajada do ensaio 8(b), contra ≈ 88 % na COMINDIR03 7(c).

---

## 4.2 · Ensaios

Todos os ensaios usam N = 3, réplicas 1 = {p0,p1}, 2 = {p1,p2}, 3 = {p2,p0}, Δ = 5000 ms e fator 3, salvo indicação em contrário. Os sensores S01, S04, S07 e S10 estão em p0, S02, S05 e S08 em p1, e S03, S06 e S09 em p2.

### Ponto 8

**(a) Figura 1.** `S01 21.0` e `S04 22.5`. Depois, logo a seguir a um `LOTE` vazio, `S01 21.4`, e o líder de p0 é terminado 0,3 s depois de a leitura ser publicada.

```
hb-p0.txt (inc;hb;instante;ultimoSeq;n;bytes)
…546;1;…144570;0;0;39
…546;2;…149564;0;0;39
…546;3;…154566;2;2;125      ← registos 1 e 2 (esperaram 3,5 s e 1,5 s)
…546;4;…159564;2;0;39       ← vazio
…546;5;…164564;2;0;39       ← vazio; S01 21.4 chega a seguir → seq 3 pendente
                             ← falha em …165136 (572 ms após hb 5)
…968;1;…183980;3;0;39       ← reinício (inc novo), lote VAZIO com ultimoSeq = 3
```

```
monitor-replica1.txt
…180505;SUSPEITA;p0;silencio_ms=15933
…184077;REINICIO;p0;inc=1793791144546->1793791183968
…184078;RETOMA;p0;inc=1793791183968;hb=1
…184080;RECUPERACAO;p0;de=3;ate=3
```

- `lider-p0.txt` tem 3 registos. Só 1 e 2 foram publicados em lote.
- As **duas** réplicas que guardam p0 (1 e 3) suspeitaram, viram o `REINICIO` e recuperaram o registo 3 por TCP. `PartitionCheck`: as duas são iguais ao líder (1..3).
- Tempo de deteção: **15,30 a 15,38 s**. A falha ocorreu só 0,57 s depois de um heartbeat, por isso fica perto do limite superior de 16 s.
- O mecanismo de lacunas da COMINDIR02 **não** teria recuperado o registo 3: não chegou nenhum registo 4 que revelasse a lacuna.
- `CausalCheck`: `V[0] = seq` OK.

**(b) Sem falhas.**

| | p0 | p1 | p2 |
|---|---|---|---|
| `rajada 300 10`: lotes (20 s) | 4 | 5 | 5 |
| registos (média por lote / máximo) | 30,0 / 87 | 23,4 / 84 | 20,8 / 68 |
| lotes vazios | 25 % | 40 % | 40 % |
| intervalo entre heartbeats (média / máx) | 5000 / 5001 ms | 4998 / 5001 ms | 4997 / 5001 ms |
| latência de lote (média / máx) | 2589 / 4998 ms | 2703 / 4980 ms | 2765 / 4957 ms |
| produtor parado 60 s: lotes vazios | 100 % (12) | 100 % (10) | 100 % (13) |
| produtor parado: intervalo (média / máx) | 5909 / **10000** ms | 6110 / **10002** ms | 5416 / **9999** ms |

- **A rajada demora ≈ 4 s** (300 × 10 ms mais o tempo de publicação), por isso cabe em 1 ou 2 lotes. Daí os máximos de 68 a 87 registos e os 25 a 40 % de lotes vazios, mesmo «em carga».
- **Com o produtor parado**, os máximos de 10 s são lotes recusados pelo daemon (5 `hb` em falta nos três ficheiros). Os participantes registaram-nos como `HB_PERDIDO` (3 a 5 por participante), mas **nenhuma** suspeita: um heartbeat perdido dá um silêncio de 10 s, abaixo dos 15 s.
- `CausalCheck` e `PartitionCheck`: OK, 6 de 6 réplicas iguais ao líder.

**(c) Falhas do líder de p1 durante `rajada 300 10`**, com reinício 30 s depois, 3 vezes:

| Falha | Último hb antes da falha | processador | produtor | réplica 1 | réplica 2 |
|---|---|---|---|---|---|
| 1 | 3,05 s | 12,92 s | 11,89 s | 12,91 s | 12,92 s |
| 2 | 4,13 s | 11,03 s | 10,99 s | 11,08 s | 11,03 s |
| 3 | 4,27 s | 10,94 s | 10,90 s | 10,98 s | 10,95 s |

- Mínimo **10,90 s**, médio **11,54 s**, máximo **12,92 s**: todos dentro de \]10 s, 16 s\]. O tempo de deteção é ≈ 15 s − (tempo desde o último heartbeat) + (fase da verificação, 0 a 1 s).
- 11 suspeitas classificadas como **falha real** (com `RETOMA` num `inc` novo) e 0 falsas suspeitas. Na primeira falha, o produtor ainda não tinha recebido nenhum lote de p1, porque arrancou depois de hb 2 e o líder morreu antes de hb 3: aparece como «sem líder observado».
- Depois de cada reinício: `REINICIO` em todos, `RECUPERACAO` nas réplicas 1 e 2 (os registos atribuídos entre o último lote e a falha), réplicas iguais ao líder e `V[1] = seq` OK.
- **Leituras perdidas sem vestígio:** o produtor enviou 93 + 76 + 72 = **241** leituras para p1, e `lider-p1.txt` só tem **53**. Perderam-se **188**, as que chegaram depois do `kill`. **Nenhum `AVISO`** no produtor: a rajada acabou ≈ 2,5 s depois da falha, muito antes da suspeita (≈ 11 s). Também se perderam todos os alarmes destinados a p1: p1 tem **0** alarmes, contra 36 em p0.

**(d) Líder de p2 suspenso durante 21 s**, com `S03 20.1`, `S06 21.2` e `S09 22.3` introduzidos durante a pausa:

```
…346284 processador SUSPEITA p2 silencio_ms=15810   │ …356534 RETOMA p2 inc=<o mesmo> hb=4
…346431 produtor    SUSPEITA p2 silencio_ms=15943   │ …356529 RETOMA …
…346462 réplica 2   SUSPEITA p2 silencio_ms=15989   │ …356551 RETOMA …
…346457 réplica 3   SUSPEITA p2 silencio_ms=15983   │ …356556 RETOMA …
```

- **4 falsas suspeitas**, todas seguidas de `RETOMA` com o **mesmo** `inc`, ≈ 10 s depois. O `HeartbeatCheck` classifica-as assim e não como falhas reais.
- Intervalos em `hb-p2.txt` após a retoma: **26 059 ms** até hb 4, depois **4 heartbeats em 40 ms** (as execuções em atraso de `scheduleAtFixedRate`) e por fim o regresso à fase original (ver «Antes de avançar» 3).
- **Destino das leituras introduzidas durante a pausa: não se perderam.**
  - o processo estava suspenso, mas a ligação HTTP de subscrição continuava aberta, e o daemon IPFS guardou as mensagens;
  - na retoma, as três leituras receberam `seq` 1..3 com o instante da retoma (`11:22:31.848`, `.872`, `.873`) e saíram no lote hb 4. A latência de lote foi de 4,7 s, medida a partir do instante de atribuição;
  - nenhum `AVISO`: a última leitura foi enviada antes de a suspeita surgir.

  Com uma pausa maior, ou com o buffer do daemon cheio, seriam perdidas.

**(e) Δ diferente em todos os processos**: um (b) com rajada, 60 s de produtor parado e outra rajada, seguido de um (c):

| Δ (ms) | Lotes / min / partição | Lotes vazios | Latência de lote média | Tempo de deteção (mín / méd / máx) | Intervalo previsto | Falsas suspeitas |
|---|---|---|---|---|---|---|
| 1000  | 60 | 89,8 % | 513 ms  | 3,15 / 3,85 / 4,11 s | \]2 s, 4 s\] | 0 (*) |
| 5000  | 12 | 79,7 % | 2535 ms | 11,96 / 12,25 / 12,90 s | \]10 s, 16 s\] | 0 |
| 10000 | 6  | 66,7 % | 3893 ms | 22,09 / 22,33 / 22,88 s | \]20 s, 31 s\] | 0 |

- **(\*) Com Δ = 1000 ms**, as réplicas arrancam 3 s antes dos líderes e o `TIMEOUT` é de 3 s. As 6 réplicas-partição suspeitaram ainda **antes** de receber o primeiro lote (estado `DESCONHECIDO` → `SUSPEITO`, «sem líder observado»). São suspeitas sobre líderes que ainda não existiam, não falsas suspeitas sobre líderes corretos.
- **Três valores de 4,06 a 4,11 s**, acima do limite de 4 s: o `taskkill` demorou mais de 115 ms a atuar depois de o instante ser anotado em `falhas.txt`, e o líder ainda publicou o hb 79 em `…408406`, 115 ms **depois** desse instante. Medido a partir desse último heartbeat, a deteção ficou em ≈ 3,9 s, dentro do previsto. Com Δ pequeno, a imprecisão da anotação da falha pesa mais.
- **Com Δ = 10000 ms**, a latência média (3,9 s) fica abaixo de Δ/2 = 5 s. As leituras não chegam uniformemente: vêm em rajadas de ≈ 4 s, e a sua posição relativa ao lote decide a espera.

### Ponto 9 — Falhas introduzidas

Cada falha foi ativada só nos líderes (`-Drep04.erro=<n>`), ou só nos participantes (falha 5), com o sistema completo, `rajada 300 10` e uma leitura isolada no fim.

| Falha introduzida | Lado onde surgiu | Detetada por `HeartbeatCheck` / `CausalCheck`? | O que permitiu concluir |
|---|---|---|---|
| Líder que só publica o `LOTE` quando `pendentes` não está vazia | Participantes (as suspeitas), por causa do líder | **`HeartbeatCheck`: sim.** 18 suspeitas sem nenhuma falha provocada: as 6 réplicas-partição suspeitaram 15 s após o arranque, antes de qualquer lote, e todos os participantes 15 s após a rajada. **`CausalCheck`: não** (OK) | Um heartbeat que depende dos dados deixa de ser um sinal de vida: volta a ambiguidade da COMINDIR03. Sem `falhas.txt`, o `HeartbeatCheck` classifica-as como «falha real» (não houve `RETOMA` até ao fim), mas os líderes estavam vivos. A incoerência entre suspeitas e falhas provocadas é a evidência |
| Líder que incrementa *L* e *V*\[*k*\] em cada heartbeat | Líder | **Pela verificação do próprio líder.** Quando chegou o primeiro `IN`, já tinham passado 3 heartbeats: `RECUSADO: V[0] = 4 diferente de seq = 1`. **Todas** as leituras foram recusadas (113 / 91 / 97). Não ficou nenhum registo, e o `CausalCheck` não tinha nada para verificar | O heartbeat não é um evento. Tratá-lo como tal faz *V*\[*k*\] avançar sem `seq`, e o invariante *V*\[*k*\] = `seq` da COMINDIR03 deixa de se verificar. Sem a verificação no líder, o `CausalCheck` mostraria `V[k] = seq: ERRO` em todas as linhas |
| Líder que lê `ultimoSeq` fora da secção sincronizada em que troca `pendentes` | Líder (cabeçalho errado), visível nas réplicas | **`HeartbeatCheck`: sim**, com `RECUPERACAO` (p1: `de=93;ate=95`; p2: `de=10;ate=10`) e **nenhum** `HB_PERDIDO` ou recusa que as justificasse. **`CausalCheck`: não** (OK) | Com uma pausa de 20 ms para alargar a janela, o lote hb 5 de p1 levou os registos 10..92 com `ultimoSeq = 95`: os registos 93..95 entraram em `pendentes` depois da troca e antes da leitura do `seq`. As réplicas recuperaram por TCP registos que vinham no lote seguinte (duplicados). Aqui é só desperdício. Se o `ultimoSeq` ficasse **abaixo** do último `seq` do lote, a falta deixaria de ser detetada |
| Líder que envia sempre `ultimoSeq` = 0 e descarta um em cada dez lotes antes de publicar | Líder; perdas nas réplicas | **`HeartbeatCheck`: sim**, `HB_PERDIDO;esperado=10;recebido=11` em todos os participantes e `hb 1..17, 1 em falta no ficheiro`. **`CausalCheck`: não** | O lote hb 10 levava os registos 1..94 (p0). O lote hb 11 trouxe 95..110: as réplicas ficaram com 16 retidos (mais de `LIMITE_RETIDOS`) e **a lacuna da COMINDIR02** recuperou 1..94. Sem `ultimoSeq`, a recuperação dependeu de chegarem registos depois. **Se o lote descartado fosse o último com dados, a perda nunca seria detetada**: é exatamente o caso da Figura 1 |
| Participantes executados com `rep04.fator` = 1 | Participantes | **`HeartbeatCheck`: sim**, 4 falsas suspeitas (p2, com `RETOMA` no mesmo `inc`). **`CausalCheck`: não** (OK) | Com `TIMEOUT` = Δ, **um** heartbeat recusado pelo daemon (p2, intervalo de 9987 ms) bastou para que todos os participantes de p2 suspeitassem. O *jitter* normal (até ≈ 15 ms) não provocou suspeitas: com verificação de segundo a segundo, o instante da verificação raramente cai nos poucos milissegundos em que `elapsed > Δ` antes de o heartbeat seguinte chegar. A margem é praticamente nula |

---

## 4.3 · Comparação e reflexão crítica

### Ponto 10 — Valores medidos (Δ = 5000 ms; 8(b) e 8(c))

| Métrica | Valor |
|---|---|
| Lotes publicados por partição (p0 / p1 / p2) | 8(b), 20 s: 4 / 5 / 5 · 8(c), 125 s: 26 / 8 (4 incarnações × 2) / 25 · ≈ 12 por minuto |
| Registos por lote (média / máximo) | 8(b): 24,4 / 87 · 8(c): 11,8 / 94 · lotes não vazios: 35 a 47 |
| Proporção de lotes vazios (rajada / produtor parado) | 35,7 % (5 em 14) / 100 % (35 em 35) |
| Intervalo entre heartbeats (média / máximo, ms) | 5000 / 5001 (sem recusas) · 10 002 com um lote recusado pelo daemon |
| Latência de lote (média / máximo, ms) | 2682 / 4998 (8(b)) · 2566 / 5028 (8(c)) |
| Tempo de deteção (mínimo / médio / máximo, s) | 10,90 / 11,54 / 12,92 (8(c), 12 medições) · 8(a): 15,30 / 15,35 / 15,38 |
| Falsas suspeitas | 0 (com Δ = 5000 e fator 3, sem pausas) · 4 em 8(d) |
| Mensagens no tópico de dados: COMINDIR03 / COMINDIR04 | 8(b): 341 / 14 · 8(c): 748 / 59 |
| Bytes por registo: `REG` individual / em `LOTE` | 65,5 / 48,6 (8(b)) · 66,9 / 50,1 (8(c)) |

### Ponto 11 — Questões

**Lote e latência.**
- **Poupança em mensagens:** de **341 para 14** (−96 %) na rajada do ensaio 8(b). Com o produtor parado há o efeito contrário: 12 mensagens por minuto e partição sem nenhum dado, porque o heartbeat tem um custo fixo.
- **Poupança em bytes:** de 65,5 para 48,6 bytes por registo (−26 %). O cabeçalho (≈ 40 bytes) é partilhado, e cada linha deixa de repetir `REG;127.0.0.1;700k;`.
- **A latência de lote aproxima-se de Δ/2:** 2,5 a 2,8 s com Δ = 5 s, e 513 ms com Δ = 1 s. Com Δ = 10 s fica abaixo (3,9 s), porque as leituras chegam em rajadas e não uniformemente. O máximo é sempre ≈ Δ.
- **Onde seria inaceitável esperar até Δ:** em alarmes e controlo em tempo real (incêndio, temperatura de um reator, travagem). Um alarme espera até 2Δ = 10 s entre origem e destino.
- **Alternativa:** **desacoplar** os dois períodos. Publicar os dados quando o lote atinge um tamanho ou uma idade máxima (por exemplo 100 ms), ou logo um `REG` por registo, e manter um heartbeat próprio e periódico com `ultimoSeq`. O heartbeat só é emitido se não tiver havido mensagem de dados no período, porque qualquer mensagem do líder serve de sinal de vida.

**Escolha do `TIMEOUT`.**
- **Compromisso:** o tempo de deteção cresce com o `TIMEOUT`: ≈ (fator − 1) a fator × Δ (3,9 s, 12,3 s e 22,3 s em 8(e)). A probabilidade de falsa suspeita diminui:
  - com fator 3, toleram-se **dois** heartbeats perdidos seguidos (8(b): um perdido, 0 suspeitas);
  - com fator 1, nenhum (falha 5: 4 falsas suspeitas por um único lote recusado);
  - nenhum `TIMEOUT` inferior à pausa tolera uma pausa de 21 s (8(d)). Só um `TIMEOUT` > 21 s, isto é fator ≥ 5 com Δ = 5 s, a teria evitado, à custa de deteções de 20 a 26 s.
- **Propriedade que falha durante a pausa de 8(d):** a **exatidão**. Um processo correto, o líder de p2, foi suspeito por todos.
- **Porque não há forma de a garantir num sistema assíncrono:** não existe limite para o atraso das mensagens nem para a velocidade dos processos. Qualquer `TIMEOUT` finito pode ser ultrapassado por um líder lento mas vivo, e esse caso é indistinguível de um líder morto. Só sob sincronia parcial (a partir de certo momento os atrasos são limitados) se obtém exatidão **eventual**.

**`ultimoSeq` e lacunas.**
- O mecanismo da COMINDIR02 só deteta uma lacuna quando chega um registo **posterior** (`seq > last + 1`). O `ultimoSeq` acrescenta a deteção da perda do **último** registo, ou dos últimos, sem precisar de tráfego posterior:
  - na Figura 1 (8(a)), o registo 3 só foi recuperado pelo `ultimoSeq = 3` de um lote **vazio**;
  - na falha 4, sem `ultimoSeq`, a perda do lote hb 10 só foi resolvida porque chegaram registos 95..110 depois. Se esse fosse o último lote com dados, as réplicas ficariam desatualizadas para sempre, sem o saber.
- **Porque é suficiente que esteja nos lotes vazios:** um lote vazio é emitido a cada Δ, faça o que fizer o produtor. Um registo perdido é, portanto, detetado no máximo um período depois. Num lote com registos, o mesmo campo cobre os registos que o lote não traz.

**Heartbeats e relógios lógicos.**
- Um evento, para os relógios lógicos, é algo que pode **influenciar** outro processo: uma atribuição de `seq`, a criação de um registo. O heartbeat não cria informação nova da partição: só repete `ultimoSeq`.
- Se fosse evento, *V*\[*k*\] contaria heartbeats além de registos e deixaria de ser igual a `seq`. Cada heartbeat tornaria também os registos posteriores «causalmente posteriores» a algo que nenhuma réplica guarda.
- **A verificação que o demonstra** é a da COMINDIR03, *V*\[*k*\] = `seq`, no líder (`RECUSADO`) e no `CausalCheck`. Com a falha 2, o primeiro `IN` chegou com *V*\[0\] = 4 e `seq` = 1 e foi recusado. Com o código correto, `V[k] = seq: OK` em todos os ensaios, mesmo com centenas de heartbeats.

**Detetar não é recuperar.**
- **Em 8(c), as leituras enviadas a p1 entre a falha e o reinício perderam-se sem vestígio:** 188 das 241 (e todos os alarmes para p1).
  - Nunca receberam `seq`, por isso nenhum `ultimoSeq` nem nenhuma lacuna as revela.
  - O produtor nem sequer avisou: o detetor só suspeitou ≈ 11 s depois, quando a rajada já tinha acabado.
  - Depois da suspeita, os `AVISO` só tornariam a perda **visível**, não a evitariam.
- **Para outro processo assumir a liderança de p1 sem duplicar `seq`:**
  - **eleição** de um único sucessor entre as réplicas de p1, com **consenso** (Raft/Paxos) ou um *lease* com épocas, para que só um se considere líder;
  - o novo líder parte do maior `seq` **aplicado** pelas réplicas, depois de as sincronizar;
  - uma **época** (o `inc` é um começo) em cada registo, para que os participantes rejeitem um líder antigo que retome;
  - **replicação síncrona** dos registos antes de responder, para que nada atribuído pelo líder antigo se perca;
  - **retenção e reenvio** das leituras pelo produtor até serem confirmadas.

  **Nada disto cabe no âmbito desta ficha**, que exclui eleição, consenso, *failover*, replicação do líder e retenção pelo produtor. **A solução desta ficha não resolve esse problema**: só o deteta.

**Medição e relógios físicos.**
- A latência de lote e o tempo de deteção comparam o `instante` do líder, o `RECEBIDO` de um participante e o instante anotado em `falhas.txt`. Isto só é válido sob a hipótese de que **todos os processos leem o mesmo relógio físico**, e por isso correm na mesma máquina.
- Mesmo assim há imprecisão: em 8(e) com Δ = 1000 ms, o `taskkill` atuou mais de 115 ms depois do instante anotado, o que pôs três medições 60 a 110 ms acima do limite teórico.
- Em máquinas diferentes, o desvio entre relógios (dezenas de ms com NTP, ou segundos sem ele) somar-se-ia a todas as medições. Podia até dar latências negativas.
- **O detetor não depende desta hipótese:** só mede **intervalos locais**, `System.nanoTime()` entre duas receções no **próprio** processo, sem nunca comparar instantes de processos diferentes. O `inc` e o `hb` são usados como identificadores, não como tempos.

---

## Critérios de aceitação

### CA1 · Fundamentos

- **Detetor de falhas:** um módulo local a cada processo que lhe fornece a lista de processos de que **suspeita** terem falhado. Aqui é um `HeartbeatMonitor` por partição, com os estados `DESCONHECIDO`, `ATIVO` e `SUSPEITO`.
- **Completude:** todo o processo que falha acaba por ser suspeito por todos os processos corretos. É garantida: um líder morto deixa de emitir e excede o `TIMEOUT`.
- **Exatidão:** nenhum processo correto é suspeito. Só sob sincronia parcial, isto é, se o intervalo observado entre heartbeats nunca exceder o `TIMEOUT`.
- **Porque só pode produzir suspeitas:** num sistema assíncrono, um processo lento e um processo morto são indistinguíveis por temporizadores (Chandra e Toueg, 1996). Por isso o estado é `SUSPEITO` e não «morto».

Perguntas de verificação:
- **Intervalo em que o líder de p0 já falhou mas a réplica ainda o considera `ATIVO`:** de *t* = 12 a *t* ≈ 25 s (em 8(a), 15,3 s medidos). Está em causa a **completude**, que é uma propriedade **eventual**. Não é violada, mas durante esse intervalo ainda não se verificou. O detetor tem um **atraso de deteção** que nenhuma configuração elimina. A exatidão não está em causa: não se suspeita de ninguém correto.
- **`FATOR_TIMEOUT` de 3 para 1:** degrada-se a **exatidão**. Basta um heartbeat perdido ou atrasado, ou um *jitter* que coincida com a verificação, para suspeitar de um líder vivo (falha 5: 4 falsas suspeitas por um só lote recusado). O tempo de deteção desce para \]0, 6\] s.
- **`FATOR_TIMEOUT` de 3 para 10:** degrada-se a **rapidez da completude**, com deteções em \]45, 51\] s. Durante esse tempo, o produtor continua a enviar leituras perdidas sem `AVISO`. Em troca, toleram-se 9 heartbeats perdidos e pausas até ≈ 50 s.
- **Porque o registo 3 só é recuperado pelo `ultimoSeq` de um lote vazio:** a regra de lacunas da COMINDIR02 só atua quando chega um `seq` maior do que `last + 1`. O registo 3 era o **último**, o líder morreu, e depois do reinício não chegaram leituras: nenhum registo 4 revelou a falta. O primeiro lote da nova incarnação, **vazio**, trouxe `ultimoSeq = 3`, e a réplica, com `last = 2`, pediu `PEDIDO 3 3`.

### CA2 · Implementação

- **Onde o líder acumula:** `PartitionLeader.onIn`, dentro de `synchronized (lock)` ([PartitionLeader.java:186](rep04-lider/src/rep04/PartitionLeader.java#L186)), com `pendentes.add(r)` ([linha 203](rep04-lider/src/rep04/PartitionLeader.java#L203)) depois do `append` e da atualização de `seq`, *L* e *V*.
- **Onde fecha o lote:** `enviarLote()` ([linha 214](rep04-lider/src/rep04/PartitionLeader.java#L214)), `synchronized (lock)` na [linha 218](rep04-lider/src/rep04/PartitionLeader.java#L218), com `pendentes = new ArrayList<>()`, `ultimoSeq = seq` e `nHb = ++hb` ([linhas 223-225](rep04-lider/src/rep04/PartitionLeader.java#L223-L225)).
- **Onde o publica:** fora do `lock`, `ps.publish(...)` ([linha 247](rep04-lider/src/rep04/PartitionLeader.java#L247)) e `hb-p<k>.txt` ([linha 253](rep04-lider/src/rep04/PartitionLeader.java#L253)). A agenda está na [linha 91](rep04-lider/src/rep04/PartitionLeader.java#L91).
- **Onde o detetor regista receções:** `PartitionReplica.Partition.onLote` chama `monitor.onHeartbeat(b.inc, b.hb)` ([PartitionReplica.java:120](rep04-replica/src/rep04/PartitionReplica.java#L120)), e o mesmo no processador e no produtor. O `onHeartbeat` está em [HeartbeatMonitor.java:25](rep04-lider/src/rep04/HeartbeatMonitor.java#L25).
- **Onde verifica o `TIMEOUT`:** `check()` ([HeartbeatMonitor.java:43](rep04-lider/src/rep04/HeartbeatMonitor.java#L43)), chamado pela thread `verificacao` ([PartitionReplica.java:245](rep04-replica/src/rep04/PartitionReplica.java#L245)).
- **Recuperação pelo `ultimoSeq`:** [PartitionReplica.java:136](rep04-replica/src/rep04/PartitionReplica.java#L136).
- **Secções sincronizadas:**
  - no líder, um **único** monitor (`lock`) protege `seq`, *L*, *V*, `pendentes`, `hb` e o ficheiro. É partilhado pela thread de entrada, pela agenda (`lote-p<k>`) e pela recuperação TCP;
  - no monitor, `synchronized` nos três métodos, partilhados pela receção, pela verificação e por quem publica;
  - na réplica, cada `Partition` sincroniza no próprio objeto: a sua subscrição e o resumo.

Perguntas de verificação:
- **A troca de `pendentes` e a leitura de `ultimoSeq` estão na mesma secção sincronizada que a atribuição de `seq`?** **Sim, o mesmo `lock`.** O que se observaria caso contrário:
  - **leitura de `seq` depois da troca, fora do `lock`** (falha 3): registos atribuídos entre as duas operações contam para `ultimoSeq`, mas não estão no lote. Medido: lote com 10..92 e `ultimoSeq = 95`, com `RECUPERACAO` desnecessárias nas réplicas;
  - **leitura antes da troca:** o lote pode trazer registos com `seq > ultimoSeq`;
  - **troca de `pendentes` sem `lock`:** concorrente com o `add` de `onIn`, um registo pode ir parar à lista antiga **depois** de ela ter sido codificada, e nunca é publicado (só recuperável por `ultimoSeq`). Pode também corromper o `ArrayList`.
- **Porque a publicação do lote ocorre fora da secção sincronizada:** `publish` é um pedido HTTP bloqueante ao daemon. Leva milissegundos, e segundos quando o daemon está lento.
  - Dentro do `lock`, a entrada (`onIn`) ficaria parada durante cada publicação e as leituras acumulavam-se no PubSub. A recuperação TCP (`serve` lê `seq` com o `lock`) também ficaria à espera.
  - Um daemon lento congelaria todo o líder, não só o heartbeat.
  - Não há nada partilhado a proteger durante a publicação: a lista trocada só é vista pela agenda.
- **Porque o detetor usa `System.nanoTime()` local:**
  - `nanoTime` é **monotónico** e não salta com acertos do relógio (NTP, mudança manual, hora de verão). Com `currentTimeMillis()`, um acerto para a frente provocava uma suspeita falsa, e um acerto para trás atrasava a deteção;
  - o `instante` dos registos é do relógio **do líder**, noutra máquina com outro desvio, e os lotes vazios nem o têm;
  - o que interessa ao detetor é o intervalo **observado** pelo próprio participante, que é o que a hipótese de sincronia parcial limita.

### CA3 · Análise

Com `HeartbeatCheck`, em todos os ensaios:
- **Regularidade:** intervalo médio de 5000 ms e máximo de 5001 a 5005 ms (Δ = 5000), 1000 / 1029 ms (Δ = 1000) e 10 000 / 10 035 ms (Δ = 10 000). Os únicos desvios são os lotes recusados pelo daemon (≈ 2Δ) e a pausa de 8(d) (26 s, seguida de 4 heartbeats em 40 ms).
- **Tamanho dos lotes:** 20 a 47 registos por lote não vazio durante as rajadas. Proporções de vazios no ponto 10.
- **Latência acrescentada:** ≈ Δ/2 (2,5 a 2,8 s com Δ = 5 s), com máximo ≈ Δ.
- **Deteção:** 10,90 a 15,38 s com Δ = 5 s, sempre dentro de \]10, 16\] s.
- **Falhas reais e falsas suspeitas:** 11 falhas reais em 8(c), todas com `RETOMA` num `inc` novo, e 4 falsas suspeitas em 8(d), todas com `RETOMA` no mesmo `inc`.

Com `CausalCheck`: `V[k] = seq: OK` e 0 violações da condição de relógio em **todos** os ensaios com o código correto (8(a) a 8(e)) e nas falhas 1, 3, 4 e 5. A falha 2 foi travada pela mesma verificação no líder.

Perguntas de verificação:
- **Os tempos de deteção de 8(c) cabem no intervalo previsto? Sim:** 10,90 a 12,92 s, em \]10, 16\] s. A deteção é ≈ `TIMEOUT` − (falha − último heartbeat) + fase da verificação (0 a 1 s) + atraso de receção (≈ 10 ms).
  - **Perto do mínimo:** as falhas 2 e 3 ocorreram 4,1 e 4,3 s depois de um heartbeat, isto é, mesmo antes do seguinte, e deram 10,9 a 11,1 s.
  - **Perto do máximo:** em 8(a), a falha foi 0,57 s depois de um heartbeat e deu 15,3 s.
  - **Diferenças entre participantes** da mesma falha (por exemplo o produtor 1 s antes dos outros): são a **fase** da thread de verificação de cada um, que arrancou num instante diferente.
- **Proporção de lotes vazios:** **35,7 %** numa `rajada 300 10` de 20 s. A rajada dura ≈ 4 s e cabe em 1 ou 2 períodos, por isso os restantes lotes do ensaio são vazios. **100 %** com o produtor parado. Ao longo de um ensaio de 2 minutos com duas rajadas (8(e), Δ = 5 s), 80 %.
- **Como `HeartbeatCheck` distingue uma falha real de uma falsa suspeita:** pelo **`inc` da `RETOMA` seguinte** comparado com o `inc` do último `RECEBIDO` antes da suspeita. O mesmo `inc` significa que o mesmo processo líder voltou a emitir, logo estava vivo, e é falsa suspeita. Um `inc` diferente significa que o processo foi substituído por um novo arranque, e é falha real.
  - **Informação que o torna possível:** a **incarnação** (`inc`) e o **número de heartbeat** (`hb`) em cada lote, registados no `RECEBIDO`. Sem `inc`, um reinício e uma retoma eram indistinguíveis.
  - O `hb-p<k>.txt` confirma: depois de uma falsa suspeita há heartbeats publicados da **mesma** incarnação.
  - **Sem `RETOMA`** até ao fim do ensaio (falha 1), não é possível distinguir. Só `falhas.txt` (as falhas efetivamente provocadas) o permite.

### CA4 · Limites

Perguntas de verificação:
- **Leituras enviadas para p1 enquanto o líder está suspeito:** são publicadas em `rep04-in-p1`, onde ninguém as consome, e **perdem-se sem vestígio**: nunca recebem `seq`, nenhum `ultimoSeq` as revela, e o produtor só escreve um `AVISO`. Pior, as que são enviadas **antes** de a suspeita surgir nem sequer têm aviso: em 8(c), perderam-se 188 de 241 com **0** avisos. O que seria necessário para não as perder:
  - **retenção pelo produtor**, com confirmação (o produtor guarda a leitura até ver o seu registo num lote) e **reenvio** depois do reinício, com identificadores para eliminar duplicados;
  - ou **substituição automática** do líder.

  Ambas estão fora do âmbito.
- **Todas as réplicas que guardam p1 suspeitam ao mesmo tempo? Não.** Cada participante tem o seu monitor, com a sua fase de verificação e o seu atraso de receção. Em 8(c), as suspeitas sobre a mesma falha diferiram até **1,03 s** (o produtor a 11,89 s e as réplicas a 12,9 s).
  - Uma réplica pode suspeitar enquanto outra ainda recebe heartbeats, por exemplo por um atraso só no seu nó.
  - Se cada uma decidisse **isoladamente** assumir a liderança, podiam surgir **dois ou mais líderes** ao mesmo tempo (*split-brain*). Cada um atribuiria `seq = last + 1` a leituras diferentes, com o mesmo número para registos distintos, e as réplicas divergiriam de forma irrecuperável.
  - O líder «suspeito» podia até estar vivo (8(d)) e continuar a atribuir números.
  - Seria preciso eleição com consenso ou *leases* com épocas: fora do âmbito.
- **Os tempos calculados por `HeartbeatCheck` continuam válidos com os processos em máquinas diferentes? Não.** O tempo de deteção subtrai um instante anotado na máquina onde se provocou a falha ao instante da `SUSPEITA` na máquina do participante. A latência de lote subtrai o `instante` do líder ao `RECEBIDO` do participante. Com relógios diferentes, o **desvio** entre eles soma-se à medição (positivo ou negativo) e pode ser da ordem do próprio valor medido. O detetor continua válido, porque só usa intervalos locais. Só as medições *a posteriori* perdem o significado.

**Limitações que se mantêm, e o que faria falta:**
1. **Detetar não é substituir o líder:** enquanto p1 está em baixo, a partição não aceita escritas. Faria falta eleição e consenso para *failover*.
2. **Leituras e alarmes perdidos sem vestígio** enquanto o líder está em baixo, sobretudo antes da suspeita. Faria falta retenção e reenvio com confirmação no produtor e no processador.
3. **Exatidão não garantida:** pausas de processo ou perdas de heartbeats no daemon produzem falsas suspeitas (8(d), falha 5). Faria falta um `TIMEOUT` maior (deteção mais lenta), ou detetores adaptativos (*φ accrual*), excluídos da ficha.
4. **Latência acrescentada** até Δ por troço (até 2Δ para alarmes). Faria falta desacoplar o heartbeat do envio dos dados.
5. **As medições dependem de um relógio físico comum.** Faria falta sincronização de relógios (NTP/Cristian), excluída da ficha.
