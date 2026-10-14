# Sprint E1-COMINDIR01 — Comunicação Indireta com Multicast e Publish/Subscribe

## Estrutura

Dois projetos separados (simulam máquinas distintas), todas as classes no pacote `rep01`:

| Projeto | Classes |
|---|---|
| `rep01-lider/src/rep01/`   | `SensorRecord`, `RecordFile`, `IpfsPubSub`, `MulticastLeader`, `PubSubLeader` |
| `rep01-replica/src/rep01/` | `SensorRecord`, `RecordFile`, `IpfsPubSub`, `MulticastReplica`, `PubSubReplica` |

`SensorRecord`, `RecordFile` e `IpfsPubSub` são iguais nos dois projetos e estão exatamente como na ficha.

### Comandos do líder (`MulticastLeader` e `PubSubLeader`)

| Comando | Efeito |
|---|---|
| `<sensor> <temperatura>` | regista um registo: `append` em `lider.txt` e depois propaga |
| `rajada <n>` | ponto 7: `n` registos do sensor `SIM` (15–30 °C, aleatórias), sem pausas; no fim mostra o instante de envio do primeiro e do último, o tempo e o débito |
| `desordem` | modo de teste do ponto 6: regista 3 registos por ordem, mas envia o 2.º e o 3.º antes do 1.º |
| `sair` | termina |

O `PubSubLeader` é uma cópia do `MulticastLeader` em que só muda o método `send` (a propagação): `socket.send(DatagramPacket)` → `ipfs.publish("rep01-temperaturas", r.toLine())`.

### Execução

Em cada projeto (Java 11+; testado com Java 21):

```bash
javac -d out src/rep01/*.java
```

**Parte A — multicast** (réplicas primeiro, cada uma no seu terminal, dentro de `rep01-replica`; o líder dentro de `rep01-lider`):

```bash
java -cp out rep01.MulticastReplica 1      # e 2, 3 noutros terminais
java -cp out rep01.MulticastLeader
fc lider.txt ..\rep01-replica\replica-1.txt   # Windows   (diff em Linux/macOS)
```

Confirmar que as réplicas estão à escuta: `netstat -an | findstr 6789` (uma linha `0.0.0.0:6789` por réplica). Se não receberem nada: `-Djava.net.preferIPv4Stack=true` e verificar a *firewall*.

**Parte B — PubSub do IPFS:**

```bash
ipfs init
ipfs config --json Pubsub.Enabled true
ipfs daemon
java -cp out rep01.PubSubReplica 1 5001     # <id> <porto da API do nó>
java -cp out rep01.PubSubLeader             # API por omissão: http://127.0.0.1:5001/api/v0
```

Segundo nó (PowerShell): `$env:IPFS_PATH="$HOME\.ipfs-no2"` e os comandos do ponto 12; depois, no primeiro nó, `ipfs swarm connect /ip4/127.0.0.1/tcp/4002/p2p/<id-do-nó-2>`.

> **Atenção 1:** os dois líderes escrevem no mesmo `lider.txt` e as réplicas no mesmo `replica-n.txt`. Entre experiências, apagar estes ficheiros — senão a numeração continua a partir do último `seq` e uma réplica que ficou retida continua retida.
>
> **Atenção 2 (Git Bash no Windows):** o Git Bash converte argumentos começados por `/` em caminhos (`/ip4/127.0.0.1/tcp/5002` passa a `C:/Program Files/Git/ip4/...`). Usar PowerShell, ou `export MSYS_NO_PATHCONV=1` antes dos `ipfs config` e `ipfs swarm connect`.

### Decisões de desenho

- **Réplicas** (`onMessage`, igual nas duas): `last` = último `seq` escrito (lido com `file.lastSeq()` ao arrancar); `pending` = `TreeMap<Long, SensorRecord>` com os registos retidos, ordenados por `seq`.
  - `seq == last + 1` → escreve e, em cascata, escreve os retidos que passaram a estar em sequência (`pending.firstKey() == last + 1`);
  - `seq <= last` ou já retido → `DUPLICADO: <seq>`;
  - `seq > last + 1` → `EM ESPERA: <seq> (falta o registo <last+1>)`, guarda em `pending`;
  - inválido → `REJEITADO: <conteúdo>`, a réplica continua. É inválido: menos/mais de 4 campos, número mal formado, `seq < 1`, **instante que não é uma data válida** e, no multicast, **datagrama que encheu o buffer** (ver falha 3: o Java trunca o datagrama sem avisar, e um registo truncado pode parecer válido).
- `onMessage` é `synchronized` porque há uma segunda thread (*daemon*) que, 2 s depois do último registo `SIM`, mostra o resumo da rajada (recebidos, escritos, retidos, duplicados, buracos, instantes do primeiro e do último). Os registos `SIM` não são mostrados um a um, para que a medição seja do mecanismo e não da consola.
- `catch (IOException e)` mostra `"IO: " + e` (com o tipo da exceção) em vez de `e.getMessage()`: a `ConnectException` do `HttpClient` não tem mensagem, e `IO: null` não permitia concluir nada.

### Ambiente dos testes e medições

Windows 11, Java 21.0.12 (Temurin), Kubo **0.43.1**, tudo na mesma máquina. Os dois nós IPFS foram criados com `ipfs init --profile=test` e endereços só em `127.0.0.1` (API 5001/5002, *swarm* 4001/4002), sem *bootstrap* e sem mDNS — os nós só se ligam com `swarm connect`, como pede o ponto 12. Cenários: **multicast** — 3 réplicas + líder; **PubSub um nó** — líder e 3 réplicas no nó 1; **PubSub dois nós** — líder e réplica 1 no nó 1, réplicas 2 e 3 no nó 2. Não houve acesso a uma segunda máquina, por isso o cenário com dois nós em máquinas diferentes não foi feito.

---

## 2.2 · Diagnóstico inicial

**(a)** Eliminam a dependência de **conhecer o servidor** (desacoplamento no espaço): o líder envia para um grupo (`230.0.0.1:6789`) ou para um tópico (`rep01-temperaturas`), sem saber quem ou quantos recebem. **Mantêm** a dependência de **estarem a correr ao mesmo tempo**: nenhum dos dois guarda mensagens, e quem não está à escuta no momento do envio perde-as.

**(b)** Datagrama para um grupo sem membros: é enviado na mesma (o `send` retorna sem erro) e é descartado pela rede ou pelo SO de destino — ninguém é avisado (falha 1: o líder enviou 3 registos sem réplicas e não houve exceção). Mensagem num tópico sem subscritores: o daemon aceita-a (`HTTP 200`, testado com o tópico `ninguem-ouve`) e descarta-a — também sem erro.

**(c)** «O líder enviou» quer dizer que o datagrama foi entregue ao SO, ou que o daemon local respondeu `200`. «A réplica tem o registo no ficheiro» quer dizer que chegou, passou a validação, estava em sequência e o `append` foi feito. O líder **só consegue saber o primeiro**: ninguém confirma a receção.

---

## 4.1 · Parte A — Multicast

### Antes de avançar (ponto 1): porque é que um `SensorRecord` sem `Serializable` viaja pela rede?

Porque não é o objeto que viaja: o líder converte-o em texto (`toLine()`) e o texto em bytes UTF-8; a réplica faz o inverso (`new String(..., UTF_8)` e `fromLine()`). Os dois lados têm de partilhar o **formato da linha**: 4 campos pela ordem `seq;sensor;temperatura;instante`, separador `;`, codificação UTF-8, ponto decimal (`Double.toString`/`parseDouble` não dependem da língua do sistema) e instante ISO. Também têm de usar o mesmo grupo/porto ou o mesmo tópico. Não precisam da mesma classe Java: um programa noutra linguagem lia a mesma linha.

### Antes de avançar (ponto 3): linhas que bloqueiam

| Programa | Linha | À espera de… |
|---|---|---|
| `MulticastLeader` | `sc.hasNextLine()` (l. 23) | que o utilizador escreva uma linha na CLI |
| `MulticastLeader` | `file.append(r)` (l. 47) | o disco (escrita curta, ≈ 0,25 ms medidos) |
| `MulticastLeader` | `socket.send(...)` (l. 59) | praticamente nada: entrega o datagrama ao SO e retorna; **não espera pelas réplicas** |
| `MulticastReplica` | `socket.receive(p)` (l. 36) | um datagrama enviado ao grupo, por quem quer que seja |
| `MulticastReplica` | `file.append(r)` (l. 83) | o disco |
| `PubSubLeader` | `sc.hasNextLine()` (l. 20) | a CLI |
| `PubSubLeader` → `IpfsPubSub.publish` | `http.send(...)` (`IpfsPubSub` l. 44) | a resposta HTTP do **daemon local** (≈ 2,7 ms), nunca das réplicas |
| `PubSubReplica` → `IpfsPubSub.subscribe` | `http.send(...)` (l. 55) | os cabeçalhos da resposta do daemon |
| `PubSubReplica` → `IpfsPubSub.subscribe` | `resp.body()...forEach(...)` (l. 61) | a próxima linha JSON, ou seja, a próxima mensagem do tópico. **Nunca termina** |

A réplica **confia** que recebeu um registo válido em `SensorRecord.fromLine(line)` (`MulticastReplica` l. 53, `PubSubReplica` l. 51). No código original, esta confiança era total: um datagrama mal formado lançava `IllegalArgumentException` e terminava a réplica.

### Ponto 4

Três réplicas e o líder, cinco registos: os três `fc`/`diff` mostram ficheiros **iguais**. Um único `send` por registo chegou às três réplicas.

### Ponto 5

Réplica 2 parada depois do registo 5; registos 6–8 enviados; réplica 2 reiniciada; registos 9–10.

**Numa frase:** a réplica 2 ficou sem os registos 6, 7 e 8 e, com o código original, escreveu o 9 e o 10 logo a seguir ao 5, deixando um buraco no ficheiro; **nenhum dos programas deu conta do problema** — não houve exceção nem aviso, só o `diff` mostrou a diferença (`6,8d5`).

### Ponto 6

`desordem` (registos 2 e 3 enviados antes do 1):

```
EM ESPERA: 2 (falta o registo 1)
EM ESPERA: 3 (falta o registo 1)
Aplicado: 1;ORD;24.3;...
Aplicado: 2;ORD;28.9;...
Aplicado: 3;ORD;21.4;...
```

O 2 e o 3 só foram escritos depois de chegar o 1, e o ficheiro ficou igual ao do líder. Com datagramas injetados: um repetido deu `DUPLICADO: 2`; `lixo`, `abc;S01;20;x`, `7;S01;20` e `0;S01;20;...` deram `REJEITADO`, e a réplica continuou à escuta.

**Ponto 5 repetido com a réplica alterada:** a réplica 2 arranca com `último registo aplicado: 5` e mostra `EM ESPERA: 9 (falta o registo 6)` e `EM ESPERA: 10 (falta o registo 6)`. O ficheiro fica parado no 5, sem buraco.

### Antes de avançar (depois do ponto 6)

- **Registos seguintes:** todos têm `seq > 6`, por isso ficam todos `EM ESPERA`. O 6 nunca vai chegar, porque nada o reenvia.
- **Ficheiro:** deixa de crescer de vez; fica parado no 5.
- **Estrutura em memória:** cresce sem limite, com um registo por cada mensagem do líder.
- **Reiniciar nesse estado:** perde-se tudo o que estava retido em memória, que nunca foi escrito. O ficheiro está intacto (só tem registos em sequência). A réplica volta a ler `lastSeq() = 5` e fica retida outra vez, agora sem os registos que estavam em memória.

### Ponto 7 — `rajada 1000` com três réplicas (valores no ponto 16)

| Execução | Envio no líder | Débito | Última receção (réplica mais lenta) − primeiro envio | Recebidos / escritos / retidos (cada réplica) |
|---|---|---|---|---|
| 1 | 547 ms | 1828 reg/s | 703 ms | 1000 / 1000 / 0 |
| 2 | 525 ms | 1905 reg/s | 579 ms | 1000 / 1000 / 0 |
| 3 | 420 ms | 2381 reg/s | 560 ms | 1000 / 1000 / 0 |

---

## 4.2 · Parte B — PubSub do IPFS

**Ponto 8.** `ipfs pubsub sub rep01-temperaturas` num terminal e `echo "teste" | ipfs pubsub pub rep01-temperaturas` no outro: `teste` apareceu no primeiro terminal.

### Antes de avançar (ponto 9)

O `publish` **não envia bytes para a rede**: envia um pedido HTTP `POST` ao **daemon local** (`127.0.0.1:5001`). É o daemon que publica no tópico e propaga a mensagem por *gossipsub* aos outros nós. A réplica fica bloqueada na `IpfsPubSub.subscribe`: primeiro no `http.send` (l. 55), à espera dos cabeçalhos; depois, para sempre, no `forEach` sobre `resp.body()` (l. 61), à espera da próxima linha JSON que o daemon escreve na resposta, que nunca termina.

### Pontos 10–12

- **Um nó:** pontos 4, 5 e 7 repetidos. Ponto 4: ficheiros iguais. Ponto 5: a réplica 2, ao voltar, mostra `EM ESPERA: 9 (falta o registo 6)`, como no multicast. `desordem`: o 12 e o 13 ficaram em espera até chegar o 11.
- **Dois nós:** antes do `swarm connect`, `ipfs swarm peers` estava vazio. Depois de `ipfs swarm connect /ip4/127.0.0.1/tcp/4002/p2p/12D3KooWPUEW…`, apareceu o nó 2. `ipfs pubsub peers rep01-temperaturas` no nó 1 dava `[]` sem réplicas no nó 2 e passou a mostrar `12D3KooWPUEW…` quando uma réplica subscreveu o tópico no nó 2. Pontos 4 e 7: as réplicas 2 e 3, ligadas ao nó 2, ficaram iguais ao líder.

### Antes de avançar (ponto 12)

Com um só nó, **as mensagens não passaram por *gossip* entre nós**: o daemon entregou cada mensagem diretamente às três subscrições locais. Só o segundo cenário demonstra que:

- a mensagem atravessa uma ligação libp2p entre dois nós por *gossipsub*;
- os nós têm de estar ligados (falha 8);
- o líder continua sem saber em que nó estão as réplicas.

Não tive duas máquinas disponíveis, por isso não repeti o cenário com um nó em cada máquina.

---

## 4.3 · Verificação

### Ponto 14 — Falhas introduzidas

Cada falha foi introduzida numa cópia do código (o código entregue não tem falhas). Usou-se a versão final da réplica, exceto onde se indica o código original.

| Falha introduzida | Mecanismo | Lado onde surgiu | Exceção obtida (ou «nenhuma») | Os ficheiros divergiram? | O que permitiu (ou não) concluir |
|---|---|---|---|---|---|
| Arrancar o líder e registar três registos antes de arrancar as réplicas | Multicast | Réplica (efeito); o líder não dá por nada | **Nenhuma** | Sim. Réplica final: `EM ESPERA: 4 (falta o registo 1)` e `EM ESPERA: 5`; `replica-1.txt` nem chega a ser criado. Código original: escreveu 4 e 5, e o `diff` mostra `1,3d0` | Sem desacoplamento no tempo, os datagramas enviados sem membros perdem-se em silêncio. O `seq` permite à réplica **detetar** o buraco, mas não recuperá-lo |
| Uma réplica com endereço de grupo `230.0.0.2` | Multicast | Réplica | **Nenhuma** | Sim. A réplica não recebe nada e o ficheiro não é criado; a outra réplica, em `230.0.0.1`, recebe tudo | O endereço de grupo é o «nome» do intermediário: um erro nele dá silêncio, não dá erro. Como não chega nada, nem o `seq` ajuda — não há buraco para detetar |
| `buffer` da réplica com 10 bytes em vez de 1000 | Multicast | Réplica | Código original: `IllegalArgumentException: Registo mal formado: 1;S01;21.5`, no `fromLine`, que **termina a réplica** (a exceção não é `IOException`). Versão final: **nenhuma**, `REJEITADO: 1;S01;21.5` | Sim: nenhum registo escrito | O `receive` **trunca o datagrama sem avisar**; o erro só aparece ao interpretar o texto. Com 20 bytes, o registo truncado `1;S01;21.5;2026-10-1` passava como válido (corrupção silenciosa). Por isso a versão final valida o instante e rejeita datagramas que enchem o buffer |
| Parar uma réplica a meio de `rajada 1000` e voltar a arrancá-la | Multicast | Réplica | **Nenhuma** (o processo foi morto; ao reiniciar não há erro) | Sim: `replica-2.txt` com 74 linhas e `lider.txt` com 1002. Reiniciada: `último registo aplicado: 74`, `EM ESPERA: 1001 (falta o registo 75)` | Perde-se tudo o que foi enviado enquanto esteve parada. A réplica deteta o buraco no registo seguinte e fica retida para sempre; as outras réplicas não são afetadas |
| Daemon IPFS do líder parado | PubSub | **Líder** | `java.net.ConnectException` (sem mensagem), lançada no `http.send` da `IpfsPubSub.publish` (l. 44), chamada por `PubSubLeader.send` (l. 58) e apanhada no `main` → `IO: java.net.ConnectException`; o líder termina | Sim. O registo 2 ficou em `lider.txt` (o `append` é anterior ao `publish`) e não chegou às réplicas. Com o daemon e o líder reiniciados, o registo 3 deu `EM ESPERA: 3 (falta o registo 2)` | É a única falha que o líder deteta, e só porque o intermediário local não responde. Mostra o custo da ordem «escrever, depois propagar»: o líder fica com um registo a mais e as réplicas ficam retidas |
| `Pubsub.Enabled` a `false` no nó das réplicas | PubSub | Réplica | `java.io.IOException: sub falhou (500)`, no `subscribe`; o daemon responde `experimental pubsub feature not enabled…` (visto com `ipfs pubsub ls`). O líder: **nenhuma** | Sim: a réplica termina sem ficheiro e o líder regista normalmente | A falha só é visível do lado que usa o nó mal configurado; o líder publica no seu nó e recebe `200` |
| Uma réplica a subscrever o tópico `rep01-temp` | PubSub | Réplica | **Nenhuma** | Sim: a réplica não recebe nada; `ipfs pubsub ls` mostra os dois tópicos, `rep01-temperaturas` e `rep01-temp` | O nome do tópico não é validado: um erro de escrita é **outro tópico**, e o resultado é silêncio |
| Dois nós sem ligação entre si (sem `swarm connect`, `bootstrap` vazio, mDNS desligado) | PubSub | Réplica do nó 2 | **Nenhuma** | Sim: a réplica do nó 1 recebeu tudo e a do nó 2 nada; `swarm peers` e `pubsub peers` vazios | A rede de nós ficou partida e o `publish` dá `200` na mesma. Nota: reiniciar só o nó 2 não chegou — o nó 1, que continuava a correr, lembrava-se do endereço do nó 2 e voltou a ligar-se sozinho. Foi preciso reiniciar os dois |

### Ponto 15 — Construções usadas

| Construção | Onde a usou | O que ficou a ser garantido | O que continua a não ser garantido |
|---|---|---|---|
| `MulticastSocket` / `joinGroup()` | `MulticastReplica.main`: `new MulticastSocket(6789)` + `joinGroup(230.0.0.1)`; `MulticastLeader.main`: `new MulticastSocket()` | A réplica recebe tudo o que for enviado para `230.0.0.1:6789`; várias réplicas partilham o porto (`SO_REUSEADDR`); o líder não precisa de conhecer as réplicas | Entrega, ordem, ausência de duplicados; receber o que foi enviado antes do `joinGroup`; que o emissor seja o líder |
| `setTimeToLive(1)` | `MulticastLeader.main` | O datagrama não atravessa nenhum *router*: fica na rede local | Nada sobre a entrega dentro da rede local; também não impede outros processos da rede local de enviar ou receber |
| `append` local antes da propagação | `MulticastLeader`/`PubSubLeader`: `file.append(r)` e só depois `send(...)` | O `lider.txt` contém sempre tudo o que foi propagado; o líder nunca reutiliza um `seq` já enviado (`lastSeq()` ao reiniciar) | Que as réplicas o tenham: se o envio falhar, o líder fica com um registo que ninguém recebeu (falha 5) |
| Número de sequência (`seq`) | Atribuído em `SensorRecord.now(++seq, …)` pelo líder; verificado em `onMessage` nas réplicas | Deteção de duplicados, buracos e desordem; o ficheiro da réplica só cresce pela ordem do líder, sem saltos | Recuperação do que falta; deteção da perda dos **últimos** registos (não chega nenhum depois que revele o buraco); limite à memória |
| Representação textual (`toLine` / `fromLine`) | `SensorRecord`; líder ao enviar e ao escrever, réplica ao receber e ao ler (`lastSeq`) | Formato igual no ficheiro e na rede, legível por qualquer linguagem e sem dependência da classe Java | Integridade: um datagrama truncado pode parecer válido (buffer de 20 bytes). O instante nem tem largura fixa (`LocalDateTime.toString()` omite `:00` nos segundos, ex.: `…T17:39`). Qualquer mudança de formato tem de ser feita nos dois lados |
| Tópico `rep01-temperaturas` | `PubSubLeader.TOPIC`, `PubSubReplica.TOPIC` | Desacoplamento no espaço: chega a todos os subscritores desse nome, em qualquer nó ligado | Guardar mensagens para quem subscreve depois; detetar nomes errados (falha 7); impedir que outros publiquem |
| Daemon IPFS e API HTTP | `IpfsPubSub.publish`/`subscribe` (`http.send` para `127.0.0.1:5001`) | O programa Java só fala HTTP com o seu nó; o daemon trata da rede, do *gossipsub* e da eliminação de duplicados, e consegue sair da rede local | Que o daemon esteja a correr (falha 5), com PubSub ativo (falha 6) e ligado aos outros nós (falha 8); entrega |
| Codificação multibase | `IpfsPubSub.toMultibase` (tópico no `arg`), `fromMultibase` (campo `data`) | Tópicos e dados com quaisquer bytes passam sem problemas num URL e num JSON; é o que o Kubo exige (sem ela: `HTTP 500`) | Nada sobre o conteúdo: é só codificação, não é cifra nem verificação de integridade |

---

## 4.4 · Comparação e reflexão crítica

### Ponto 16 — Valores medidos (média de 3 × `rajada 1000`, 3 réplicas, mesma máquina)

| Métrica | Multicast | PubSub — um nó | PubSub — dois nós |
|---|---|---|---|
| Tempo de envio da rajada no líder (ms) | **497** (547 / 525 / 420) | **3105** (3893 / 2700 / 2721) | **2881** (2865 / 2656 / 3122) |
| Latência da rajada até à réplica mais lenta (ms) | **614** (703 / 579 / 560) | **3105** (3893 / 2700 / 2721) | **2883** (2866 / 2658 / 3124) |
| Débito no líder (registos/s) | **2038** | **332** | **349** |
| Registos perdidos, por réplica | 0 / 0 / 0 | 0 / 0 / 0 | 0 / 0 / 0 |
| Duplicados detetados e registos retidos em memória no fim da rajada | 0 e 0 | 0 e 0 | 0 e 0 |
| Componentes que tiveram de estar a correr além da JVM | Nenhum (só o SO e a rede) | 1 daemon Kubo | 2 daemons Kubo (um por «máquina») |
| Passos de configuração até ao primeiro registo replicado | 0 (compilar e arrancar) | 4: instalar o Kubo, `ipfs init`, `Pubsub.Enabled true`, `ipfs daemon` | 13: os 4 anteriores + `IPFS_PATH`, `init`, `Pubsub.Enabled`, API, Gateway, Swarm e `daemon` no nó 2 + obter o `id` + `swarm connect` |
| O que aconteceu à réplica que arrancou atrasada | `EM ESPERA: 9 (falta o registo 6)`: ficheiro parado no 5, tudo o resto retido | Igual | Igual (réplica 2, no nó 2) |
| Funcionaria entre duas redes diferentes sem alterar os *routers*? | **Não**: os *routers* não encaminham multicast, e com TTL 1 o datagrama morre no primeiro *router* | **Sim**, se os nós se conseguirem ligar: as ligações libp2p são TCP/QUIC *unicast* normais | **Sim**, idem; aqui só se demonstrou com dois nós na mesma máquina |

Notas sobre as medições:

- No PubSub, o «envio do primeiro» é registado quando o `publish` retorna, e o daemon já entregou a mensagem às subscrições locais antes de responder. Por isso a réplica chega a registar a receção do primeiro **antes** desse instante (ex.: 632798 vs 632812), e a latência é praticamente igual ao tempo de envio.
- A primeira execução de cada série é mais lenta (aquecimento da JVM e abertura da ligação HTTP).
- **Micro-medição** (1000 operações isoladas, 3 vezes): `append` 241–266 ms (≈ 0,25 ms cada), `send` multicast 39–42 ms (≈ 0,04 ms), `publish` 2499–2863 ms (≈ 2,7 ms).
- **Teste de stress** (fora do ponto 16):
  - `rajada 20000` em multicast: as réplicas receberam 18 997 / 19 001 / 19 036 registos (perderam 1003 / 999 / 964). O primeiro buraco foi em `seq` 5581 / 5574 / 5672 — diferente em cada réplica. No fim, cada uma tinha ≈ 5600 registos escritos e ≈ 13 400 retidos.
  - `rajada 5000` em PubSub com dois nós: 0 perdas.

### Ponto 17 — Questões

**Desempenho.** O **multicast** teve menor latência (614 ms contra ≈ 2900–3100 ms por cada 1000 registos) e maior débito (≈ 2000 contra ≈ 340 registos/s).

- **Multicast:** entre o `send` e o `receive` quase não se gasta tempo — o `send` custa ≈ 0,04 ms e o SO copia o datagrama para cada socket do grupo. O tempo vai sobretudo para o `append` (≈ 0,25 ms por registo, no líder **e** em cada réplica). As réplicas ficam ≈ 100 ms atrás do líder porque os datagramas esperam no buffer de receção de cada socket.
- **PubSub:** cada registo é um pedido HTTP completo ao daemon do líder — corpo *multipart*, interpretação, assinatura da mensagem, encaminhamento *gossipsub* e resposta —, ≈ 2,7 ms medidos. A seguir, o daemon da réplica (no mesmo nó, ou a um salto por libp2p) escreve uma linha JSON na resposta do `subscribe`. O estrangulamento é o `publish` no líder; as réplicas acompanham sem atraso (a última receção é ≈ 0–2 ms depois do último envio). O segundo nó não acrescentou atraso mensurável na mesma máquina; entre máquinas acrescentaria o tempo de rede de cada salto.

**Escalabilidade.**

- **Multicast, 3 → 300 réplicas:** o líder continua a fazer **um** `send` por registo. Na rede local passa no máximo uma cópia por ligação até às máquinas que se juntaram ao grupo, e a carga de cada réplica não muda. O tráfego total cresce, no máximo, com o número de ligações da rede, não com o número de réplicas.
- **PubSub, 3 → 300 réplicas:** cada nó reencaminha cada mensagem para os seus vizinhos da malha (por omissão ≈ 6). O tráfego total cresce ≈ nº de nós × nº de vizinhos, e cada nó recebe a mesma mensagem várias vezes (os duplicados são eliminados na entrega, mas já gastaram rede). A carga de cada nó fica limitada pelo nº de vizinhos, não pelo nº total de nós. O nó do líder envia a todos os vizinhos subscritores a que está ligado, e a latência cresce com o número de saltos.
- **Réplicas espalhadas pela Internet:** o multicast deixa de funcionar. O PubSub continua a funcionar, com a latência de cada salto pela Internet.

**Fiabilidade.** Na rajada de 1000 nenhum dos dois perdeu registos. No stress, o **multicast** perdeu ≈ 5 % (≈ 1000 de 20 000 por réplica) e o PubSub nada (5000).

As perdas do multicast foram **no recetor**, e sabe-se por três razões:

1. o `lider.txt` tem os 20 000 registos e o `send` nunca falhou — logo, não foi o emissor;
2. cada réplica perdeu registos **diferentes** (primeiro buraco em 5581, 5574, 5672). Se fosse o emissor ou a rede antes de o SO copiar o datagrama, todas perderiam os mesmos;
3. o líder envia mais depressa do que cada réplica consegue fazer `fromLine` + `append`. O buffer de receção do socket encheu e o SO descartou os datagramas seguintes. Depois da primeira perda, cada registo passa a escrever `EM ESPERA` na consola, a réplica fica ainda mais lenta, e as perdas multiplicam-se.

No PubSub, o próprio `publish` (≈ 3 ms) limita o ritmo do líder, e as réplicas nunca ficaram para trás. Se ficassem, o daemon também descartaria mensagens para um subscritor lento: é *best-effort*.

O `seq` permitiu **saber** exatamente o que faltava (buracos, duplicados, desordem) e garantir que o ficheiro nunca tem saltos. **Não** permitiu **recuperar** nada, nem detetar a perda dos últimos registos de uma rajada.

**Recuperação.** Para se pôr em dia sozinha, a réplica precisaria de obter os registos em falta: pedi-los ao líder (confirmações/retransmissão) ou receber uma cópia do estado. Ou então o intermediário teria de guardar as mensagens (desacoplamento no tempo, como numa fila de mensagens). **Nada disto cabe no âmbito desta ficha**: a replicação de base não resolve o problema, e a resposta fica para mais tarde. Com os mecanismos da ficha, só se pode **detetar** e **parar** de escrever.

**Dependências e alcance.**

- **Num único edifício**, escolheria o **multicast**: não precisa de nada além da JVM (0 passos de configuração contra 4 ou 13), teve ≈ 5× menos latência (614 ms contra ≈ 3000 ms por 1000 registos) e ≈ 6× mais débito. O ritmo de uma rede de sensores (algumas leituras por segundo) está muito abaixo do ponto em que o multicast perdeu registos (≈ 2400 registos/s mantidos durante vários segundos).
- **Em várias cidades**, escolheria o **PubSub**: o multicast não atravessa os *routers*. O PubSub funcionou entre dois nós com o mesmo comportamento e sem perdas, à custa de um daemon por máquina, de ≈ 3 ms por publicação e de ter de garantir que os nós se encontram (falha 8).

**Ponto único de falha.** **Nenhuma** das duas soluções ajuda: só o líder atribui `seq`, e se ele falha não há escritas (as réplicas ficam corretas até ao último registo recebido). O desacoplamento no espaço até permitiria que outra máquina publicasse no mesmo grupo ou tópico — no PubSub qualquer nó pode publicar.

Mas com **duas máquinas a registar ao mesmo tempo**, cada uma atribuiria `seq` sozinha e ambas usariam o mesmo número para registos diferentes. Cada réplica escreveria o primeiro que lhe chegasse com esse `seq` e trataria o outro como `DUPLICADO`. Réplicas diferentes podiam ficar com registos diferentes no mesmo `seq`, uma divergência que o número de sequência **não deteta**. Escolher um novo líder, ou pôr duas máquinas de acordo sobre a ordem, exige eleição ou consenso: fora do âmbito desta ficha.

**Segurança.** As réplicas **não conseguem** distinguir um registo do líder de um forjado: aceitam qualquer linha bem formada com o `seq` esperado. Ficou demonstrado com o injetor de datagramas e com o `ipfs pubsub pub` pela CLI — e um registo forjado com o próximo `seq` seria escrito, e o verdadeiro passaria a `DUPLICADO`.

- **Multicast:** o endereço de origem do `DatagramPacket` (`p.getAddress()`) não chega. Qualquer processo na máquina do líder tem o mesmo endereço, e o endereço de origem de um datagrama UDP pode ser falsificado.
- **PubSub:** o campo `from` **ajudaria**. Identifica o nó que publicou, e o Kubo assina as mensagens por omissão (`Pubsub.DisableSigning: false`), por isso um nó não consegue fazer-se passar por outro. A verificação teria de ser feita **na réplica**, antes de aplicar o registo (em `onMessage`, ou no `subscribe`, que hoje só extrai o campo `data`): comparar o `from` com o *peer ID* do nó do líder. Mesmo assim, identifica o **nó**, não o programa: qualquer processo com acesso à API do nó do líder publica com o mesmo `from`.

Autenticar o próprio registo vai além destes mecanismos: a replicação de base não resolve o problema.

---

## Critérios de aceitação

### CA1 · Modelo de replicação

**Caminho de um registo.**

1. CLI → `sc.nextLine()` → validação do formato → `SensorRecord.now(++seq, …)`.
2. `file.append(r)` em `lider.txt`.
3. `toLine()` → bytes UTF-8.
4. Propagação:
   - **multicast:** um `DatagramPacket` para `230.0.0.1:6789` → SO/rede → uma cópia para cada socket que fez `joinGroup` → `receive` na réplica;
   - **PubSub:** `POST /pubsub/pub?arg=u…` ao daemon local → *gossipsub* → daemon da réplica → linha JSON na resposta do `POST /pubsub/sub` → `extractField("data")` → `fromMultibase`.
5. Na réplica: `onMessage` → validação → regra do `seq` → `append` em `replica-n.txt`, ou retido em `pending`.

**Bloqueios:** ver a tabela do ponto 3. O líder bloqueia **na leitura da CLI** (sempre) e, no PubSub, **também no envio** (`http.send` até o daemon responder, ≈ 2,7 ms). No multicast o `send` não espera por ninguém. A réplica bloqueia no `receive`, ou no `forEach` do `subscribe`.

- **Falha entre o `append` e o envio:** o líder tem o registo *n* e as réplicas não. Ao reiniciar, o líder continua em *n+1* e as réplicas ficam retidas à espera de *n* para sempre (demonstrado na falha 5).
- **Ordem inversa** (enviar, depois escrever): as réplicas teriam *n* e o líder não. Ao reiniciar, o líder voltava a usar *n* para um registo **diferente**; as réplicas tratavam-no como `DUPLICADO` e ignoravam-no. Ficavam registos diferentes com o mesmo `seq`, uma divergência que **ninguém deteta**.
- Por isso: «escrever localmente, depois propagar». O ficheiro do líder contém sempre tudo o que foi propagado, e uma falha dá, no pior caso, uma réplica **atrasada e consciente disso** (`EM ESPERA`), nunca uma réplica com dados contraditórios.

**Porque é o líder a numerar.** Só o líder conhece a ordem das escritas. Se cada réplica numerasse pela ordem de chegada, perdas e desordem dariam numerações diferentes em cada réplica, e deixava de ser possível detetar buracos ou duplicados.

Ao **reiniciar**, o líder lê `lastSeq()` do `lider.txt` e continua em *último + 1*. Se o `lider.txt` se perdesse, voltava ao 1: as réplicas tratariam tudo como `DUPLICADO` até ultrapassar o seu último `seq`, descartando registos novos em silêncio.

**O líder sabe quantas réplicas existem?** Não. No código não há lista, contador nem endereço de réplicas: só as constantes `GROUP`/`PORT` (multicast) e `TOPIC` (PubSub). É isso que torna possível juntar réplicas sem mudar o líder.

### CA2 · Replicação com multicast

- **Datagramas que saem da placa de rede por cada `send`:** **um**, mesmo com três réplicas na mesma máquina — as três cópias locais são feitas pelo SO, não pela placa de rede. Com três réplicas em três máquinas da mesma rede, continua a ser **um**: o *switch* e as placas de rede das máquinas que se juntaram ao grupo é que fazem as cópias.
- **Bind ao mesmo porto:** o construtor `MulticastSocket(6789)` ativa `SO_REUSEADDR` antes do `bind`. O `netstat` mostrou três sockets em `0.0.0.0:6789`, e o SO entrega uma cópia de cada datagrama do grupo a cada um.
- **Réplica arrancada depois do líder:** **nenhuma exceção**. A réplica descobriu que faltavam registos porque o primeiro `seq` recebido (4) era maior do que *último + 1* (1): `EM ESPERA: 4 (falta o registo 1)`. Guardou o 4 e o 5 em memória e não escreveu nada no ficheiro; com o código original, escreveu-os logo, com um buraco.
- **TTL** (número máximo de *routers* que o datagrama pode atravessar):
  - `1`: não passa nenhum *router*; fica na rede local;
  - `0`: nem sai da máquina — testado: uma réplica na **mesma** máquina continuou a receber;
  - `32`: poderia atravessar até 31 *routers*, se estes encaminhassem multicast — na Internet, em geral, não encaminham; o único efeito garantido é expor o tráfego a mais redes.

### CA3 · Replicação com PubSub do IPFS

- **O programa Java não fala com as réplicas:** fala só com o **seu daemon**, por **HTTP** (`POST` para `http://127.0.0.1:5001/api/v0`). Os daemons falam entre si por libp2p (TCP/QUIC) com *gossipsub*.
- **Multibase:** os nomes de tópico e os dados podem ter quaisquer bytes, que não cabem diretamente num URL nem numa *string* JSON. O base64url sem *padding* só usa caracteres seguros, e o prefixo `u` diz qual foi a base usada. **Sem codificação** (testado): `HTTP 500 — "URL arg must be multibase encoded: selected encoding not supported"`, ou seja, `IOException: pub falhou (500)` no líder e `sub falhou (500)` na réplica.
- **O pedido de subscrição nunca termina:** fica bloqueado `PubSubReplica.main` → `IpfsPubSub.subscribe`, primeiro no `http.send` (l. 55) e depois, para sempre, no `forEach` sobre `resp.body()` (l. 61). Cada linha recebida chama `onMessage` na própria thread principal.
- **Dois nós:** segundo repositório (`IPFS_PATH`) com portos próprios (API 5002, Gateway 8081, Swarm 4002) e `ipfs swarm connect /ip4/127.0.0.1/tcp/4002/p2p/<id>` no primeiro. Confirmação: `ipfs swarm peers` mostrou o nó 2 e `ipfs pubsub peers rep01-temperaturas` mostrou o *peer ID* do nó 2 depois de lá subscrever uma réplica.

### CA4 · Falhas, consistência e interpretação

- **Falhas sem exceção nenhuma:**
  - réplica arrancada depois do líder;
  - grupo `230.0.0.2`;
  - buffer de 10 bytes (versão final);
  - réplica parada a meio da rajada;
  - tópico `rep01-temp`;
  - nós sem ligação.

  Foram detetadas **comparando os ficheiros** (`fc`/`diff`, número de linhas), pelas mensagens `EM ESPERA`/`REJEITADO` nas réplicas e, no PubSub, com `ipfs pubsub ls`, `swarm peers` e `pubsub peers`.
- **Réplica retida à espera de um registo perdido:** o ficheiro deixa de crescer e fica correto até ao último registo em sequência. A memória cresce com cada mensagem nova, sem limite. **Não consegue sair desse estado sozinha** com os mecanismos desta ficha: ninguém volta a enviar o registo em falta.
- **Daemon do líder parado:**
  - **lado:** o líder;
  - **onde:** no `http.send` da `IpfsPubSub.publish` (l. 44), chamado por `PubSubLeader.send` (l. 58);
  - **tipo:** `java.net.ConnectException`, uma `IOException` sem mensagem;
  - **efeito:** apanhada no `main` (`IO: java.net.ConnectException`), e o líder termina. O registo já estava no `lider.txt`.
- **Rajada de 1000:** 0 perdas em qualquer mecanismo. Só com 20 000 o multicast perdeu ≈ 1000 registos por réplica, **no recetor** — o líder tem todos e cada réplica perdeu registos diferentes (ver «Fiabilidade»).

### CA5 · Comparação e limites

- **Menor latência:** o multicast (614 ms contra 2883–3105 ms por 1000 registos). Um `send` é uma chamada ao SO (≈ 0,04 ms); um `publish` é um pedido HTTP completo ao daemon, com assinatura e encaminhamento (≈ 2,7 ms).
- **Réplicas em três cidades:** continuava a funcionar o **PubSub**, porque as ligações entre nós são *unicast* normais pela Internet; basta que os nós se encontrem (*bootstrap* ou `swarm connect`). O multicast não passa os *routers*.
- **Réplica nova amanhã:** arranca com `lastSeq() = 0`, o primeiro registo que recebe tem um `seq` muito maior, e fica `EM ESPERA` para sempre. **Nenhum** dos mecanismos ajuda: não guardam mensagens (não há desacoplamento no tempo). Precisaria de transferência de estado ou de um intermediário que guardasse as mensagens — fora do âmbito.
- **O líder deixa de funcionar:** as escritas param, e as réplicas ficam como estavam (corretas até ao último registo recebido). Os registos que o líder escreveu e não propagou ficam só no `lider.txt`. Escolher outro líder e continuar a numeração sem conflitos (eleição, consenso) está fora do âmbito.

**Limitação comum escolhida: não há confirmação de entrega nem recuperação.** Nos dois mecanismos o líder envia uma vez e não sabe quem recebeu; uma réplica que perde um registo fica retida para sempre. Para o resolver faria falta que as réplicas pedissem os registos em falta, ou que o intermediário guardasse as mensagens até serem consumidas. A replicação de base não resolve o problema, e a resposta fica para mais tarde.
