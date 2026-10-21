# Sprint E1-COMINDIR02 — Particionamento e Recuperação com Publish/Subscribe

## Estrutura

Três projetos separados (simulam máquinas distintas), todas as classes no pacote `rep02`:

| Projeto | Classes |
|---|---|
| `rep02-produtor/src/rep02/` | `SensorRecord`, `RecordFile`, `IpfsPubSub`, `Wire`, `Partitioner`, **`Producer`** |
| `rep02-lider/src/rep02/`    | `SensorRecord`, `RecordFile` (+ `range`), `IpfsPubSub`, `Wire`, `Partitioner`, **`PartitionLeader`** |
| `rep02-replica/src/rep02/`  | `SensorRecord`, `RecordFile` (+ `range`), `IpfsPubSub`, `Wire`, `Partitioner`, **`PartitionReplica`**, **`PartitionCheck`** |

- `SensorRecord`, `RecordFile` e `IpfsPubSub` são as da COMINDIR01, com o pacote alterado para `rep02` (mais nada).
- `RecordFile.range`, `Wire` e `Partitioner` estão exatamente como no enunciado.

### Execução

Em cada projeto: `javac -d out src/rep02/*.java`. Depois, cada programa no seu terminal, **réplicas → líderes → produtor**, todos na **mesma pasta** (os ficheiros `lider-p<k>.txt` e `replica-<n>-p<k>.txt` ficam aí, para o `PartitionCheck`):

```bash
# Parte A (uma partição)
java -cp out rep02.PartitionReplica 1 0          # e 2, 3; opcional: -Dperda=0.2 ou -Ddescartar=<seq> antes de -cp
java -cp out rep02.PartitionLeader 0 1           # [endereço] [api]
java -cp out rep02.Producer 1                    # [api]

# Parte B (N = 3, R = 2)
java -cp out rep02.PartitionReplica 1 0,1
java -cp out rep02.PartitionReplica 2 1,2
java -cp out rep02.PartitionReplica 3 2,0
java -cp out rep02.PartitionLeader 0 3           # e 1 3, 2 3
java -cp out rep02.Producer 3
java -cp out rep02.PartitionCheck 3 <pasta>      # [R], 2 por omissão
```

Confirmar os portos de recuperação: `netstat -an | findstr 700`. Testar a recuperação à mão: ligar-se a `127.0.0.1:7000` e escrever `PEDIDO 1 5`.

### Comandos

| Programa | Comando | Efeito |
|---|---|---|
| `Producer` | `<sensor> <temperatura>` | publica `IN;<sensor>;<temp>` em `rep02-in-p<k>`, com `k = of(sensor)`, e mostra `→ p<k>` |
| `Producer` | `rajada <n> <s>` | *n* leituras de sensores aleatórios `S01..S<s>`, 15–30 °C; no fim, os instantes da primeira e da última publicação e quantas leituras foram para cada partição |
| `Producer` | `rajada-quente <n> <s>` | igual, mas 80 % das leituras são de `S01` e as restantes de `S02..S<s>` |
| `Producer` | `sair` | termina |

### Decisões de desenho

- **Líder** (`PartitionLeader`):
  - **thread `entrada-p<k>`:** `subscribe(topicIn(k))` → por cada `IN`: valida, verifica a partição, depois `synchronized (lock) { seq + 1; append; seq = ... }` e só então `publish(topicData(k), Wire.reg(...))`;
  - **thread principal:** `ServerSocket(7000 + k)`, atende **uma ligação de cada vez**;
  - **estado partilhado:** o `seq` (protegido por `lock`) e o ficheiro (os métodos do `RecordFile` são `synchronized`).
- O `seq` só avança **depois** de o `append` correr bem. A recuperação lê o `seq` dentro do `lock` e limita o *até* a esse valor, por isso nunca pede ao ficheiro um registo que ainda lá não está.
- **Se a publicação de um `REG` falhar**, o líder **não termina**: o registo já está no ficheiro, e as réplicas recuperam-no por TCP quando derem pela falta. Foi exatamente o que aconteceu nos testes (ver «Recusas do daemon»).
- **Réplica** (`PartitionReplica`):
  - um objeto `Partition` por partição, com estado próprio (`last`, `pending`, endereço e porto do líder, ficheiro) e uma **thread de subscrição** própria;
  - a recuperação corre **na própria thread de subscrição**, como pede o enunciado;
  - os métodos de `Partition` são `synchronized` no próprio objeto, porque uma thread auxiliar lê o estado para mostrar o resumo `RESUMO` depois de 2 s sem mensagens.
- **Mensagens das réplicas:** têm o prefixo `[p<k>]` (há várias partições no mesmo processo). Mantêm-se `DUPLICADO`, `EM ESPERA`, `REJEITADO`, `RECUPERAÇÃO`, `RECUPERADO`, `RECUPERAÇÃO FALHOU` e `DESCARTADO (simulação)`.
- **Simulação de perdas:** `-Dperda` e `-Ddescartar` só se aplicam às mensagens recebidas pelo PubSub. O `-Ddescartar=<seq>` descarta a primeira receção desse `seq` **em cada partição** da réplica.
- **Rajadas:** durante uma rajada, o líder e a réplica não mostram cada registo (só os que chegam com mais de 200 ms de intervalo), para a consola não atrasar a medição. O resumo (`LOTE` no líder, `RESUMO` na réplica) aparece no fim.
- **Produtor:**
  - se o daemon **recusar** uma publicação (resposta diferente de 200), a leitura é mostrada como `PUBLICAÇÃO RECUSADA` e contada, e o produtor continua;
  - só termina se o daemon não estiver acessível (`ConnectException`);
  - uma leitura recusada perde-se no troço produtor → líder, que é uma das limitações que a ficha quer que se reconheça.
- **`PartitionCheck <N> <pasta> [R]`:** o argumento opcional `R` (2 por omissão) permite verificar também o cenário N = 1 com três réplicas.

### Ambiente dos testes

Windows 11, Java 21 (compilado com `--release 11`), Kubo 0.43.1, tudo na mesma máquina. Valores medidos com 3 réplicas (Parte A) ou 3 líderes e 3 réplicas (Parte B).

**Isolamento dos nós IPFS (importante).** Na primeira série de testes, os nós estavam na **rede IPFS pública** (bootstrap ativo). Um nó **externo** (`12D3KooWPfvh…`, que não era nenhum dos nossos) estava subscrito a `rep02-in-p0`, `rep02-p0` e `rep02-p1`; muito provavelmente, outro aluno a fazer esta ficha.
- O líder dele **recebeu as leituras do nosso produtor** e publicou-as com a numeração dele: um `REG` com `seq` 328 continha a nossa leitura 1.
- As nossas réplicas **receberam esses `REG` e escreveram-nos**: a réplica 1 ficou com `328;S04;21.8;…` e o líder com `328;S03;22.3;…`.

Os tópicos são **globais** na rede IPFS: qualquer nó pode subscrever ou publicar com o mesmo nome. Para os testes, os dois nós foram **isolados**:
- `ipfs config --json Bootstrap '[]'`;
- `ipfs config --json Discovery.MDNS.Enabled false`;
- e ligados só um ao outro com `ipfs swarm connect`.

Confirmámos que, depois disto, o nó 1 só tem o nó 2 como par. Todos os valores abaixo são dos testes com os nós isolados.

**Recusas do daemon.** Com **vários processos a publicar através do mesmo nó** (produtor e líderes), o Kubo recusou algumas publicações com `pub falhou (500): validation ignored`.
- Teste isolado com dois publicadores simultâneos: no mesmo nó, 1 recusa em 600; em nós diferentes, 0.
- **No produtor:** a leitura perde-se no troço produtor → líder (≈ 0,5–1,5 % com N = 1; ≈ 1–5 % com N = 3).
- **No líder:** o `REG` não sai, mas o registo está no ficheiro, e **todas** as réplicas o recuperam por TCP. É uma perda real, não simulada, e o mecanismo tratou-a sempre.

---

## 2.2 · Diagnóstico inicial

**(a)** Pelos números, **não** pode concluir que o 7 se perdeu: pode estar só atrasado, porque o PubSub não garante a ordem. Esta ficha usa um **limiar**: só quando há **mais de `LIMITE_RETIDOS` = 10 registos retidos** é que a réplica considera os que faltam perdidos e os pede. O custo:
- uma perda só é tratada depois de chegarem 11 registos posteriores, o que dá atraso e memória ocupada;
- a perda dos **últimos** registos de uma série nunca é tratada, porque não chegam registos posteriores suficientes.

**(b)** O líder **não** precisa de saber quem são as réplicas: responde a quem se ligar. A réplica **precisa** de saber onde está o líder, mas não o tem configurado: **aprende-o com cada `REG`**, que transporta `REG;<endereço>;<porto>;…`.

**(c)** **Nenhuma.** Cada partição tem a sua sequência: `seq` = 5 em p0 e `seq` = 3 em p1 são contadores independentes e não se comparam. Os instantes também não servem para ordenar com segurança, porque são relógios de máquinas diferentes. Entre partições não há ordem garantida.

---

## 4.1 · Parte A — Recuperação de registos perdidos

### Antes de avançar (ponto 1): custo do `range`

O `range` lê o ficheiro **inteiro** em cada pedido, por isso o custo de responder cresce **linearmente com o tamanho do histórico**, e não com o tamanho do pedido. Recuperar um único registo num ficheiro com um milhão de linhas lê o milhão de linhas.

Evitava-se com um **índice** `seq → posição no ficheiro`. Como o `seq` começa em 1 e não tem saltos, a linha *n* tem `seq` = *n*; com linhas de tamanho fixo, até se calculava a posição diretamente. Outra hipótese era manter os registos recentes em memória.

### Antes de avançar (ponto 4): estado partilhado no líder

| Estado | Entrada (thread `entrada-p<k>`) | Recuperação (thread principal) | Proteção |
|---|---|---|---|
| `seq` (último atribuído) | lê e escreve | lê (para limitar o *até*) | `synchronized (lock)` |
| `lider-p<k>.txt` | escreve (`append`) | lê (`range`) | métodos `synchronized` do `RecordFile` |
| `host`, `porto`, `ps`, `partitioner` | só leitura | só leitura | não mudam depois do arranque |

**Porque é que o endereço é um argumento:** o `InetAddress.getLocalHost()` devolve **um** dos endereços da máquina, muitas vezes o de *loopback* (`127.0.0.1`) ou uma interface errada (VPN, virtual), e nunca o endereço público atrás de NAT. Só quem configura o sistema sabe que endereço **as réplicas conseguem alcançar**. Na falha 3 vê-se o que acontece quando o endereço anunciado não é alcançável.

### Antes de avançar (ponto 5): o PubSub durante a recuperação

Enquanto a thread de subscrição está na ligação TCP, **não lê** a resposta do `pubsub/sub`. As mensagens publicadas entretanto **não se perdem logo**: ficam à espera, primeiro nos *buffers* TCP da ligação HTTP e do `HttpClient`, depois na fila que o daemon mantém para essa subscrição. Se a espera for longa e a fila encher, o daemon descarta mensagens para esse subscritor lento, porque o PubSub é *best-effort*. Essas perdas aparecem depois como novas lacunas.

Na falha 3 (endereço inalcançável), cada tentativa bloqueia 5 s. Em 40 s, a réplica 1 processou só 27 de 99 registos.

### Ponto 6 — testes (um nó IPFS, três réplicas)

| Teste | Resultado |
|---|---|
| **(a)** 5 leituras | os quatro ficheiros são **iguais** |
| **(b)** réplica 2 parada durante 3 leituras e reiniciada, + 2 leituras | réplica 2 **não convergiu**: 5 linhas contra 10 do líder, com **2 retidos** (9 e 10). Leituras de uma em uma: a recuperação foi desencadeada pela **leitura com `seq` 19**, a 11.ª retida: `RECUPERAÇÃO: 6..8 pedida a 127.0.0.1:7000` → `RECUPERADO: 6, 7, 8` → convergiu |
| **(c)** réplica 3 com `-Ddescartar=6`, só a leitura 6 | réplica 3 com **5 registos** (prefixo do líder, falta o último), sem `EM ESPERA` e sem aviso. Precisou de **11 leituras adicionais** (7 a 17): na 17.ª, `RECUPERAÇÃO: 6..6`, e convergiu |
| **(d)** réplica 3 com `-Dperda=0.2`, 3 × `rajada 1000 10` | ver tabela abaixo |
| **(e)** réplica 3 parada, `replica-3-p0.txt` apagado, `rajada 20 10` (histórico de 2972 registos) | **1 ligação**: `RECUPERAÇÃO: 1..2972`, 2972 registos por TCP; convergência **1703 ms** depois da primeira publicação |
| **(f)** como (d), réplicas 2 e 3 no nó 2 | ver tabela abaixo |

**(d) e (f), por execução:**

| Execução | Ligações (réplica 3) | Registos por TCP (réplica 3) | Última escrita da réplica 3 (ms) | Retidos no fim (réplica 3) | Em falta no fim (réplica 3) | Ligações nas réplicas 1 e 2 (REG recusados no líder) |
|---|---|---|---|---|---|---|
| (d) 1 | 162 | 225 | …225848 | 9 | 15 | 17 |
| (d) 2 | 164 | 197 | …255475 | 6 | 7 | 4 |
| (d) 3 | 155 | 198 | …285703 | 10 | 13 | 10 |
| (f) 1 | 156 | 207 | …579102 | 10 | 15 | 21 |
| (f) 2 | 171 | 224 | …608120 | 9 | 12 | 11 |
| (f) 3 | 171 | 211 | …638773 | 0 | 0 | 6 |

No fim das três execuções de (d), o `PartitionCheck` confirmou que as réplicas 1 e 2 eram **iguais** ao líder e que a réplica 3 era um **prefixo** a que faltavam 13 registos, sem saltos.

### Antes de avançar (depois do ponto 6)

Sim, a réplica 3 terminou quase sempre com registos em falta (15, 7, 13 em (d)), e o número retido no fim é **sempre ≤ `LIMITE_RETIDOS`** (9, 6, 10). A explicação:
- a recuperação só é pedida quando os retidos passam de 10;
- no fim da rajada já não chegam mais mensagens;
- por isso, as perdas entre os últimos registos ficam por recuperar: no máximo 10 retidos, mais os registos perdidos que estão antes deles.

A execução (f) 3 terminou completa porque, por acaso, nenhum dos últimos registos se perdeu.

---

## 4.2 · Parte B — Particionamento

### Antes de avançar (ponto 7): partição de S01 a S20

`hashCode` de `"S01"` = 81300, `"S02"` = 81301, … (consecutivos até `"S09"`), `"S10"` = 81330, …

| Partição | N = 3 | N = 4 |
|---|---|---|
| p0 | S01, S04, S07, S10, S13, S16, S19 (**7**) | S01, S05, S09, S12, S16 (5) |
| p1 | S02, S05, S08, S11, S14, S17, S20 (**7**) | S02, S06, S13, S17, S20 (5) |
| p2 | S03, S06, S09, S12, S15, S18 (**6**) | S03, S07, S10, S14, S18 (5) |
| p3 | — | S04, S08, S11, S15, S19 (5) |

Ao passar de N = 3 para N = 4, **mudam de partição 13 dos 20 sensores** (65 %). Só S01, S02, S03, S16, S17, S18 e S20 ficam na mesma.

### Ponto 9

`PartitionLeader 0 3`, `1 3` e `2 3`: o `netstat -an | findstr 700` mostra os portos **7000, 7001 e 7002** em `LISTENING`.

### Antes de avançar (ponto 9): verificação da partição no líder

Com o produtor correto, nunca falha. Deteta **configuração incorreta**:
- um produtor com N diferente do dos líderes (falha 8: 85 de 200 leituras rejeitadas com `PARTIÇÃO ERRADA`);
- uma função de partição diferente;
- um produtor que publica no tópico errado.

Tem de ser feita **no líder** porque é ele que **escreve** na partição e é a autoridade sobre o que lá entra. O produtor pode estar mal configurado, ser outro programa, ou haver vários produtores; e o tópico é aberto, qualquer processo pode publicar nele. Se a verificação ficasse só no produtor, uma leitura de outra partição ficaria registada na partição errada, e a ordem por sensor deixava de estar garantida.

### Antes de avançar (ponto 10): partições na mesma réplica

A recuperação de uma partição **não bloqueia** a receção de outra: cada partição tem a sua thread de subscrição, e a recuperação corre nessa thread. No teste 12(e), a réplica 1 recuperou **as duas partições ao mesmo tempo**: `RECUPERAÇÃO: 205..464` em p0 e `RECUPERAÇÃO: 221..499` em p1.

As partições **não partilham estado**: cada `Partition` tem `last`, `pending`, endereço do líder, ficheiro e `IpfsPubSub` próprios. A sincronização só é necessária dentro de cada partição, entre a sua thread de subscrição e a thread do resumo (métodos `synchronized` no objeto `Partition`). Partilham apenas a consola, que é segura entre threads, e as opções `-Dperda`/`-Ddescartar`, só de leitura.

### Ponto 12 — testes (N = 3, R = 2, um nó IPFS)

- **(a)** `S01`…`S06`:
  - o produtor mostrou S01 → p0, S02 → p1, S03 → p2, S04 → p0, S05 → p1, S06 → p2;
  - o `PartitionCheck` confirmou que cada registo está na partição esperada e em **exatamente duas réplicas**, todas iguais ao líder: «Réplicas iguais ao líder: 6 de 6», «RESULTADO: OK».
- **(b)** `rajada 3000 20` × 3:
  - registos por partição: 1051/979/819, 983/991/870 e 978/954/916;
  - no fim, todas as 6 cópias **iguais** ao líder;
  - os 203 `REG` recusados nos líderes foram todos recuperados por TCP.
- **(c)** `rajada-quente 3000 20` × 3:
  - p0 com 2551, 2560 e 2540 registos, contra 219/216/235 em p1 e 189/197/200 em p2;
  - todas as cópias iguais ao líder no fim.
- **(d)** líder p1 parado durante ≈ 4 s a meio de `rajada 3000 20` e arrancado de novo:
  - **produtor:** não deu por nada; publicou 1013 leituras para p1, todas com resposta 200;
  - **p0 e p2:** não foram afetadas; registaram tudo o que lhes foi enviado (1012 e 876);
  - **p1:** registou só **593** de 1013. As **420 leituras** publicadas em `rep02-in-p1` enquanto o líder estava parado, ou ainda a arrancar, perderam-se, e **ninguém o detetou**: o `IN` não tem número de sequência;
  - **o líder reiniciado:** retomou a numeração em 202 (`lastSeq()`). As réplicas de p1 ficaram coerentes com o ficheiro do líder (prefixo; faltavam os últimos 11).
- **(e)** réplica 3 com `-Dperda=0.2`, réplica 1 parada ≈ 3 s a meio de `rajada 3000 20`:

  | Réplica | Partição | Ligações | Registos por TCP | Resultado |
  |---|---|---|---|---|
  | 1 | p0 | 23 | 283 (260 numa só, depois do reinício) | prefixo, faltam 4 |
  | 1 | p1 | 36 | 316 (279 numa só, depois do reinício) | **igual** |
  | 2 | p1 | 42 | 44 | **igual** |
  | 2 | p2 | 23 | 23 | prefixo, faltam 3 |
  | 3 | p0 | 173 | 235 | prefixo, faltam 11 |
  | 3 | p2 | 147 | 188 | prefixo, faltam 3 |

  Todos os ficheiros ficaram **sem saltos** (`sequência: OK`). O que falta são sempre os últimos registos da rajada: menos de 11 retidos no fim.
- **(f)** N = 1, `rajada 3000 20` × 3: débito do líder de 175/203/213 registos/s, contra um débito **agregado** de 189/229/229 com N = 3 (ver ponto 16).

### Antes de avançar (ponto 12)

Os três líderes partilham:
- a **CPU** e o **disco** da mesma máquina;
- o **mesmo daemon IPFS**: uma única API HTTP, um único router de PubSub e o mesmo processo a assinar mensagens;
- e, sobretudo, **um único produtor**, que publica sequencialmente por HTTP a ≈ 200 leituras/s.

O débito agregado com N = 3 (≈ 216 registos/s) mal passou o de N = 1 (≈ 197), porque o estrangulamento é o **produtor**, não os líderes: cada líder só esteve ocupado ≈ 1/3 do tempo. O ganho observado **não é representativo**: com um líder por máquina, cada um com o seu daemon, e vários produtores, cada partição escreveria em paralelo e o débito agregado cresceria até ≈ N vezes o de um líder.

---

## 4.3 · Verificação

### Ponto 14 — Falhas introduzidas

| Falha introduzida | Lado onde surgiu | Exceção obtida (ou «nenhuma») | Os ficheiros divergiram? | Recuperou? De que forma? | O que permitiu concluir |
|---|---|---|---|---|---|
| Réplica parada durante `rajada 1000` e arrancada de novo | Réplica | Nenhuma | Durante a paragem, sim. No fim, **não**: as 3 réplicas iguais ao líder (982 registos) | **Sim**: ao 11.º retido, **uma** ligação `RECUPERAÇÃO: 119..627` → 509 registos por TCP | É o caso para que o mecanismo foi feito: a lacuna é seguida de muitos registos, e um só pedido recupera tudo |
| Réplica com `-Ddescartar` igual ao `seq` da última leitura de uma série | Réplica | Nenhuma, e nenhuma mensagem | Sim: a réplica fica com menos 1 registo (prefixo) | **Só depois de mais 11 leituras** (teste 6(c)); se a série acabar ali, **nunca** | A perda do último registo é invisível: não há lacuna sem um registo posterior, e o limiar exige 11 |
| Líder arrancado com um endereço anunciado que não é o seu (`10.255.255.1`) | Réplica | `SocketTimeoutException: Connect timed out`, apanhada → `RECUPERAÇÃO FALHOU` | Sim: réplica 1 com 27 de 99, réplica 3 (perda 0,2) com 4 de 99 | **Não**: tenta de novo em cada mensagem, e cada tentativa bloqueia 5 s | A recuperação depende de o endereço anunciado ser alcançável (acoplamento no espaço). Cada tentativa falhada **atrasa toda a partição**: até a réplica sem perdas simuladas ficou para trás |
| Porto de recuperação ocupado por outro processo quando o líder arranca | Líder (no arranque) e réplica | Líder: `BindException: Address already in use` (apanhada: «Servidor de recuperação indisponível»). Réplica: `SocketTimeoutException: Read timed out` | Sim: réplica 1 com 29 de 98, réplica 3 com 1 de 98 | **Não**. A thread de entrada **continua**: o líder regista e publica, mas sem servidor. As réplicas ligam-se ao **outro** processo, que aceita e não responde | Uma falha no arranque de uma das atividades não é visível do lado da outra. O líder parece funcionar, mas ninguém consegue recuperar, e as réplicas falam com um impostor |
| Líder alterado para fechar a ligação a meio da resposta, sem `FIM` | Réplica | `EOFException: ligação fechada antes de FIM` → `RECUPERAÇÃO FALHOU` | Sim, temporariamente (no fim: 290 de 313) | **Parcialmente, e progressivamente**: os registos recebidos antes do corte ficam escritos; a nova tentativa pede só o que falta: 46..298 → 126 recebidos; 172..298 → 63; 235..298 → 32; 267..298 → 16… | Aplicar cada registo à medida que chega (e não só no `FIM`) garante progresso mesmo com ligações interrompidas. O `FIM` é o que permite distinguir uma resposta completa de uma interrompida |
| `lider-p0.txt` apagado com o líder parado, réplicas intactas, líder arrancado de novo | Réplicas (em silêncio) | **Nenhuma** | **Sim**: o líder recomeça em `seq` 1 com dados novos; as réplicas têm os antigos 1..5 | **Não**: as réplicas tratam os novos 1, 2, 3 como `DUPLICADO` e ignoram-nos; nunca há lacuna, por isso nunca há pedido | O ficheiro do líder é a **fonte de verdade** da numeração e da recuperação; se se perder, o `seq` repete-se e a divergência é **silenciosa**. Só o `PartitionCheck` a detetou («DIVERGENTE: difere do líder na linha 1») |
| Líder da partição 1 parado durante uma rajada | Ninguém dá por nada (produtor: nenhuma) | Nenhuma | As réplicas de p1 ficam coerentes com o líder, mas **o líder perdeu 420 de 1013 leituras** | **Não**: não há numeração no troço produtor → líder | O particionamento **limita** o alcance da falha (p0 e p2 continuaram), mas não a resolve. A perda no troço produtor → líder é invisível |
| Produtor executado com N = 4 e líderes com N = 3 | Líderes | Nenhuma; `PARTIÇÃO ERRADA: S05 pertence a p1`… (85 rejeições) | Os ficheiros estão coerentes, mas faltam leituras: só 63 de 200 registadas | **Não**: 85 leituras rejeitadas e 50 publicadas em `rep02-in-p3`, onde não há líder (perdidas em silêncio) | A verificação no líder **detetou** a configuração errada e impediu que leituras ficassem na partição errada. As de p3 nem isso: nenhum líder as recebe |
| Duas instâncias de `PartitionReplica 2 1,2` na mesma pasta | Réplica | Nenhuma | **Sim**: cada registo aparece **duas vezes** em `replica-2-p1.txt` e `replica-2-p2.txt` | **Não**: cada instância acha que está tudo bem; ao reiniciar, `lastSeq()` lê só a última linha e não vê as repetições | Duas instâncias com o mesmo identificador escrevem no mesmo ficheiro sem coordenação. Só o `PartitionCheck` deteta («sequência: ERRO, repetição ou recuo») |

### Ponto 15 — Construções usadas

| Construção | Onde a usou | O que ficou a ser garantido | O que continua a não ser garantido |
|---|---|---|---|
| Endereço e porto do líder em cada `REG` | `PartitionLeader.onIn`: `Wire.reg(host, porto, r)`; `PartitionReplica.onMessage` atualiza `leaderHost`/`leaderPort` a cada `REG` | A réplica sabe sempre onde recuperar, mesmo se o líder mudar de máquina; o líder não precisa de conhecer as réplicas | Que o endereço seja alcançável (falha 3); que seja do líder verdadeiro (qualquer processo pode publicar um `REG` com outro endereço) |
| `ServerSocket` / `accept()` no líder | `PartitionLeader.main`, `new ServerSocket(7000 + k)` | Um ponto fixo, por partição, onde as réplicas pedem o que lhes falta | Atender vários pedidos ao mesmo tempo (é um de cada vez); que o porto esteja livre (falha 4) |
| `LIMITE_RETIDOS` | `PartitionReplica.onMessage`: `pending.size() > Wire.LIMITE_RETIDOS` → `recover()` | Não se pede por uma simples troca de ordem; o número de retidos fica limitado (≈ 10) enquanto a recuperação funciona | A recuperação dos últimos registos de uma série; um limite de memória quando a recuperação falha (os retidos crescem) |
| Protocolo `PEDIDO` / registos / `FIM` | `PartitionLeader.serve` e `PartitionReplica.recover` | Respostas completas distinguíveis de interrompidas (falha 5); pedidos inválidos recebem `ERRO` | Autenticação; integridade além do TCP; que o líder ainda tenha o registo (falha 6) |
| `connect` e `setSoTimeout` com `T_TCP_MS` | `PartitionReplica.recover` (e `setSoTimeout` no líder) | Nenhuma tentativa bloqueia mais do que 5 s por passo; a falha é detetada e repetida | Que não haja atraso: cada tentativa falhada custa até 5 s à partição inteira |
| `RecordFile.range` sobre o ficheiro do líder | `PartitionLeader.serve` | Tudo o que o líder propagou pode ser reenviado (o ficheiro é a fonte) | Custo proporcional ao histórico (lê o ficheiro inteiro); disponibilidade se o ficheiro se perder |
| Função de partição `floorMod(hashCode, N)` | `Partitioner.of`, usado no `Producer` e no `PartitionLeader` | Partição determinística e igual em qualquer JVM; nunca negativa | Equilíbrio de carga com chaves quentes (ponto 12(c)); estabilidade quando N muda (65 % dos sensores mudam) |
| Sequência independente por partição | `seq` de cada `PartitionLeader`; `last`/`pending` de cada `Partition` | Ordem total **dentro** de cada partição (e por sensor), sem coordenação entre líderes | Qualquer ordem **entre** partições |
| Atribuição de partições às réplicas (R = 2) | Argumentos `PartitionReplica 1 0,1`, `2 1,2`, `3 2,0` | Cada partição está em exatamente duas réplicas; a falha de uma réplica não torna nenhuma partição indisponível para leitura | A disponibilidade para **escrita**, que depende do líder único da partição; a atribuição é fixa (manual) |
| Secção sincronizada no líder (`seq` + `append`) | `PartitionLeader.onIn`, `synchronized (lock)` | O `seq` atribuído e a linha no ficheiro são consistentes; a recuperação nunca vê um `seq` sem a linha correspondente | Nada entre processos: duas instâncias do mesmo líder dariam números repetidos |

---

## 4.4 · Comparação e reflexão crítica

### Ponto 16 — Valores medidos (média de 3 execuções; nós IPFS isolados da rede pública)

**Recuperação** (`rajada 1000 10`, uma partição, **um nó IPFS**; a última coluna repete-se com dois nós):

| Métrica | COMINDIR01 (PubSub, um nó) | COMINDIR02, sem perdas simuladas | COMINDIR02, réplica 3 com `-Dperda=0.2` | idem, **dois nós** (6(f)) |
|---|---|---|---|---|
| Tempo de envio da rajada no produtor (ms) | 5512 (no líder) | 5704 | 5523 | 5488 |
| Tempo até à convergência (ms) | 5512 | 5904 | 5791 (\*) | 5968 (\*) |
| Débito no líder (registos/s) | 182 | 172 | 175 | 170 |
| Ligações TCP de recuperação | 0 | 12 por réplica (\*\*) | réplica 3: 160; réplicas 1 e 2: 10 | réplica 3: 166; réplicas 1 e 2: 13 |
| Registos obtidos por TCP | 0 | 12 por réplica (\*\*) | réplica 3: 207; réplicas 1 e 2: 10 | réplica 3: 214; réplicas 1 e 2: 13 |
| Registos em falta no fim, por réplica | 0 / 0 / 0 | 0 / 0 / 0 | 0 / 0 / 11,7 | 0 / 0 / 9 |
| Registos retidos em memória no fim | 0 | 0 | réplica 3: 8,3 | réplica 3: 6,3 |
| Leituras recusadas no produtor (perdidas antes do líder) | — | 9,3 em 1000 | 4 em 1000 | 11,3 em 1000 |

(\*) Última escrita da réplica mais lenta; a réplica 3 nem sempre escreveu o último registo da rajada (ver «em falta»).
(\*\*) Ligações causadas pelos `REG` recusados no líder pelo daemon (perdas reais), todas recuperadas.

**Particionamento** (`rajada 3000 20` e `rajada-quente 3000 20`, um nó IPFS):

| Métrica | N = 1 | N = 3, uniforme | N = 3, chave quente |
|---|---|---|---|
| Registos por partição (p0 / p1 / p2) | 2977 / — / — | 1004 / 975 / 868 | 2550 / 223 / 195 |
| Desequilíbrio (máximo / média) | 1,00 | 1,06 | **2,58** |
| Débito agregado de escrita dos líderes (registos/s) | 197 | 216 | 227 |
| Tempo até à convergência (ms) | 15243 | 13318 | 13204 |
| Registos guardados por réplica (do total) | todos (8930 de 8930) | ≈ 2/3: 5936 / 5529 / 5617 de 8541 | 8321 / 1256 / 8237 de 8907 |
| Partições indisponíveis com o líder da p1 parado | 1 de 1 (todo o sistema) | 1 de 3 (p1, só para escrita) | 1 de 3 (p1) |

### Ponto 17 — Questões

**Custo da recuperação.** Com 20 % de perdas na réplica 3, a recuperação custou, por rajada de 1000 registos, ≈ 160 ligações TCP e ≈ 207 registos transferidos, cerca de 1,3 registos por ligação.
- **Porquê tantas ligações:** cada pedido cobre só a **primeira** lacuna (de último + 1 até ao menor retido − 1), e com 20 % de perdas há uma lacuna a cada ≈ 5 registos.
- **Comparação com a propagação:** cada `REG` propagado é uma publicação PubSub (uma por registo, para todas as réplicas). Cada ligação de recuperação é uma ligação TCP **por réplica e por lacuna**: abrir a ligação, pedido, resposta, `FIM` e fechar.
- **Quem paga:** o **líder**, que abre e lê o ficheiro inteiro em cada pedido e atende um de cada vez, e a **réplica que pede**, cuja partição fica parada durante o pedido. As outras réplicas **não** pagam nada, ao contrário de uma retransmissão pelo PubSub, que chegaria a todas.

**Critério de decisão.**
- **`LIMITE_RETIDOS` = 1:** pedia ao líder em quase todas as trocas de ordem, que se resolvem sozinhas seguidas de um `DUPLICADO`; mais ligações e mais carga no líder.
- **1000:** uma perda só era tratada depois de 1001 registos; atraso enorme, até 1000 registos em memória, e perdas no fim de séries até 1000 registos sem recuperação.
- **Teste 6(c):** a perda do **último** registo não cria lacuna, porque não chega nenhum registo depois dele. A réplica não sabe que lhe falta nada. Para o recuperar, o líder teria de enviar periodicamente o **último `seq` atribuído** (um anúncio periódico, *heartbeat*), para a réplica comparar com o seu `last`. Os anúncios periódicos estão **excluídos** desta ficha: **a solução desta ficha não resolve esse problema.**

**Concentração de pedidos.**
- **Quanto custa:** 300 réplicas que perdem o mesmo registo abrem **300 ligações** ao líder, quase ao mesmo tempo. Com atendimento sequencial, a última espera por todas as anteriores: se cada uma demorar *t* (≈ alguns ms, dominado pela leitura do ficheiro), a última é atendida ≈ 300 *t* depois. Com 1 milhão de registos no ficheiro, *t* passaria a centenas de ms, e o total a minutos.
- **Dentro do âmbito:** dava para atender várias ligações em simultâneo com **uma thread por ligação**, como na TCP01 (`Connection extends Thread`). O `range` é `synchronized`, mas é só leitura e podia deixar de o ser.
- **Mesmo assim**, a carga do líder **continua a crescer com o número de réplicas**: é um pedido por réplica e por lacuna. Só diminuiria se as réplicas recuperassem umas das outras, ou se a retransmissão fosse única, para todas.

**Acoplamento.**
- **Que endereço anunciar:** o líder tem de anunciar um endereço **alcançável pelas réplicas**, que não é necessariamente o seu. Atrás de NAT, é o endereço público do router, com um **reencaminhamento de porto** para o líder. Atrás de uma *firewall*, o porto 7000 + k tem de estar aberto.
- **Se nenhum endereço for alcançável:** a propagação continua a funcionar pelo PubSub, mas a recuperação falha sempre (falha 3). As réplicas que perderem registos ficam retidas como na COMINDIR01, e pior: **cada tentativa bloqueia a partição 5 s**.

**Histórico.**
- **Medido (6(e)):** a réplica nova recebeu 2972 registos numa ligação e convergiu **1,7 s** depois da primeira publicação do produtor, incluindo escrever cada registo no ficheiro e na consola. São ≈ 0,6 ms por registo; um milhão de registos daria ≈ 600 s, cerca de 10 minutos.
- **Durante esse tempo**, o líder **não atende mais ninguém** (um de cada vez), e as outras réplicas que precisem de recuperar ficam à espera e podem esgotar o *timeout* de 5 s; a da réplica nova bloqueia também a sua própria partição.
- **O que o evitaria**, fora do âmbito: *snapshots* (enviar o estado compactado em vez do histórico) ou compactação do histórico.

**Distribuição de carga.**
- **Com a chave quente**, p0 ficou com ≈ 86 % dos registos (2550 contra 223 e 195; desequilíbrio 2,58). O líder de p0 trabalhou ≈ 200 registos/s, os outros ≈ 17. As réplicas 1 e 3, que guardam p0, ficaram com ≈ 6,6× mais dados do que a réplica 2.
- **Alternativa:** uma chave composta, por exemplo `sensor + "#" + (contador % M)` ou `sensor + minuto`, espalharia as leituras de `S01` por várias partições.
- **O que se perde:** a **ordem por sensor**. As leituras de `S01` passariam a ter números de sequência de partições diferentes, sem ordem entre si.

**Alteração do número de partições.**
- **Fração que muda:** de N = 3 para N = 4, mudam **13 dos 20 sensores (65 %)**; em geral, com `floorMod(hash, N)`, muda cerca de 1 − 1/N da chave.
- **Ficheiros já escritos:** teriam de ser **redistribuídos**. Os registos de cada sensor que mudou iriam para outra partição, com nova numeração, e o histórico de cada partição deixaria de ser coerente com os números já atribuídos.
- **Família de funções que reduz a fração:** o *hashing* consistente, em que só ≈ 1/N das chaves muda. Fica fora do âmbito, tal como o reparticionamento dinâmico.

**Disponibilidade.**
- **COMINDIR01:** a falha do líder **parava todas as escritas**.
- **Aqui:** pára só as da **partição** desse líder (1 de 3). As leituras dessa partição perdem-se em silêncio (12(d): 420), e as réplicas dessa partição **também deixam de conseguir recuperar**, porque o servidor de recuperação é o próprio líder (`Connect refused`/*timeout*).
- **Resolve ou limita?** O particionamento **limita o alcance** da falha, não a resolve.
- **O que faltaria** para outra máquina assumir a partição: detetar a falha, eleger um novo líder entre as réplicas dessa partição, garantir que tem o histórico completo, e continuar a numeração sem conflitos. São eleição e consenso, fora do âmbito.

**Troço produtor → líder.**
- **No 12(d)**, perderam-se as leituras de p1 publicadas enquanto o líder estava parado (420 de 1013), e **ninguém deu conta**: o produtor recebeu `200` em todas as publicações, e o líder não sabe o que não recebeu.
- **Para aplicar a mesma recuperação** a esse troço:
  - o produtor teria de **numerar** as leituras (uma sequência por produtor e por partição) e **guardar** o que publicou, num ficheiro, como o líder guarda os registos;
  - teria de anunciar o seu endereço e ter um servidor de recuperação;
  - e o líder teria de manter um estado por produtor (último número recebido, retidos) e pedir as lacunas.
- O produtor passaria a ter o papel que o líder tem hoje para as réplicas.

**Segurança.**
- **Qualquer processo pode publicar** em `rep02-p0` um `REG` com o endereço que quiser. As réplicas passam a **recuperar desse endereço** e escrevem o que ele enviar: registos falsos, mas bem formados, com os números de sequência certos. Isto **aconteceu de facto** nos nossos testes: um nó externo da rede IPFS pública publicou `REG` em `rep02-p0`, e a réplica 1 escreveu um registo que não era do nosso líder.
- **Um processo a abrir ligações continuamente ao porto 7000** ocupa o líder, que atende uma de cada vez, até 5 s cada. As réplicas verdadeiras deixam de conseguir recuperar: é uma negação de serviço.
- **Onde verificar:** na **réplica**, antes de aplicar um `REG` e antes de usar o endereço anunciado.
- **O campo `from` ajudaria:** identifica o nó IPFS que publicou, e as mensagens são **assinadas** por omissão. A réplica poderia aceitar só `REG` cujo `from` seja o *peer ID* do nó do líder; mas a nossa `IpfsPubSub` só extrai o campo `data`.
- **Na recuperação**, o TCP não autentica nada: seria preciso que o líder provasse a sua identidade. Isso está para além desta ficha.

---

## Critérios de aceitação

### CA1 · Mecanismo de recuperação

- **Como decide que se perdeu:** o registo esperado (`last + 1`) não chegou e já há **mais de 10 retidos** depois dele (`pending.size() > Wire.LIMITE_RETIDOS` em `PartitionReplica.onMessage`).
- **Como sabe onde está o líder:** pelo `REG` mais recente: `leaderHost = host; leaderPort = port;`, atualizados em **cada** mensagem.
- **Onde está no código:**
  - a réplica abre a ligação em `recover()`: `s.connect(new InetSocketAddress(leaderHost, leaderPort), Wire.T_TCP_MS)`;
  - o líder aceita em `main`: `server.accept()`;
  - e responde em `serve(...)`: `file.range(from, upTo)`, uma linha por registo, e depois `FIM`.
- **O ficheiro do líder é a fonte:** é a única cópia completa e ordenada do que foi propagado. Como o líder escreve **antes** de publicar, tudo o que uma réplica possa ter perdido está lá.

Perguntas de verificação:
- **Perdeu-se o registo 10. Quantos têm de chegar depois?** **11** (o 11.º retido desencadeia o pedido; teste 6(b): 9 a 19). **Se for o último**, nunca é recuperado (teste 6(c)).
- **Porquê o endereço em todas as mensagens?** Porque uma réplica pode arrancar a qualquer momento e tem de aprender o endereço com a primeira mensagem que receber. E porque, se o líder for reiniciado noutra máquina, as réplicas passam a pedir ao endereço novo logo no `REG` seguinte, sem qualquer configuração.
- **Onde fica bloqueada a réplica durante a recuperação?** No `connect` e nos `readLine` de `recover()` (até 5 s cada), **na thread de subscrição dessa partição**. Entretanto, as mensagens PubSub dessa partição ficam à espera nos *buffers* da ligação HTTP e do daemon. As outras partições continuam (12(e)).
- **Porque é que «escrever, depois propagar» passou a ser necessário?** Porque a recuperação lê o ficheiro do líder. Se o líder propagasse antes de escrever e falhasse entre as duas coisas, as réplicas teriam um registo que o líder **não pode reenviar**. Ao reiniciar, o líder repetiria o número, como na falha 6.

### CA2 · Convergência das réplicas

- **Réplica parada durante uma rajada (falha 1):** convergiu com **uma** ligação (509 registos).
- **Réplica nova (6(e)):** recuperou **todo o histórico** (2972 registos) numa ligação, e o `PartitionCheck` deu «IGUAL ao líder».
- **Parada durante 500 registos:** abre **1 ligação** e recebe ≈ 500 registos por ela, porque o pedido cobre tudo de `last + 1` até ao menor retido − 1. Mais ligações só se houver outras perdas.
- **Um registo pode chegar pelas duas vias?** Sim: um `REG` atrasado pode chegar depois de o mesmo registo ter vindo por TCP. **Não fica repetido** porque a regra é a mesma nas duas vias: `seq <= last` (ou já retido) → `DUPLICADO`, ignorado. Nos testes com os nós isolados, isto não chegou a acontecer: os registos recuperados eram sempre os que o PubSub tinha perdido de vez.
- **Máximo de retidos:** em funcionamento normal, **≤ 11** por partição (o 11.º desencadeia a recuperação, que os despeja). O limite deixa de se verificar quando **a recuperação falha** (falhas 3, 4, 5): os retidos continuam a crescer.
- **Porque não 0, e 1000?** Com 0, pedia-se ao líder a cada troca de ordem (desnecessário e caro). Com 1000, as perdas demoravam 1000 registos a ser tratadas, a memória crescia até 1000 por partição, e as perdas no fim de uma série ficavam quase sempre por recuperar.

### CA3 · Particionamento

- **Função de partição:** `Math.floorMod(sensorId.hashCode(), N)`. Como cada sensor vai sempre para a mesma partição, cada registo fica em **exatamente uma**. Como cada partição está em duas réplicas, fica em **exatamente R = 2 réplicas** (12(a), `PartitionCheck`).
- **Porquê `floorMod` e não `%`?** O `%` em Java devolve **negativo** quando o `hashCode` é negativo: `"zzzzzz".hashCode()` = −685785664, `% 3` = **−1**, que não é partição nenhuma; `floorMod` = **2**.
- **O que garante o mesmo resultado em processos diferentes?** O algoritmo de `String.hashCode()` está **fixado pela especificação da linguagem**, e o N tem de ser igual nos dois. Se não for, o líder rejeita com `PARTIÇÃO ERRADA` (falha 8).
- **Uma sequência global partilhada** exigiria que os líderes se coordenassem para atribuir cada número: um contador central (ponto único de falha e de estrangulamento) ou consenso entre líderes. Está fora do âmbito, e acabaria com a vantagem de os líderes escreverem em paralelo.
- **Ordem garantida:** entre duas leituras do **mesmo sensor**, a ordem do líder da sua partição. Entre leituras de **partições diferentes**, nenhuma.

### CA4 · Concorrência e falhas

| Processo | Threads | Estado partilhado | Proteção |
|---|---|---|---|
| Líder | `entrada-p<k>` (bloqueia no `forEach` do `subscribe`); principal (bloqueia no `accept()` e no `readLine()`); `resumo-p<k>` | `seq`, ficheiro, estatística do lote | `synchronized (lock)`; `RecordFile` com métodos `synchronized` |
| Réplica | uma `sub-p<k>` por partição (bloqueia no `forEach` do `subscribe`, e no `connect`/`readLine` durante a recuperação); `resumo` | estado de cada `Partition`, partilhado só entre a sua thread e o resumo | métodos `synchronized` de `Partition` |
| Produtor | só a principal (bloqueia no `hasNextLine()` e em cada `publish`) | — | — |

- **Sem a mesma secção sincronizada para `seq` e `append`:** a recuperação podia ler um `seq` cujo registo ainda não estivesse no ficheiro, e o `range` devolvia menos registos do que o pedido. Com duas threads de entrada, havia números repetidos ou registos fora de ordem no ficheiro.
- **Se o servidor lesse a meio de um `append`:** podia ler uma linha incompleta, e o `fromLine` lançava `IllegalArgumentException`. Não acontece porque `append` e `range` são `synchronized` no mesmo `RecordFile`.
- **Líder da p1 parado:**
  - as leituras de p1 perdem-se em silêncio (420 no 12(d));
  - p0 e p2 não são afetadas;
  - as réplicas de p1 não recebem nada e, se precisarem de recuperar, falham (ninguém no porto 7001) até o líder voltar.
- **Falhas não recuperadas:** 3 (endereço errado), 4 (porto ocupado), 6 (ficheiro do líder apagado), 7 (leituras perdidas antes do líder), 8 (N diferente) e 9 (instâncias duplicadas), além da última de uma série (2). A 6 é a mais grave, porque é **silenciosa**: o mecanismo assume que o ficheiro do líder é a verdade.

### CA5 · Comparação e limites

- **Custo de recuperar uma réplica parada durante `rajada 1000`:** **1 ligação TCP, 509 registos, convergência no fim da rajada** (falha 1). Com 20 % de perdas: ≈ 160 ligações e ≈ 207 registos por rajada de 1000.
- **Comparação com a COMINDIR01:** o débito e o tempo de convergência ficaram praticamente iguais (≈ 175 contra 182 registos/s; ≈ 5,8 s contra 5,5 s), e as réplicas que perdiam registos passaram a convergir, em vez de ficarem retidas para sempre.
- **Chave quente:** p0 ficou com 86 % da carga (desequilíbrio 2,58). O particionamento por chave **não resolve** este caso, porque todas as leituras de `S01` vão para a mesma partição.
- **N de 3 para 4:** mudam **65 %** dos sensores. Os registos já escritos ficariam na partição antiga e teriam de ser redistribuídos e renumerados.
- **Leitura perdida entre produtor e líder:** ninguém tem condições para a detetar. Faltaria numerar as leituras no produtor e aplicar a esse troço a mesma recuperação.

**Limitações que se mantêm, e o que faria falta:**
1. **Perda dos últimos registos de uma série** → o líder teria de anunciar periodicamente o último `seq` (*heartbeats*, fora do âmbito).
2. **Perdas no troço produtor → líder** → numeração e recuperação nesse troço, com o produtor a guardar o que publicou.
3. **Falha do líder de uma partição** → eleição de um novo líder entre as réplicas, com consenso sobre o histórico (fora do âmbito).
4. **Isolamento e autenticidade dos tópicos** → verificar o `from` assinado das mensagens e autenticar o líder na recuperação. Ficou demonstrado como necessário pela contaminação vinda da rede pública.
