# Sprint E1-COMINDIR03 — Relógios Lógicos e Relógios Vetoriais sobre Partições

## Estrutura

Quatro projetos separados (simulam máquinas distintas), todas as classes no pacote `rep03`, tópicos com o prefixo `rep03`:

| Projeto | Classes |
|---|---|
| `rep03-produtor/src/rep03/`    | `SensorRecord`, `RecordFile`, `IpfsPubSub`, `Wire`, `Partitioner`, `VectorClock`, **`Producer`** |
| `rep03-lider/src/rep03/`       | `SensorRecord`, `RecordFile` (+ `range`, **+ `last`**), `IpfsPubSub`, `Wire`, `Partitioner`, `VectorClock`, **`PartitionLeader`** |
| `rep03-replica/src/rep03/`     | `SensorRecord`, `RecordFile`, `IpfsPubSub`, `Wire`, `Partitioner`, `VectorClock`, **`PartitionReplica`**, `PartitionCheck`, **`CausalCheck`** |
| `rep03-processador/src/rep03/` | `SensorRecord`, `IpfsPubSub`, `Wire`, `Partitioner`, `VectorClock`, **`AlarmProcessor`** |

- `VectorClock` está **exatamente** como no enunciado.
- `IpfsPubSub` e `Partitioner` são os da COMINDIR02, só com o pacote alterado.
- `SensorRecord` ganhou `lamport` e `vector`. A linha passa a ser `seq;sensor;temperatura;instante;L;V`.
- `Wire` ganhou `LIMIAR_ALARME = 28.0`; `in(...)` e `reg(...)` levam *L* e *V*.

### Execução

Em cada projeto: `javac -d out src/rep03/*.java`. Depois, cada programa no seu terminal, **réplicas → líderes → processador → produtor**, todos na **mesma pasta**:

```bash
java -cp out rep03.PartitionReplica 1 0,1        # e 2 1,2 / 3 2,0; opcional -Ddescartar=<seq> ou -Dperda=0.2 antes de -cp
java -cp out rep03.PartitionLeader 0 3           # e 1 3, 2 3   [endereço] [api]
java -cp out rep03.AlarmProcessor 3              # [api]
java -cp out rep03.Producer 3                    # [api]
java -cp out rep03.CausalCheck 3 <pasta>         # escreve também <pasta>/vista-lamport.txt
java -cp out rep03.PartitionCheck 3 <pasta>      # (da COMINDIR02) convergência das réplicas
```

### Decisões de desenho

- **Líder** (`PartitionLeader.onIn`):
  - valida o `IN`, que agora tem quatro campos: sensor, temperatura, *L*<sub>msg</sub> e *V*<sub>msg</sub>. Um vetor com dimensão diferente de N é **rejeitado** (`REJEITADO … Vetor com dimensão 4, esperado 3`);
  - **numa única secção `synchronized (lock)`**:
    1. `newL = max(L, Lmsg) + 1`;
    2. `newV = V.copy(); newV.merge(Vmsg); newV.tick(k)`;
    3. `newSeq = seq + 1` e verificação `newV.get(k) == newSeq`;
    4. `append` do registo;
    5. só depois `seq`, `L` e `V` avançam e se escreve `EVENTO: seq=… L=… V=…`;
  - os relógios são calculados sobre **cópias** e só passam a ser o estado do líder depois de o `append` correr bem. É a mesma regra da COMINDIR02 para o `seq`: um registo que falhou a escrita não deixa rasto nos relógios;
  - **se `V[k] ≠ seq`**, o evento é **recusado** (`RECUSADO: V[k] = … diferente de seq = …`) e nada é escrito. É um invariante: escrever um vetor errado corrompia a causalidade de todos os registos seguintes;
  - **no arranque**, *L* e *V* vêm da **última linha** de `lider-p<k>.txt` (`RecordFile.last()`). O líder confirma que o vetor tem dimensão N e que `V[k] = seq`; se não, não arranca;
  - o `REG` é publicado **fora** da secção sincronizada, com os carimbos do registo. O `SensorRecord` guarda uma **cópia** do vetor.
- **Produtor:** envia sempre `L = 0` e `V = new VectorClock(N)` (`IN;S02;29.3;0;0,0,0`).
- **Processador** (`AlarmProcessor`):
  - uma thread `sub-p<k>` por partição, todas a partilhar *L*<sub>p</sub>, *V*<sub>p</sub> e os `seq` já vistos, protegidos por `lock`;
  - por cada `REG`: ignora alarmes (`ALARM-…`) e duplicados (mesma partição e mesmo `seq`). Depois `Lp = max(Lp, L)` e `Vp.merge(V)`;
  - se a temperatura for ≥ 28,0: tira `l = Lp` e `v = Vp.copy()` **dentro** do `lock` e publica, **fora** do `lock`, `IN;ALARM-<sensor>;<temp>;<l>;<v>` em `topicIn(of("ALARM-" + sensor))`. Escreve `ALARME: S02 p1#1 → p2 (L=1 V=0,1,0)`;
  - **os registos `ALARM-` são ignorados por inteiro**, incluindo para o máximo. É a leitura literal de «ignora registos de sensores começados por `ALARM-`». A consequência é que um alarme não fica causalmente dependente de outros alarmes que o processador tenha visto, a não ser pelo caminho do líder (o líder de p<sub>j</sub> funde *V*<sub>msg</sub> com o seu próprio vetor, que já inclui os alarmes anteriores de p<sub>j</sub>).
- **Réplica:** igual à COMINDIR02. **Não consulta nem atualiza** relógios. Só uma mudança, para o ensaio 7(e): `Aplicado` mostra também o instante de escrita (`(escrito <ms>)`).
  - `SensorRecord.fromLine(linha)` aceita a dimensão que vier na linha, porque a réplica não conhece N;
  - `SensorRecord.fromLine(linha, N)` exige dimensão N, e é a que usam o processador e o `CausalCheck`.
- **`CausalCheck <N> <pasta>`:**
  - lê os `lider-p*.txt` e verifica `V[k] = seq` em cada linha;
  - classifica **todos os pares de partições diferentes** (`a → b`, `b → a`, concorrente);
  - verifica a condição de relógio em **todos os pares `a → b`**, incluindo os da mesma partição;
  - conta os concorrentes com *L* distinto;
  - escreve `vista-lamport.txt`, ordenado por (*L*, *k*), em que cada linha é `p<k>;` mais a linha do registo;
  - com até 30 pares entre partições, mostra-os um a um (ensaio 7(a));
  - calcula também o tamanho médio de um `REG` com e sem `;L;V`, reconstruindo-o a partir de cada linha com o endereço `127.0.0.1` e o porto `7000 + k`.

### Ambiente dos testes

Windows 11, Java 21 (compilado com `--release 11`), Kubo 0.43.1, tudo na mesma máquina, com os dois nós IPFS **isolados da rede pública** como na COMINDIR02 (`Bootstrap []`, MDNS desligado, ligados só um ao outro). Todos os processos usaram o nó 1 (API 5001).

**Recusas do daemon.** Como na COMINDIR02, o Kubo recusou algumas publicações (`pub falhou (500): validation ignored`) quando vários processos publicavam pelo mesmo nó ao mesmo tempo:
- no **produtor**: 12 a 20 leituras perdidas em 300 (perdas no troço produtor → líder);
- no **líder**: 3 a 14 `REG` não publicados; as réplicas recuperaram-nos por TCP;
- no **processador**: 0 a 4 alarmes perdidos (perdas no troço processador → líder).

---

## 2.2 · Diagnóstico inicial

**(a)** Na COMINDIR02, **não**. Os líderes não comunicam, e o produtor só escreve, não observa nada. Dois registos de partições diferentes são **sempre concorrentes**. O elemento novo é o **processador de alarmes**, que **lê** uma partição e **escreve** noutra: o alarme passa a depender causalmente do registo que o originou (e de tudo o que o processador já tinha visto).

**(b)**
- *L*(*x*) = 4 < *L*(*y*) = 7: só se pode concluir que **não** é verdade que *y* → *x*. Se fosse, ter-se-ia *L*(*y*) < *L*(*x*). Pode ser *x* → *y* ou *x* ∥ *y*: o relógio de Lamport não distingue.
- *V*(*x*) = (2,1,0) e *V*(*y*) = (1,3,2): na componente 0, *x* é maior (2 > 1); na componente 1, *y* é maior (1 < 3). Os vetores são **incomparáveis**, logo *x* ∥ *y*: são **concorrentes**.

**(c)** Cada componente conta os **eventos de um processo**, e os únicos processos que geram eventos são os **líderes** (as atribuições de `seq`), um por partição. Réplicas, produtor e processador **não** geram eventos: só aplicam registos ou transportam carimbos. Uma componente por réplica ou por produtor ficaria sempre a zero. Por isso a dimensão é **N**, o número de partições.

---

## 4.1 · Relógios nos líderes e propagação causal

### Antes de avançar (ponto 1): `VectorClock` não é sincronizada

- **Monitor:** o mesmo `lock` que já protegia o `seq` no líder. *L*, *V*, `seq` e o `append` mudam juntos, na mesma secção, e a thread de recuperação lê o `seq` com esse mesmo `lock`.
- **Porque é preciso o `copy()`:** a publicação é feita **fora** do `lock`, e o vetor do líder (ou do processador) continua a ser alterado por eventos seguintes. Se o registo ou a mensagem guardassem uma **referência** para o vetor vivo, o que se publica ou escreve podia já incluir eventos posteriores: um carimbo errado, «do futuro». No líder, o `SensorRecord.now(...)` guarda uma cópia. No processador, a cópia é tirada dentro do `lock`. A falha 4 do ponto 8 mostra exatamente este erro.

### Antes de avançar (ponto 5): o processador acumula **todos** os registos

- **Com esta escolha:** um alarme depende causalmente de **tudo** o que o processador recebeu até ali, de **todas** as partições. Daí resultam muitas relações entre partições: em 7(c), **≈ 88 %** dos pares entre partições ficaram causais, contra **0 %** sem processador (7(b)). Cada alarme «puxa» para trás de si todo o passado observado, e esse efeito propaga-se pelos eventos seguintes da partição de destino.
- **Com a alternativa** (carimbos só do registo que originou o alarme): o alarme dependeria só desse registo e do seu passado. Perdiam-se as relações com os registos **de outras partições** que o processador já tinha visto. Mas o processador **conhecia-os** quando decidiu, por isso podem ter influenciado a decisão (por exemplo, um alarme que só se emite depois de várias leituras altas). A causalidade é **potencial**: o que se viu antes de agir conta, mesmo que não tenha sido usado. A alternativa **subestimava** a causalidade e permitia, por exemplo, que um registo observado antes do alarme aparecesse como concorrente com ele.

---

## 4.2 · Ensaios

### Ponto 7

**(a) Figura 1.** Leituras `S02 29.3`, `S01 21.0`, `S04 22.5`, uma de cada vez, com o processador ativo:

| Evento | Ficheiro | Linha | Figura 1 |
|---|---|---|---|
| *a* | `lider-p1.txt` | `1;S02;29.3;…;1;0,1,0` | seq=1 · L=1 · V=(0,1,0) ✔ |
| *b* | `lider-p0.txt` | `1;S01;21.0;…;1;1,0,0` | seq=1 · L=1 · V=(1,0,0) ✔ |
| *c* | `lider-p2.txt` | `1;ALARM-S02;29.3;…;2;0,1,1` | seq=1 · L=2 · V=(0,1,1) ✔ |
| *d* | `lider-p0.txt` | `2;S04;22.5;…;2;2,0,0` | seq=2 · L=2 · V=(2,0,0) ✔ |

O processador escreveu `ALARME: S02 p1#1 → p2 (L=1 V=0,1,0)`. O `CausalCheck` deu:

```
p0#1 ∥ p1#1   V 1,0,0 / 0,1,0   L 1 / 1
p0#2 ∥ p1#1   V 2,0,0 / 0,1,0   L 2 / 1
p0#1 ∥ p2#1   V 1,0,0 / 0,1,1   L 1 / 2      (b ∥ c, mas L(b) < L(c))
p0#2 ∥ p2#1   V 2,0,0 / 0,1,1   L 2 / 2      (d ∥ c, empate em L)
p1#1 → p2#1   V 0,1,0 / 0,1,1   L 1 / 2      (a → c)
```

`vista-lamport.txt`: *b* (L1, p0), *a* (L1, p1), *d* (L2, p0), *c* (L2, p2). Fica *d* antes de *c*, como na tabela da ficha: o empate é decidido pelo número da partição.

**(b) Processador parado, `rajada 300 10`:** 287 registos (13 leituras recusadas pelo daemon no produtor); 27 048 pares entre partições.
- **Pares causalmente relacionados entre partições: 0.** Todos os 27 048 são concorrentes.
- 26 801 (99,1 %) têm *L* distinto. Sem alarmes, *L* = `seq` em cada partição, por isso só empatam os pares com o mesmo `seq`: 74 + 74 + 99 = 247 pares.
- Condição de relógio: 13 993 pares `a → b` (todos dentro de cada partição), 0 violações.

**(c) Processador ativo, `rajada 300 10` × 3** (sistema limpo em cada execução):

| Execução | Registos (p0 / p1 / p2) | Alarmes | Pares causais | Pares concorrentes | Concorrentes com *L* distinto | Violações |
|---|---|---|---|---|---|---|
| 1 | 124 / 95 / 104 | 36 | 30 095 (87,1 %) | 4 461 | 4 206 (94,3 %) | 0 |
| 2 | 118 / 95 / 104 | 37 | 29 465 (88,3 %) | 3 897 | 3 659 (93,9 %) | 0 |
| 3 | 125 / 91 / 115 | 43 | 32 290 (89,2 %) | 3 925 | 3 676 (93,7 %) | 0 |

Em todas: `V[k] = seq` OK nas três partições. O `PartitionCheck` confirmou que as réplicas são iguais ao líder ou um **prefixo** sem saltos. Os prefixos são a limitação já conhecida da COMINDIR02: os últimos `REG` recusados no fim da rajada só são recuperados quando chegarem mais de 10 registos.

**(d) Reinício do líder de p2.** Antes de parar: `3;S06;21.4;…;3;0,1,3`. Ao arrancar de novo:

```
Líder da partição 2 (N = 3) | … | último seq 3 | L = 3 | V = 0,1,3
EVENTO: seq=4 L=4 V=0,1,4          (S03 22.0, do produtor)
EVENTO: seq=5 L=6 V=1,3,5          (ALARM-S05, do processador: L=5 V=1,3,4)
EVENTO: seq=6 L=7 V=1,3,6
```

*L* e *V* prosseguiram a partir dos valores anteriores, e o `CausalCheck` deu OK.

**(e) Alarme escrito antes da origem, na réplica 1 (p0 e p1).** Para o tornar reproduzível, a réplica 1 foi arrancada com `-Ddescartar=2`. Foi introduzido `S01 21.0` e depois `S01 29.5`, que dá p0#2 e origina `ALARM-S01` → p1#1.

```
[p0] Aplicado: 1;S01;21.0;…;1;1,0,0 (escrito …850802)
[p0] DESCARTADO (simulação): 2
[p1] Aplicado: 1;ALARM-S01;29.5;…;3;2,1,0 (escrito …852679)     ← o alarme
[p0] EM ESPERA: 3 … 13 (falta o registo 2)
[p0] RECUPERAÇÃO: 2..2 pedida a 127.0.0.1:7000
[p0] Aplicado: 2;S01;29.5;…;2;2,0,0 (escrito …866036)           ← a origem, 13,4 s depois
```

O alarme tem *V* = (2,1,0): depende de p0#2. Mesmo assim, foi escrito **13,4 s antes** do registo que o originou. As réplicas aplicam cada partição pela sua ordem, sem olhar para os carimbos. O mesmo acontece sem simulação sempre que o `REG` de origem é recusado pelo daemon e só chega mais tarde por TCP.

### Ponto 8 — Falhas introduzidas

Cada falha foi introduzida numa **cópia** do código (o código entregue não foi alterado), com o sistema completo e `rajada 300 10`, ou com leituras à mão no caso do reinício.

| Falha introduzida | Lado onde surgiu | Detetada por `CausalCheck`? | O que permitiu concluir |
|---|---|---|---|
| Líder que incrementa *V*[*k*] sem fazer `merge` com *V*<sub>msg</sub> | Líder (nos vetores que escreve) | **Não**. `V[k] = seq` OK, 0 violações, `RESULTADO: OK`. Mas com 36 alarmes, **0 pares causais** entre partições, como se não houvesse processador | Sem o `merge`, o vetor reduz-se ao `seq` e perde toda a informação causal entre partições. O *L* continuava certo, por isso a condição de relógio continuou a verificar-se. Só se percebe o erro sabendo que devia haver relações causais. Numa verificação que só confirma invariantes, um vetor «a menos» passa |
| Líder que atualiza *L* sem considerar *L*<sub>msg</sub> (`L = L + 1`) | Líder (no *L* que escreve) | **Sim**: 403 violações da condição de relógio, por exemplo `p1#7 → p0#7 mas L = 7 >= 7` | O *V* continuava certo e mostrava `a → b`, mas o *L* deixou de crescer ao receber conhecimento de outra partição. É precisamente a parte `max(L, Lmsg)` que garante a condição de relógio. Os vetores servem de referência para detetar erros no *L* |
| Líder reiniciado com *L* e *V* a zero (`seq` continua do ficheiro) | Líder | **Com a verificação do ponto 4 ativa: não é preciso.** O líder recusou a primeira leitura (`RECUSADO: V[2] = 1 diferente de seq = 5`), que se perdeu. Depois «sarou» sozinho: o alarme seguinte trazia *V*<sub>msg</sub>[2] = 4, que o processador tinha visto, e o `merge` repôs `V[2] = 5 = seq`. **Sem a verificação: sim**, `V[2] = seq: ERRO em 3 linhas` (por exemplo `seq 5 com V = 0,0,1`) | Partir de zero quebra o invariante `V[k] = seq` e, pior, faz o novo evento parecer **anterior** aos eventos antigos da mesma partição (`(0,0,1) < (0,1,4)`). A verificação no líder impede que isso chegue ao ficheiro. Por isso *L* e *V* têm de vir da última linha do ficheiro |
| Processador que publica o vetor sem `copy()`, alterado depois por outra thread | Processador (no vetor que publica), visível nos ficheiros dos líderes | **Sem alterar o tempo: não** (0 violações). A janela entre sair do `lock` e serializar o vetor é de microssegundos, e a corrida não se viu. **Com a janela alargada** (pausa de 50 ms antes de publicar): **sim**, 20 violações, por exemplo `p1#92 → p0#77 mas L = 92 >= 92`. Controlo com a mesma pausa **e** `copy()`: 0 violações | O *L* foi copiado dentro do `lock`, mas o *V* foi lido depois, já com registos que chegaram entretanto. O alarme ficou com um vetor «do futuro», mais recente do que o seu *L*. Uma corrida rara pode não aparecer nos testes, e quando aparece é por vezes detetável pela incoerência entre *L* e *V* |
| Produtor executado com N = 4 e restantes processos com N = 3 | Líderes | Não há nada para detetar: **nenhum registo** foi escrito | Os três líderes rejeitaram **todas** as leituras: `REJEITADO: IN;S01;21.0;0;0,0,0,0 (Vetor com dimensão 4, esperado 3)`. As leituras para p3 (por exemplo `S04`, que com N = 4 vai para p3) foram publicadas em `rep03-in-p3`, onde não há líder, e perderam-se em silêncio. O vetor torna visível a configuração errada logo no primeiro `IN`, antes ainda da verificação da partição |

---

## 4.3 · Comparação e reflexão crítica

### Ponto 9 — Valores medidos em 7(c) (média das três execuções)

| Métrica | Valor |
|---|---|
| Registos por partição (p0 / p1 / p2) | 122,3 / 93,7 / 107,7 |
| Alarmes publicados | 38,7 |
| Pares entre partições causalmente relacionados | 30 616,7 (88,2 %) |
| Pares entre partições concorrentes | 4 094,3 (11,8 %) |
| Pares concorrentes com *L* distinto (proporção) | 3 847,0 (94,0 %) |
| Tamanho médio de uma mensagem `REG` sem carimbos (bytes) | 50,7 |
| Tamanho médio de uma mensagem `REG` com carimbos (bytes) | 62,7 (+12,0, ≈ +24 %) |

Para comparação, sem processador (7(b)): 0 pares causais, 100 % concorrentes, 99,1 % com *L* distinto.

### Ponto 10 — Questões

**Lamport e concorrência.**
- O relógio de Lamport **ordena 94 %** dos pares concorrentes, porque têm *L* distinto. Só os 6 % que empatam ficam visivelmente «sem ordem», e mesmo esses são desempatados por *k* na vista.
- **Consequência:** em `vista-lamport.txt`, um alarme aparece depois de **muitíssimas** leituras com que é concorrente. Quem concluísse que «a leitura aparece antes do alarme, logo provocou-o» erraria em quase todos os pares concorrentes. Na Figura 1, *b* (`S01 21.0`) aparece antes de *c* (`ALARM-S02`) e não tem nada a ver com ele.
- *L*(*a*) < *L*(*c*) é condição **necessária** para *a* → *c*, mas não suficiente. Para concluir causalidade é preciso comparar os **vetores**.

**Dimensão do vetor.**
- **Custo com N = 3:** +12 bytes por `REG`, de 50,7 para 62,7 (≈ +24 %). São o `;L` e o `;v0,v1,v2`, com contadores de 2 a 3 dígitos.
- **Crescimento:** linear em N (uma componente por partição) e logarítmico no valor dos contadores (número de dígitos). Com N = 100 e contadores de 6 dígitos seriam ≈ 700 bytes por mensagem, mais do que o registo.
- **Se N variar** com o sistema em funcionamento: os vetores já escritos têm a dimensão antiga. O `parse` recusa-os: um líder com N = 4 **não arranca** sobre um ficheiro com N = 3 (`Vetor com dimensão 3, esperado 4: 2,0,0`), e o `CausalCheck 4` marca todas as linhas como inválidas. Para além disso, a função de partição muda, e os sensores mudam de partição. Seria preciso converter todos os vetores (acrescentar uma componente a zero é semanticamente correto) e reparticionar. Os relógios de dimensão variável estão fora do âmbito.

**Relógios e números de sequência.**
- `V[k] = seq` em **todas** as linhas de todos os ensaios corretos, por isso o `seq` não traz informação que o vetor não tenha.
- **Mas não é redundante na prática.** A réplica usa o `seq` para aplicar pela ordem, detetar lacunas e pedir `PEDIDO <de> <até>`, sem precisar de saber N nem de interpretar vetores. A recuperação continua a depender dele.
- **O que o vetor acrescenta:** as outras componentes dizem **quanto de cada outra partição** o evento «conhece» (*V*[*i*] para *i* ≠ *k*). É isso que permite comparar registos de partições diferentes.

**Entrega causal.**
- **A condição:** antes de escrever um registo *r* de p1, a réplica 1 teria de confirmar que já escreveu, em p0, todos os registos com `seq ≤ V(r)[0]`, ou seja, `last(p0) ≥ V(r)[0]`. Em 7(e), o alarme tinha `V = (2,1,0)` e a réplica tinha `last(p0) = 1`: teria de **reter** o alarme até chegar p0#2, 13,4 s depois.
- **Só sobre as partições que guarda:** a réplica 1 não guarda p2, por isso não tem `last(p2)` para comparar com *V*(*r*)[2]. Também não precisa: não tem registos de p2 que pudessem ficar fora de ordem no seu disco. As dependências que passam por p2 já estão refletidas, por transitividade, nas componentes 0 e 1 do vetor.
- **Cabe no âmbito?** **Não.** A ficha exclui explicitamente a entrega causal nas réplicas. **A solução desta ficha não resolve esse problema**: só permite **detetá-lo** depois, comparando os vetores.

**Troço produtor → líder.**
- **Não.** O produtor envia sempre `L = 0` e `V = (0,0,0)`, por isso uma leitura não leva nada que a distinga das outras nem que a relacione com o que veio antes.
- Os relógios só contam **eventos dos líderes**. Uma leitura que nunca chega ao líder não gera evento, e nenhum vetor fica com uma componente «a mais» à espera dela.
- Em 7(b) e 7(c), 12 a 20 leituras foram recusadas pelo daemon, e o `CausalCheck` deu OK em todas as execuções.
- O mesmo vale para os alarmes perdidos no troço processador → líder (0 a 4 por execução): o processador sabe que falhou, mas nenhum carimbo o regista.

---

## Critérios de aceitação

### CA1 · Fundamentos

- **«Aconteceu antes»** (Lamport, 1978) é a menor relação que satisfaz três regras:
  1. dois eventos do mesmo processo estão pela ordem em que ocorreram;
  2. o envio de uma mensagem acontece antes da sua receção;
  3. a relação é transitiva.

  Se nem *a* → *b* nem *b* → *a*, os eventos são **concorrentes**.
- **Na Figura 1:** *a* → *c*, porque o processador observou *a* e enviou o `IN` que gerou *c*. *b* → *d*, porque são eventos do mesmo líder, por esta ordem. *b* ∥ *c*, *c* ∥ *d*, *a* ∥ *b* e *a* ∥ *d*.
- **Lamport e vetores:** o relógio de Lamport dá só a **condição de relógio** (*a* → *b* ⇒ *L*(*a*) < *L*(*b*), e não o contrário). O vetor **caracteriza** a relação: *a* → *b* ⇔ *V*(*a*) < *V*(*b*), e concorrência ⇔ vetores incomparáveis.

Perguntas de verificação:
- **Concorrentes com *c*:** *b* e *d*. *V*(*b*) = (1,0,0) e *V*(*c*) = (0,1,1): *b* é maior na componente 0 e *c* nas componentes 1 e 2. *V*(*d*) = (2,0,0): o mesmo raciocínio. Nenhum é ≤ ao outro.
- **Porque é que (*L*, *k*) é consistente com a causalidade:** se *a* → *b*, então *L*(*a*) < *L*(*b*), e *a* fica antes de *b* na ordem por (*L*, *k*), seja qual for *k*. O desempate por *k* só atua quando *L* é igual, e eventos com o mesmo *L* **nunca** são causalmente relacionados (seria *L*(*a*) < *L*(*b*)). A arbitrariedade só afeta pares concorrentes, cuja ordem relativa não tem significado. Verificado: 0 violações em todos os ensaios corretos.
- **Se o processador tivesse visto *b* antes do alarme:** *L*<sub>p</sub> = max(1, 1) = 1 e *V*<sub>p</sub> = (1,1,0). O líder de p2 calcularia *V* = max((0,0,0), (1,1,0)) + tick(2) = **(1,1,1)** e *L* = max(0, 1) + 1 = 2. Nesse caso *b* → *c*, e só *d* continuaria concorrente com *c*.

### CA2 · Implementação

- **Onde o líder atualiza *L* e *V*:** em `PartitionLeader.onIn`, dentro de `synchronized (lock)`: `newL = Math.max(lamport, lMsg) + 1`, `newV.merge(vMsg)`, `newV.tick(k)`, a verificação `newV.get(k) != newSeq` e o `file.append(r)`. Só então `seq = newSeq; lamport = newL; vector = newV;`.
- **Onde o processador acumula:** em `AlarmProcessor.onReg`, dentro de `synchronized (lock)`: `lamport = Math.max(lamport, r.getLamport()); vector.merge(r.getVector());`. Os carimbos do alarme são `l = lamport; v = vector.copy();`, tirados ainda dentro do `lock`.
- **Onde se serializam os carimbos:**
  - `SensorRecord.toLine()` (`…;L;V`), usado no ficheiro, no `REG` e na recuperação por TCP;
  - `Wire.in(...)` (`IN;sensor;temp;L;V`);
  - `VectorClock.toString()`/`parse(...)`.
- **A secção sincronizada protege** *L*, *V*, `seq` e o ficheiro, partilhados entre a thread de entrada (escreve) e a thread de recuperação (lê o `seq`).

Perguntas de verificação:
- **Mesma secção? Sim.** Se a atualização dos relógios estivesse fora, numa secção separada:
  - outra thread podia meter-se entre as duas partes. Um registo podia sair com o *V* de outro evento, ficando `V[k] ≠ seq` (o erro que o `CausalCheck` assinala), ou com *L* fora de ordem dentro da própria partição (violação da condição de relógio);
  - se o `append` falhasse, os relógios já teriam avançado sem registo correspondente;
  - e a recuperação podia ver um `seq` sem a linha no ficheiro.
- **Ao reiniciar:** de `RecordFile.last()`, a última linha do `lider-p<k>.txt`, que tem *L* e *V* do último evento (ensaio 7(d)). **A partir de zero** (falha 3): `V[k]` recomeçava em 1 com o `seq` em 5. O líder recusa esses eventos, ou, sem a verificação, escreve vetores que fazem os novos eventos parecer **anteriores** aos antigos, e *L* volta a valores já usados.
- **Porque é que as réplicas não atualizam relógios:** não geram eventos. Não atribuem números, não criam registos e não enviam nada que influencie outro processo. Só **copiam** registos com os carimbos que o líder lhes deu. Os carimbos descrevem o momento em que o líder **criou** o registo, não o momento em que uma réplica o escreveu.

### CA3 · Análise

Com `CausalCheck`, em todos os ensaios com o código correto:
- **`V[k] = seq`:** OK em todas as linhas de todos os ficheiros (7(a)–(d), 3 × 7(c));
- **condição de relógio:** 0 violações em ≈ 47 000 pares `a → b` por execução de 7(c);
- **concorrentes com *L* distinto:** 94,0 % com alarmes e 99,1 % sem alarmes.

Perguntas de verificação:
- **Rajada sem alarmes:** **0** pares causais entre partições (7(b): 0 em 27 048). O produtor envia sempre o vetor nulo, e os líderes não comunicam, por isso cada vetor só tem a sua componente diferente de zero, e vetores com componentes distintas são sempre incomparáveis.
- **Fração dos concorrentes com *L* distinto:** 94,0 % com alarmes, 99,1 % sem. **Diminui com a frequência de alarmes.** Cada alarme leva o *L* do processador (o máximo de todas as partições) para o líder de destino, e os *L* das três partições passam a avançar «sincronizados», com mais empates entre eventos concorrentes. Sem alarmes, *L* = `seq` em cada partição, e só empatam pares com o mesmo `seq`. Ao mesmo tempo, o **número** de pares concorrentes cai muito (de 100 % para ≈ 12 %), porque os alarmes tornam causais a maioria dos pares.
- **Com o processador parado:** **nenhuma relação nova** entre partições. As que já estão nos ficheiros mantêm-se, porque os vetores ficam escritos e os líderes continuam a partir deles. Mas os eventos novos de partições diferentes só são comparáveis através dessas dependências antigas: um evento novo de p0 continua concorrente com um evento novo de p1.

### CA4 · Limites

Perguntas de verificação:
- **A réplica 1 pode escrever um alarme antes da origem?** **Sim**, e aconteceu em 7(e): `ALARM-S01` (p1#1, *V* = (2,1,0)) foi escrito 13,4 s antes de p0#2. Para o impedir era precisa **entrega causal**: reter um registo de p1 até `last(p0) ≥ V(r)[0]`. Está fora do âmbito.
- **N de 3 para 4:** os vetores já escritos têm 3 componentes. O líder com N = 4 **não arranca** (`Vetor com dimensão 3, esperado 4`) e o `CausalCheck 4` rejeita todas as linhas. Teriam de ser convertidos (acrescentar a componente 3 a zero), e os sensores teriam de ser reparticionados (65 % mudam de partição, COMINDIR02).
- **Leitura perdida entre o produtor e o líder:** **não deixa vestígio** nos relógios. O produtor manda sempre `L = 0`, `V = 0`, e os relógios só contam eventos dos líderes.

**Limitações que se mantêm, e o que faria falta:**
1. **Ausência de entrega causal nas réplicas** (7(e)) → reter registos até as dependências das outras partições guardadas estarem aplicadas, com `last(p_i) ≥ V(r)[i]`.
2. **Dimensão fixa do vetor** → relógios de dimensão variável, ou uma conversão e reparticionamento coordenados quando N muda.
3. **Perdas no troço produtor → líder** (e processador → líder) → numerar as leituras na origem e recuperar as lacunas, como as réplicas fazem com os líderes.
4. **Custo do vetor** (+24 % por mensagem com N = 3, linear em N) → com muitas partições, só um mecanismo mais compacto o resolveria, e isso está fora do âmbito.
5. **A ordem de Lamport não serve para concluir causalidade:** ordena 94 % dos pares concorrentes como se fossem ordenados. Só os vetores permitem distinguir.
