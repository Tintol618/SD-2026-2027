# Sprint E1-UDP02 — Ordenação e Retenção de Mensagens Fora de Ordem

## Estrutura

| Projeto | Ficheiro |
|---|---|
| Servidor | `UDPServer/src/UDPServer.java` |
| Cliente  | `UDPClient/src/UDPClient.java` |

Execução (dois terminais):

```bash
cd UDPServer/src && java UDPServer.java
cd UDPClient/src && java UDPClient.java
```

O cliente é o da UDP01 (modo automático por omissão; comandos `/manual`, `/auto`, `/sair`), apenas passa a interpretar as novas respostas.

## Protocolo

- **Pedido:** `<N>,<mensagem>`
- **Estado do servidor:**
  - `L` (`lastInOrder`) — número da última mensagem **entregue** em ordem. Inicial `0`.
  - `receptionList` — lista de receção (`ArrayList<String>`): mensagens **entregues**, por ordem.
  - `pending` — estrutura temporária (`TreeMap<Integer,String>`, N → mensagem): mensagens **recebidas** fora de ordem, ainda **não entregues**.
- **Respostas:**

| Situação | Resposta | Efeito |
|---|---|---|
| `N == L+1` | `ok,<novo L>` | entrega N e, em cascata, as seguintes que estavam guardadas |
| `N > L+1` | `waitingfor,<L+1>` | guarda N na estrutura temporária; L não muda |
| `N <= L` ou N já guardada | `dup,<N>` | duplicado, ignorado; nada muda |
| mal formada (sem vírgula / N não numérico / N < 1) | `waitingfor,<L+1>` | ignorada; nada muda |

A resposta `ok` leva o **último** número entregue (e não N), para o cliente saber até onde a cascata chegou.

## 4.1 · Estruturas de dados

**Custo da UDP01.** Na UDP01 uma mensagem adiantada é descartada; o cliente tem de a reenviar depois de reenviar a que faltava. Cada mensagem que chega antes do "buraco" custa uma retransmissão.

**Recebida vs. entregue.**
- *Recebida* — o datagrama chegou ao servidor.
- *Entregue* — passou para a aplicação (lista de receção), o que só pode acontecer **por ordem**.

Uma mensagem fora de ordem é recebida mas não entregue: fica na estrutura temporária até o buraco fechar.

**Escolha das estruturas.**
- Lista de receção → `ArrayList`: as entregas são sempre acrescentadas no fim e já por ordem; a posição `i` corresponde à mensagem `i+1`.
- Estrutura temporária → `TreeMap<Integer,String>`: o acesso é **pelo número** ("tenho a L+1?"), por isso um mapa indexado por N é o natural (`containsKey`/`remove`). O `TreeMap` mantém as chaves ordenadas, o que torna o estado legível nos logs; um `HashMap` também funcionaria.
- Não é preciso guardar mais nada além de L: tudo o que está em `pending` é > L+1.

## 4.2 · Processamento — `processDeliveredMessages`

```java
static int processDeliveredMessages(int nLastMessageInOrder, int nCurrentMessage, String currentMessage)
```

**Contrato**
- *Entradas:* L atual, número N da mensagem recebida, conteúdo (sem o número).
- *Saída:* o novo L (última mensagem entregue depois do processamento).
- *Efeitos:* pode acrescentar à lista de receção e acrescentar/retirar da estrutura temporária.
- *Garantias:* a lista de receção fica sempre por ordem e sem buracos; nenhuma mensagem está ao mesmo tempo nas duas estruturas; um duplicado não altera nada.

**Entrega em cascata.** Quando chega a L+1, é entregue; depois, enquanto a estrutura temporária tiver a seguinte (L+2, L+3, …), essa é retirada e entregue também. Uma única mensagem pode assim desbloquear várias.

```java
receptionList.add(currentMessage);
int last = nCurrentMessage;
while (pending.containsKey(last + 1)) {
    last++;
    receptionList.add(pending.remove(last));
}
return last;
```

O `main` só trata da rede e do *parsing*; toda a lógica de ordenação está isolada neste método.

## 4.3 · Verificação

### Cenário normal (modo automático)

| Mensagem | Resposta | L | Lista de receção | Temporária |
|---|---|---|---|---|
| 1,olá   | ok,1 | 1 | [olá] | {} |
| 2,mundo | ok,2 | 2 | [olá, mundo] | {} |
| 3,cruel | ok,3 | 3 | [olá, mundo, cruel] | {} |

### Desordenação múltipla: 1, 3, 4, 2, 3 (modo manual)

| Mensagem | Resposta | L | Lista de receção | Temporária | Justificação |
|---|---|---|---|---|---|
| 1,olá   | ok,1         | 0→1 | [olá] | {} | 1 = L+1 → entregue |
| 3,mundo | waitingfor,2 | 1   | [olá] | {3=mundo} | 3 > L+1 → recebida, guardada |
| 4,cruel | waitingfor,2 | 1   | [olá] | {3=mundo, 4=cruel} | 4 > L+1 → recebida, guardada |
| 2,lindo | ok,4         | 1→4 | [olá, lindo, mundo, cruel] | {} | 2 = L+1 → entregue; cascata entrega 3 e 4 |
| 3,mundo | dup,3        | 4   | [olá, lindo, mundo, cruel] | {} | 3 ≤ L → duplicado, ignorado |

Lista final completa e ordenada: `[olá, lindo, mundo, cruel]` (mensagens 1, 2, 3, 4). A estrutura temporária termina vazia.

### CA4 — Sequência 1, 3, 4, 2

Igual às quatro primeiras linhas da tabela acima. Interpretação: a 3 e a 4 são **recebidas mas não entregues** enquanto falta a 2 (o servidor responde `waitingfor,2`, mas **não** as descarta). Quando chega a 2, uma só mensagem fecha o buraco e provoca a entrega em cascata de 2, 3 e 4; L salta de 1 para 4 e a resposta é `ok,4`. O cliente não reenviou nada.

## 4.4 · Reflexão crítica

### CA1 — Custo de retransmissão com 1, 3, 4, 5, 2

| | UDP01 (descarta) | UDP02 (guarda) |
|---|---|---|
| 1 | aceite | entregue |
| 3, 4, 5 | descartadas (`waitingfor,2`) | guardadas |
| 2 | aceite | entregue + cascata 3, 4, 5 |
| Retransmissões | **3** (3, 4 e 5 têm de ser reenviadas) | **0** |
| Datagramas enviados no total | 8 | 5 |

Em geral, na UDP01 o custo é igual ao número de mensagens que chegam adiantadas; na UDP02 é zero (enquanto as mensagens não se perdem).

### Crescimento não limitado da estrutura temporária (CA5)

Não há limite para o que se guarda. Se a 2 se perder para sempre, todas as mensagens seguintes ficam guardadas indefinidamente; e um N muito grande (ex.: `1000000,x`) é guardado e nunca será entregue. A memória cresce sem controlo — basta um cliente com erro (ou malicioso) para esgotar o servidor.

**Solução:** uma **janela de receção** de tamanho fixo W: só se guardam mensagens com `L+1 < N <= L+W`; fora da janela são rejeitadas (`waitingfor,L+1`) e o cliente terá de as reenviar mais tarde. Assim a estrutura temporária tem no máximo W−1 elementos.

### Duplicados

Agora são identificados: `N <= L` (já entregue) ou N já presente na estrutura temporária (já recebido) → `dup,N`, sem alterar o estado. Na UDP01 eram confundidos com mensagens fora de ordem.

### Outras limitações

- **Perdas:** continuam sem recuperação. Se a L+1 se perder, o servidor só pede `waitingfor,L+1`; se a última se perder (ou a resposta), o cliente fica bloqueado no `receive`. Solução: *timeout* no cliente (`setSoTimeout`) e retransmissão.
- **Vários clientes:** L, lista e estrutura temporária são únicos e partilhados. Solução: um estado por cliente, num `Map` indexado por (IP, porto) de origem.

### Características do UDP

| Característica | UDP01 | UDP02 |
|---|---|---|
| Desordenação | Detetada, mensagem descartada | **Resolvida** (reordenada sem retransmissão) |
| Duplicação | Não identificada | **Detetada** e ignorada |
| Perda | Parcialmente detetada | Parcialmente detetada; não recuperada |
| Corrupção | *Checksum* do UDP (≈ perda) | *Checksum* do UDP (≈ perda) |
