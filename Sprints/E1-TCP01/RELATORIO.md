# Sprint E1-TCP01 — Comunicação Baseada em Streams e Serialização de Objetos

## Estrutura

Dois projetos separados (simulam duas máquinas), todas as classes no pacote `tcp01`:

| Projeto | Classes |
|---|---|
| `tcp01-servidor/src/tcp01/` | `TCPServer`, `Connection`, `Person`, `Place` |
| `tcp01-cliente/src/tcp01/`  | `TCPClient`, `Person`, `Place` |

`Person` e `Place` existem **nos dois** projetos, com o mesmo pacote e o mesmo `serialVersionUID`: o cliente precisa delas para **criar** e **serializar** o objeto; o servidor precisa delas para o **reconstruir** (`readObject`) e para fazer o *cast*.

### Execução (servidor sempre primeiro)

Em cada projeto, num terminal próprio:

```bash
cd tcp01-servidor && javac -d out src/tcp01/*.java && java -cp out tcp01.TCPServer
cd tcp01-cliente  && javac -d out src/tcp01/*.java && java -cp out tcp01.TCPClient
```

Confirmar que o servidor está à escuta: `netstat -an | findstr 7896`.

Resultado esperado — cliente: `Received: Viseu`; servidor: `[Thread-0] recebido: Ana Silva (2000), 3504-510 Viseu`.

---

## 4.1 · Trocar texto entre cliente e servidor

### Operações que bloqueiam (e o que esperam)

| Ficheiro | Linha | À espera de… |
|---|---|---|
| `TCPServer` | `listenSocket.accept()` | que um cliente se ligue ao porto 7896 |
| `Connection` | `in.readUTF()` (versão texto) / `new ObjectInputStream(...)` e `in.readObject()` (versão objetos) | que o cliente envie uma string completa / o cabeçalho da stream de objetos / um objeto completo |
| `TCPClient` | `new Socket("localhost", 7896)` | que a ligação TCP fique estabelecida (falha logo se ninguém estiver à escuta) |
| `TCPClient` | `in.readUTF()` | a resposta do servidor |

As escritas (`writeUTF`, `writeObject`) não bloqueiam à espera do outro lado: os bytes vão para os *buffers* do TCP e a escrita retorna.

**Ponto 3.** Com o código de partida, o cliente recebe `Received: mensagem em UTF` — o servidor devolve o *echo*.

**Ponto 4 — cliente sem servidor.** Falha na linha `s = new Socket("localhost", serverPort)` com `java.net.ConnectException: Connection refused` (apanhada no `catch (IOException)` → `IO: ... Connection refused`). Não chega a haver ligação, por isso nada é enviado.

## 4.2 · Enviar um objeto

**Ponto 5 — `Person`.** Implementa `Serializable` (sem isso o Java recusa serializá-la) e declara `private static final long serialVersionUID = 1L` para fixar a versão. Tem construtor e *getters*. Está nos dois projetos, no pacote `tcp01` (ver justificação acima).

**Ponto 6 — cliente.** A stream de saída passa a ser um `ObjectOutputStream` (`writeObject(person)` + `flush()`); a de entrada continua a ser um `DataInputStream` (`readUTF`), porque a resposta continua a ser texto.

**Ponto 7 — servidor.** A stream de entrada passa a ser um `ObjectInputStream`; a de saída continua `DataOutputStream`.
- `readObject()` devolve `Object`, por isso confirma-se o tipo com `instanceof` e faz-se o *cast* para `Person`; se chegar outra classe, responde-se com uma mensagem de erro em vez de rebentar com `ClassCastException`.
- A `ClassNotFoundException` é tratada num `catch` próprio (surge quando a classe do objeto recebido não existe no servidor).
- **Decisão de desenho:** o `ObjectInputStream` é criado no `run()`, e não no construtor da `Connection`. A criação bloqueia até chegar o cabeçalho do `ObjectOutputStream` do cliente; como o construtor corre na thread principal, um cliente que se ligasse e não enviasse nada **bloquearia o `accept()`** e mais ninguém seria atendido.

**Ponto 8.** O cliente recebeu o nome da pessoa (`Received: Ana Silva`).

## 4.3 · Enviar um objeto com dependências

**Ponto 9.** `Place` (`postalCode`, `locality`) também implementa `Serializable` e declara `serialVersionUID`, e existe nos dois projetos no pacote `tcp01`. A `Person` tem um atributo `private Place place` e o construtor `Person(String name, Place place, int year)`. O cliente só chama `writeObject(person)`.

**Ponto 10.** O servidor devolve `person.getPlace().getLocality()`; o cliente recebe `Received: Viseu`. Como o `Place` nunca foi escrito explicitamente, isto prova que `writeObject` percorreu o grafo de objetos alcançáveis a partir da `Person` e serializou o `Place` com ela.

**Ponto 11 — variantes sem alterar a lógica**

| Variante | Alteração | Resultado |
|---|---|---|
| Continua a funcionar | Método novo `getAge(int)` na `Person`, nos dois projetos | `Received: Viseu` — métodos não fazem parte do estado serializado e o `serialVersionUID` declarado não mudou |
| Continua a funcionar | Atributo novo `email` na `Person` **só no servidor**, mesmo `serialVersionUID` | `Received: Viseu` — o Java aceita; o campo que não veio na stream fica com o valor por omissão (`null`) |
| Deixa de funcionar | `serialVersionUID = 2L` na `Person` do servidor | **Servidor**, no `readObject()`: `InvalidClassException: tcp01.Person; local class incompatible: stream classdesc serialVersionUID = 1, local class serialVersionUID = 2`. Cliente: `EOF: null` |
| Deixa de funcionar | **Sem** `serialVersionUID` declarado e método novo só no servidor | **Servidor**: `InvalidClassException` com UIDs calculados automaticamente diferentes (`1681929146346088193` vs `6160865710256460438`) — mostra por que razão se declara o UID explicitamente |

## 4.4 · Verificação

### Ponto 13 — Falhas deliberadas

| Falha introduzida | Lado onde surgiu | Exceção obtida | Momento | O que a exceção permitiu (ou não) concluir |
|---|---|---|---|---|
| Arrancar o cliente sem o servidor | Cliente | `java.net.ConnectException: Connection refused` | Ligação (`new Socket`) | Não há ninguém à escuta no porto; nada chegou a ser enviado. Não distingue "servidor desligado" de "porto errado". |
| Retirar `implements Serializable` do `Place` | Cliente (causa) e servidor (consequência) | Cliente: `NotSerializableException: tcp01.Place`. Servidor: `WriteAbortedException: writing aborted; java.io.NotSerializableException: tcp01.Place` | Escrita no cliente (`writeObject`); leitura no servidor (`readObject`) | A causa real está no cliente: basta uma classe do grafo não ser serializável para toda a escrita falhar. O servidor só sabe que a escrita foi abortada do outro lado (a mensagem traz a causa, mas o erro não é dele). |
| `serialVersionUID` da `Person` diferente nos dois lados | Servidor (cliente vê `EOF: null`) | `InvalidClassException: tcp01.Person; local class incompatible: stream classdesc serialVersionUID = 1, local class serialVersionUID = 2` | Leitura (`readObject`) | Os bytes chegaram intactos, mas as versões da classe são incompatíveis. O cliente só vê a ligação fechada sem resposta (`EOFException`) — não fica a saber porquê. |
| `Person` num pacote diferente no servidor (`servidor.Person`) | Servidor (cliente vê `EOF: null`) | `ClassNotFoundException: tcp01.Person` | Leitura (`readObject`) | Para o Java, `tcp01.Person` e `servidor.Person` são classes diferentes, mesmo com código igual: o nome completo inclui o pacote. |

### Concorrência (CA2) — demonstração

Um cliente "parado" liga-se e não envia nada; enquanto está ligado, arranca-se o `TCPClient` normal.

| Servidor | Resultado do cliente normal |
|---|---|
| `Connection` com `this.start()` | `Received: Viseu` — atendido de imediato pela `Thread-1`, enquanto a `Thread-0` continua à espera do cliente parado |
| `Connection` com `this.run()` (pedido processado na thread do `accept()`) | Fica **bloqueado** à espera da resposta enquanto o cliente parado estiver ligado — o servidor só volta ao `accept()` depois de terminar o primeiro |

### Ponto 14 — Construções usadas

| Construção | Onde a usou | O que ficou a ser garantido | O que continua a não ser garantido |
|---|---|---|---|
| `ServerSocket` / `accept()` | `TCPServer.main` | Um porto fixo (7896) conhecido pelos clientes; cada `accept()` devolve um `Socket` dedicado a uma ligação | Que o pedido do cliente seja válido ou chegue alguma vez; não há limite ao número de ligações |
| `Connection extends Thread` | `Connection`, criada por cada `accept()`, `start()` no construtor | Cada cliente é atendido na sua thread; a thread principal volta logo ao `accept()` | Escalabilidade: uma thread por cliente (milhares de clientes → milhares de threads); não há partilha segura de estado entre threads |
| `implements Serializable` | `Person` e `Place` (nos dois projetos) | O Java aceita converter os objetos em bytes e reconstruí-los | Que o outro lado tenha a classe, no mesmo pacote e com versão compatível; controlo sobre o que é enviado |
| `ObjectOutputStream` / `ObjectInputStream` | Cliente: saída (`writeObject`); servidor: entrada (`readObject`) | Envio de objetos completos, com o grafo de dependências, sobre a stream TCP | O tipo do objeto recebido (é preciso *cast*); interoperabilidade com outras linguagens |
| `serialVersionUID` | `Person` e `Place`, `= 1L` | Versão estável: mudanças compatíveis (métodos, atributos novos) não quebram a comunicação; incompatibilidades são detetadas (`InvalidClassException`) | Que as versões estejam iguais nos dois lados — tem de ser gerido à mão |
| Referência para `Place` | Atributo `place` da `Person` | O `Place` viaja automaticamente com a `Person` | Que todas as classes do grafo sejam serializáveis (senão `NotSerializableException`); controlo sobre o volume enviado |

---

## Critérios de aceitação

### CA1 · Modelo de comunicação TCP

- **Percurso:** `new Socket` estabelece a ligação (o servidor sai do `accept()`); `writeObject`/`writeUTF` coloca os bytes na stream; o TCP divide-os em segmentos **numerados**, com *checksum*, confirmados e **retransmitidos** se não houver confirmação até ao *timeout*; no servidor a stream entrega os bytes **intactos e por ordem**, e o `readObject`/`readUTF` só retorna quando tem a mensagem completa.
- **Bloqueios:** ver tabela em 4.1.
- **Cliente antes do servidor:** falha em `new Socket(...)` com `ConnectException: Connection refused`.
- **Intacta ≠ percebida:** no caso do `serialVersionUID` diferente e da `Person` noutro pacote, os bytes chegaram sem um único erro e o pedido falhou (`InvalidClassException`, `ClassNotFoundException`).
- **`ServerSocket` + `Socket` vs. só `Socket`:** o `ServerSocket` só serve para aceitar ligações; cada ligação aceite tem o seu `Socket`. O cliente não aceita ligações, só se liga. Com dois clientes ligados, o servidor tem **3 sockets**: 1 `ServerSocket` + 2 `Socket`.
- **Servidor fecha sem responder:** o `readUTF()` do cliente lança `EOFException` (`EOF: null`).

### CA2 · Concorrência no servidor

- A nova thread começa no `start()` (chamado no construtor); o código dela é o `run()`. Chamar `run()` diretamente executa-o na thread principal — demonstrado acima: o segundo cliente fica bloqueado.
- Sem a `Connection`, um cliente que se ligue e não envie nada impede o `accept()` de atender mais alguém.
- Com três clientes ligados: **4 threads** (a principal + 3 `Connection`). Depois de terminarem: só a principal, de novo no `accept()`.
- O socket do cliente fecha-se no `finally` do `run()` porque é essa thread que o usa e só ela sabe quando terminou; se a thread principal o fechasse, podia fechá-lo a meio do atendimento. O `finally` garante o fecho mesmo em caso de exceção.

### CA3 · Serialização de objetos

- Sem `Serializable`, o erro surge **no lado que escreve** (`NotSerializableException` no `writeObject` do cliente); o servidor só vê a consequência (`WriteAbortedException`).
- O *cast* é necessário porque `readObject()` devolve `Object`; se chegasse outra classe, o *cast* direto lançaria `ClassCastException` — por isso o servidor verifica com `instanceof` e responde com erro.
- A `Person` não tem de ser **exatamente** igual (um método novo, ou um atributo novo com o mesmo UID, funcionam), mas tem de ter o **mesmo nome completo** (pacote incluído) e um `serialVersionUID` **compatível** — demonstrado no ponto 13.
- Entrada de objetos e saída de dados funcionam porque são **sentidos diferentes** da ligação: cada sentido usa um só tipo de stream. Se o servidor também devolvesse um objeto, os dois lados passariam a usar `ObjectOutputStream`/`ObjectInputStream`, e cada lado teria de criar **primeiro** o `ObjectOutputStream` — senão os dois ficavam bloqueados à espera do cabeçalho um do outro.

### CA4 · Dependências, versões e erros

- O `Place` chegou porque `writeObject` serializa todo o grafo alcançável a partir da `Person`; o `Place` tem de implementar `Serializable` e existir no servidor (mesmo pacote).
- `Place` não serializável: o cliente falha com `NotSerializableException`; o servidor vê `WriteAbortedException`, cuja mensagem refere a causa, mas a falha não é dele.
- UID diferente: rejeitado pelo **servidor**, no momento da **leitura**. Atributo novo **sem** mexer no UID declarado: funciona (o campo em falta fica com o valor por omissão). Sem UID declarado, qualquer alteração muda o UID calculado e o objeto é rejeitado.
- `EOFException` no servidor significa que a ligação fechou antes de chegar a mensagem completa. Para distinguir: se o cliente registou um erro antes de enviar (ex.: `NotSerializableException`) → erro do cliente; se o servidor tinha um erro próprio (ex.: *cast*, classe em falta) → erro do servidor; se nenhum dos lados registou erro e a ligação caiu → rede. A exceção sozinha não basta: é preciso cruzar os registos dos dois lados.

### CA5 · Limites da solução

**Limitação escolhida: compatibilidade e interoperabilidade da serialização nativa.** O objeto só pode ser lido por um programa **Java** que tenha **a mesma classe**, no mesmo pacote e com `serialVersionUID` compatível. Um servidor noutra linguagem não consegue interpretar os bytes, e qualquer alteração incompatível na `Person` obriga a atualizar todos os clientes ao mesmo tempo.
**O que faria falta:** um formato de dados independente da linguagem e da classe Java, acordado entre as partes — algo que a serialização de base não resolve e que fica para mais tarde.

Outras limitações:
- **Atributo que não deve ser enviado (palavra-passe):** com os mecanismos desta ficha **não é possível** impedi-lo — todo o estado do grafo alcançável é enviado. A serialização de base não resolve este problema.
- **Dez mil clientes:** dez mil threads, cada uma com a sua pilha de memória; o servidor esgota recursos. Uma thread por ligação não escala — fica para mais tarde.
- **Validação (`year` plausível):** o servidor aceita qualquer `Person` bem formada. A verificação tem de ser feita pelo servidor, depois do `readObject()` e antes de usar o objeto — nunca confiar no cliente.

---

## 4.5 · Reflexão crítica

**Problemas que não eram de transmissão.** `serialVersionUID` incompatível, classe noutro pacote, classe não serializável e o tipo do objeto recebido (*cast*). Em todos, o TCP entregou os bytes corretamente. Quem os tinha de resolver era a **aplicação**: o programador, ao manter as classes iguais nos dois lados e ao tratar as exceções.

**Vários clientes quando o servidor muda a `Person`.** Os clientes antigos continuam a enviar a versão antiga. Se a alteração for compatível e o `serialVersionUID` se mantiver, o servidor continua a aceitá-los. Se a alteração for incompatível, muda-se o UID à mão e o servidor **rejeita** (`InvalidClassException`) os clientes desatualizados, em vez de interpretar mal os dados. O `serialVersionUID` é o mecanismo que torna esta incompatibilidade **detetável**; não a resolve — os clientes têm de ser atualizados.

**Riscos de enviar todo o grafo.** Volume: um objeto que referencie listas ou outros objetos grandes arrasta-os todos, mesmo que o servidor só precise de um campo (aqui, o servidor só usa a localidade mas recebe a `Person` inteira). Informação: dados sensíveis alcançáveis a partir do objeto (palavras-passe, dados pessoais) são enviados sem que ninguém o tenha pedido explicitamente.

**Thread por ligação vs. um de cada vez.** Ganha: clientes lentos ou parados não bloqueiam os outros (demonstrado) e o tempo de resposta não depende da fila. Gasta: memória e tempo de criação de uma thread por cliente, troca de contexto entre threads, e passa a ter de se preocupar com acesso concorrente a estado partilhado. Um de cada vez é mais simples e barato, mas um único cliente lento para o serviço todo.

**Quando preferir datagramas.** Quando a mensagem é pequena, independente e vale mais chegar depressa do que garantidamente, ou quando o próprio protocolo já repete o pedido. Exemplo: consultas de **DNS** (um pedido, uma resposta; se se perder, repete-se, sem custo de estabelecer ligação), ou **jogos online** em tempo real, onde a posição antiga de um jogador já não interessa quando chega a nova.
