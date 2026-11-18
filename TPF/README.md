# TPF · Arquivo distribuído zero-knowledge

Trabalho prático final (Épico 2). Esta pasta tem os projetos do trabalho:

| Projeto | Estado |
|---|---|
| `tpf-lider/` | **Sprint E2-01**: `LeaderBot` com os 7 comandos, validação dos argumentos e respostas provisórias |
| `tpf-replica/` | sprints seguintes |

Requisitos: **Java 17+** e **Maven 3.9+**. A única dependência é a **JDA** (API do Discord), descarregada pelo Maven.

## 1. Aplicação e bot no Discord Developer Portal

1. Em <https://discord.com/developers/applications> → **New Application** (por exemplo `Arquivo TPF`).
2. Separador **Bot**:
   - **Reset Token** → copia o token. **Não o guardes no repositório.** Vê o passo 4.
   - Em **Privileged Gateway Intents**, ativa **Message Content Intent**. É obrigatório para o bot ler as mensagens do canal `#arquivo`.
3. Separador **OAuth2 → URL Generator**:
   - em **Scopes**, marca `bot` e `applications.commands`;
   - em **Bot Permissions**, marca só `View Channels`, `Send Messages` e `Read Message History`.

   Abre o URL gerado e adiciona o bot ao servidor do grupo. O URL tem esta forma (com o *Application ID* do separador *General Information*):

   ```
   https://discord.com/oauth2/authorize?client_id=<APPLICATION_ID>&scope=bot+applications.commands&permissions=68608
   ```

   (`68608` = View Channels 1024 + Send Messages 2048 + Read Message History 65536)

## 2. Servidor de teste

1. Cria um servidor próprio do grupo.
2. Cria o canal de texto **`#arquivo`**.
3. Em **Definições do servidor → Funções**, cria a função **`admin-arquivo`** e atribui-a aos membros do grupo.
4. Copia o **ID do servidor**: ativa *Definições → Avançadas → Modo de programador* e depois clica com o botão direito no servidor → **Copiar ID do servidor**.

## 3. Token fora do repositório

O `LeaderBot` lê o token, por esta ordem:
1. da variável de ambiente **`DISCORD_TOKEN`**;
2. do ficheiro **`token.txt`** na pasta de onde o bot é arrancado.

`token.txt`, `master.key`, `.env` e `*.key` estão no `.gitignore` desta pasta. Para confirmares que o token não vai para o git:

```
git status --ignored TPF/tpf-lider
git check-ignore -v TPF/tpf-lider/token.txt
```

## 4. Compilar e arrancar o líder

```
cd TPF/tpf-lider
mvn -q package                          # gera target/tpf-lider.jar (com a JDA lá dentro)
java -jar target/tpf-lider.jar --teste  # valida os comandos sem ligar ao Discord
```

No PowerShell:
```
$env:DISCORD_TOKEN = "<token do bot>"
java -jar target/tpf-lider.jar [ID do servidor]    # sem ID: usa o único servidor onde o bot está
```

Ou com o token em `token.txt`:
```
executar.bat [ID do servidor]
```

Quando o bot fica online, regista os comandos **no servidor de teste** (*guild commands*, disponíveis logo) e escreve em `eventos-lider.txt`:

```
<instante>;ARRANQUE;bot Arquivo TPF online em «…»; 7 comandos registados; canal #arquivo; papel admin-arquivo
```

## 5. Comandos (Sprint E2-01: respostas provisórias)

| Comando | Quem | Validação |
|---|---|---|
| `/registar <texto>` | todos | texto não vazio |
| `/estado` | todos | — |
| `/config r <n>` | `admin-arquivo` | `n` inteiro ≥ 1 (`/config r abc` é recusado) |
| `/recuperar <de> <ate>` | todos | inteiros ≥ 1 e `de ≤ ate` (`/recuperar 20 10` é recusado) |
| `/replica-token` | `admin-arquivo` | — |
| `/replica-remover <id>` | `admin-arquivo` | letras, dígitos, `-` e `_`, até 32 |
| `/rajada <n> <intervalo-ms>` | `admin-arquivo` | `n` em 1..100000, intervalo em 0..60000 |

- Cada comando válido recebe uma resposta que **ecoa os argumentos**, por exemplo «`/recuperar` recebido: de=10, ate=20 — ainda não implementado».
- Os argumentos inválidos recebem uma mensagem de erro, visível só para quem enviou o comando.
- Um membro **sem** `admin-arquivo` que use um comando de administração recebe «⛔ `/config` é só para membros com o papel **admin-arquivo**».
- As mensagens escritas no canal **`#arquivo`** são detetadas e respondidas de forma provisória. As mensagens de bots e de outros canais são ignoradas.
- Os argumentos numéricos são do tipo texto no Discord, para que seja o **bot** a validá-los e a responder com uma mensagem clara.

Todos os eventos ficam em `eventos-lider.txt` (`instante;evento;detalhe`), com os eventos `ARRANQUE`, `COMANDO`, `INVALIDO`, `RECUSADO`, `MENSAGEM` e `ERRO`.
