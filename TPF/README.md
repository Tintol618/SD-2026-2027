# TPF · Arquivo distribuído zero-knowledge

Trabalho prático final (Épico 2). Esta pasta tem os projetos do trabalho:

| Projeto | Estado |
|---|---|
| `tpf-lider/` | **Sprint E2-01**: `LeaderBot` com os 7 comandos, validação dos argumentos e respostas provisórias · **Sprint E2-02**: registo durável com LSN, chunks e cifra (secção 6) |
| `tpf-replica/` | sprints seguintes |

Requisitos: **Java 17+** e **Maven 3.9+**. A única dependência é a **JDA** (API do Discord), descarregada pelo Maven. A cifra usa só o JDK (`javax.crypto`).

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

`token.txt`, `master.key`, `.env` e `*.key` estão no `.gitignore` desta pasta. Os dados gerados pelo líder (`registos.log`, `selados/` e `eventos-*.txt`) também estão. Para confirmares que o token não vai para o git:

```
git status --ignored TPF/tpf-lider
git check-ignore -v TPF/tpf-lider/token.txt
```

## 4. Compilar e arrancar o líder

```
cd TPF/tpf-lider
mvn -q package                          # gera target/tpf-lider.jar (com a JDA lá dentro)
java -jar target/tpf-lider.jar --teste  # testes sem Discord: comandos, chunk e cifra
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

## 5. Comandos

| Comando | Quem | Validação | Estado |
|---|---|---|---|
| `/registar <texto>` | todos | texto não vazio | **E2-02**: responde com o LSN atribuído |
| `/estado` | todos | — | provisório |
| `/config r <n>` | `admin-arquivo` | `n` inteiro ≥ 1 (`/config r abc` é recusado) | provisório |
| `/recuperar <de> <ate>` | todos | inteiros ≥ 1 e `de ≤ ate` (`/recuperar 20 10` é recusado) | provisório |
| `/replica-token` | `admin-arquivo` | — | provisório |
| `/replica-remover <id>` | `admin-arquivo` | letras, dígitos, `-` e `_`, até 32 | provisório |
| `/rajada <n> <intervalo-ms>` | `admin-arquivo` | `n` em 1..100000, intervalo em 0..60000 | **E2-02**: injeta n registos sintéticos |

- Os comandos provisórios recebem uma resposta que **ecoa os argumentos**, por exemplo «`/recuperar` recebido: de=10, ate=20 — ainda não implementado».
- Os argumentos inválidos recebem uma mensagem de erro, visível só para quem enviou o comando.
- Um membro **sem** `admin-arquivo` que use um comando de administração recebe «⛔ `/config` é só para membros com o papel **admin-arquivo**».
- As mensagens escritas no canal **`#arquivo`** são registadas pelo mesmo caminho de `/registar` e o bot responde com o LSN. As mensagens de bots e de outros canais são ignoradas.
- Os argumentos numéricos são do tipo texto no Discord, para que seja o **bot** a validá-los e a responder com uma mensagem clara.

Todos os eventos ficam em `eventos-lider.txt` (`instante;evento;detalhe`): `ARRANQUE`, `COMANDO`, `INVALIDO`, `RECUSADO` e `ERRO` (E2-01), e `CHAVE`, `ARQUIVO`, `REGISTO`, `SELADO`, `RAJADA` e `AVISO` (E2-02). O texto registado nunca aparece nos eventos, só o seu tamanho.

## 6. Caminho de escrita local (Sprint E2-02)

```
/registar, #arquivo, /rajada ─► Arquivo.registar (sincronizado)
   ├─ LSN = último + 1
   ├─ linha em registos.log + fsync (FileChannel.force(true)) ─► só agora o bot responde com o LSN
   └─ chunk aberto ─► selado a 64 KB ou 5 s depois do 1.º registo
         └─ serialização + padding (múltiplo de 4096) ─► HKDF-SHA256 + AES-256-GCM ─► selados/<chunkId>.bin + .hdr
```

| Classe | Papel |
|---|---|
| `Wire` | parâmetros: `CHUNK_MAX_BYTES`, `PAD_BYTES`, `SELAGEM_MS` (e os da cifra) |
| `MasterKey` | gera a `master.key` no primeiro arranque; recusa-a se não tiver 32 bytes |
| `Registo` | `lsn`, `instante`, `autor`, `canal`, `texto`; a linha do `registos.log` |
| `Arquivo` | o caminho de escrita: LSN, `registos.log`, chunk aberto, temporizador e selagem |
| `Chunk` | chunk aberto, serialização com padding e leitura inversa |
| `ChunkCrypto` | HKDF, AES-256-GCM, `seal` e `open` |
| `Sealed` | chunk selado e os ficheiros `.bin` e `.hdr` |
| `Verificar` | `--verificar` e `--simular` |
| `ChunkCryptoTeste` | testes do registo, do chunk e da cifra (`--teste`) |

### Parâmetros

| Parâmetro | Omissão | Alterar com |
|---|---|---|
| `CHUNK_MAX_BYTES` | 65536 | `-Dtpf.chunkMaxBytes=<bytes>` |
| `PAD_BYTES` | 4096 | `-Dtpf.padBytes=<bytes>` |
| `SELAGEM_MS` | 5000 | `-Dtpf.selagemMs=<ms>` |
| caminho da master key | `master.key` | `-Dtpf.masterKey=<ficheiro>` |

As opções `-D` vão antes de `-jar`: `java -Dtpf.selagemMs=2000 -jar target/tpf-lider.jar`.

### `master.key`

- No primeiro arranque, se não existir, é gerada com 32 bytes de `SecureRandom`. Fica com leitura só para o dono: `0400` em POSIX; no Windows, uma ACL com uma só entrada, a do dono (`icacls master.key` mostra `(R,D,WDAC)`).
- Se existir mas não tiver 32 bytes, o líder **recusa arrancar**.
- No arranque, o líder abre todos os chunks de `selados/`. Se algum não abrir (chunk alterado ou `master.key` errada), **recusa arrancar** e indica o `--verificar`.
- **Não a apagues**: uma chave nova não abre os chunks já selados.

### `registos.log`

Uma linha por registo, com o texto em Base64 (pode ter `;` e mudanças de linha):

```
lsn;instante;autor;canal;texto em Base64
1;1795618550544;ana;arquivo;b2zDoSBtdW5kbw==
```

- No arranque, a numeração continua a partir do último LSN, e o líder confirma que os LSN são 1, 2, 3… sem saltos.
- Uma última linha incompleta (falha a meio de uma escrita, antes do `fsync`, portanto nunca confirmada) é cortada (`AVISO`).
- Os registos com LSN maior do que o último já selado voltam para o chunk aberto e são selados de novo: uma falha entre o `fsync` e a selagem não perde nada.

### Formato binário do chunk em claro

Inteiros em big-endian, texto em UTF-8:

| Campo | Bytes |
|---|---|
| `"TPFC"` (magic) | 4 |
| formato = 1 | 1 |
| número de registos *n* | 4 (int32) |
| *n* × registo: | |
|   `lsn` | 8 (int64) |
|   `instante` (ms desde 1970) | 8 (int64) |
|   comprimento do autor + autor | 2 (uint16) + *a* |
|   comprimento do canal + canal | 2 (uint16) + *c* |
|   comprimento do texto + texto | 4 (int32) + *t* |
| padding `0x00` até ao múltiplo seguinte de `PAD_BYTES` | 0 a 4095 |

- Cada registo ocupa `24 + a + c + t` bytes e o cabeçalho 9.
- **Selagem por tamanho:** se o registo seguinte fizesse o chunk passar de `CHUNK_MAX_BYTES`, o chunk aberto é selado **antes** e o registo abre um chunk novo. Assim o chunk em claro nunca passa de 64 KB (65536 já é múltiplo de 4096). Um registo sozinho maior do que o limite forma um chunk só dele.
- **Selagem por tempo:** um `ScheduledExecutorService` sela o chunk `SELAGEM_MS` depois do seu primeiro registo.
- **Leitura inversa** (`Chunk.ler`): lê os *n* registos e ignora o que sobra, que é padding.
- Depois de selado, o chunk é imutável (`acrescentar` lança exceção).

### Cifra e ficheiros em `selados/`

```
salt  = 32 bytes aleatórios        nonce = 12 bytes aleatórios        (novos em cada chunk)
DEK   = HKDF-SHA256(MK, salt, info = "tpf-chunk-v1")
        extração  PRK = HMAC-SHA256(chave = salt, MK)
        expansão  DEK = HMAC-SHA256(chave = PRK, "tpf-chunk-v1" | 0x01)        (um só bloco, 32 bytes)
aad   = "tpf-v1" | keyVersion (int32 big-endian) | salt | nonce               keyVersion = 1
.bin  = AES-256-GCM(DEK, nonce, aad, chunk em claro com padding) = texto cifrado | tag (16 bytes)
chunkId = SHA-256 do .bin, em hexadecimal
```

| Ficheiro | Conteúdo |
|---|---|
| `selados/<chunkId>.bin` | só os bytes cifrados: texto cifrado seguido da tag GCM (tamanho = chunk em claro + 16) |
| `selados/<chunkId>.hdr` | uma linha `keyVersion;salt;nonce`: `keyVersion` em decimal, `salt` e `nonce` em Base64. Exemplo: `1;3ohLGgrToHIwu7l3pcgsP4Q6mxh8ZV7Jjo0YsLBGogg=;BWHwqTR7RWccgnhw` |

- Os dois ficheiros são escritos num temporário, sincronizados e movidos de forma atómica.
- `open` verifica por esta ordem: SHA-256 do `.bin` contra o `chunkId`, `keyVersion` e autenticação GCM. Qualquer falha lança exceção e nada é devolvido.
- Evento `SELADO;chunkId=…;lsn=a..b;registos=n;tamanho=<em claro>;cifrado=<.bin>;motivo=tamanho|tempo|paragem`.

### Ferramentas sem Discord

```
java -jar target/tpf-lider.jar --teste            # 21 casos de comandos + 24 testes de registo, chunk e cifra
java -jar target/tpf-lider.jar --verificar        # abre cada chunk de selados/ e compara-o com registos.log
java -jar target/tpf-lider.jar --simular 500 0    # 500 registos sintéticos pelo caminho de escrita, sem Discord
```

O `--verificar` confirma, para cada chunk:
- que o nome é o SHA-256 do conteúdo;
- que abre;
- que o tamanho em claro é múltiplo de `PAD_BYTES`;
- que os registos são exatamente os do `registos.log`;
- que nenhum texto registado aparece em claro no `.bin`;
- que os chunks cobrem os LSN 1..*k* sem buracos.
