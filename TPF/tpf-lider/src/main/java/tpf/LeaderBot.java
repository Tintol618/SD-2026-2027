package tpf;

import net.dv8tion.jda.api.JDA;
import net.dv8tion.jda.api.JDABuilder;
import net.dv8tion.jda.api.entities.Guild;
import net.dv8tion.jda.api.entities.Member;
import net.dv8tion.jda.api.events.interaction.command.SlashCommandInteractionEvent;
import net.dv8tion.jda.api.events.message.MessageReceivedEvent;
import net.dv8tion.jda.api.hooks.ListenerAdapter;
import net.dv8tion.jda.api.interactions.commands.OptionMapping;
import net.dv8tion.jda.api.interactions.commands.OptionType;
import net.dv8tion.jda.api.interactions.commands.build.Commands;
import net.dv8tion.jda.api.interactions.commands.build.SlashCommandData;
import net.dv8tion.jda.api.interactions.commands.build.SubcommandData;
import net.dv8tion.jda.api.requests.GatewayIntent;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

// Líder do arquivo (Sprint E2-01): bot Discord que recebe e encaminha todos os comandos da especificação,
// valida os argumentos e responde com uma mensagem provisória. Nenhuma funcionalidade do arquivo está
// implementada ainda (registo, cifra, réplicas e recuperação chegam nos sprints seguintes).
//
// Uso: LeaderBot <guildId>        (token em DISCORD_TOKEN ou no ficheiro token.txt; ver README.md)
//      LeaderBot --teste          (verifica a validação dos comandos sem ligar ao Discord)
public class LeaderBot extends ListenerAdapter {
    static final String CANAL = System.getProperty("tpf.canal", "arquivo");
    static final String PAPEL_ADMIN = System.getProperty("tpf.papel", "admin-arquivo");
    static final Set<String> SO_ADMIN = Set.of("config", "replica-token", "replica-remover", "rajada");
    static final Path EVENTOS = Paths.get("eventos-lider.txt");

    public static void main(String[] args) throws InterruptedException {
        if (args.length == 1 && args[0].equals("--teste")) {
            System.exit(ComandosTeste.correr() ? 0 : 1);
        }
        String token = lerToken();
        String guild = args.length > 0 ? args[0] : System.getenv("TPF_GUILD_ID");
        if (token == null || guild == null || guild.isBlank()) {
            System.out.println("Uso: LeaderBot <guildId>");
            System.out.println("  O token do bot vem da variável de ambiente DISCORD_TOKEN ou do ficheiro token.txt");
            System.out.println("  (nunca no repositório). O guildId é o ID do servidor de teste (ou TPF_GUILD_ID).");
            System.exit(2);
        }
        long guildId;
        try {
            guildId = Long.parseLong(guild.trim());
        } catch (NumberFormatException e) {
            System.out.println("guildId inválido: " + guild);
            System.exit(2);
            return;
        }

        // GUILD_MESSAGES para receber as mensagens dos canais; MESSAGE_CONTENT (intent privilegiado,
        // ativado no Developer Portal) para ler o seu conteúdo
        JDA jda = JDABuilder.createDefault(token, GatewayIntent.GUILD_MESSAGES, GatewayIntent.MESSAGE_CONTENT)
                .addEventListeners(new LeaderBot())
                .build();
        jda.awaitReady();

        Guild g = jda.getGuildById(guildId);
        if (g == null) {
            System.out.println("O bot não está no servidor " + guildId + ". Usa o URL de convite do README.");
            jda.shutdown();
            System.exit(3);
            return;
        }
        // comandos de servidor (guild commands): ficam disponíveis de imediato, ao contrário dos globais
        g.updateCommands().addCommands(comandos()).queue(
                cmds -> evento("ARRANQUE", "bot " + jda.getSelfUser().getName() + " online em «" + g.getName()
                        + "»; " + cmds.size() + " comandos registados; canal #" + CANAL + "; papel " + PAPEL_ADMIN),
                erro -> evento("ERRO", "registo dos comandos falhou: " + erro));
    }

    static String lerToken() {
        String t = System.getenv("DISCORD_TOKEN");
        if (t != null && !t.isBlank()) return t.trim();
        Path f = Paths.get("token.txt");
        try {
            if (Files.exists(f)) {
                String s = Files.readString(f, StandardCharsets.UTF_8).trim();
                if (!s.isEmpty()) return s;
            }
        } catch (IOException e) {
            System.out.println("Não foi possível ler token.txt: " + e.getMessage());
        }
        return null;
    }

    // os 7 comandos da especificação; os argumentos numéricos são texto para que o bot (e não só o
    // cliente Discord) valide e recuse valores como "abc" com uma mensagem clara
    static List<SlashCommandData> comandos() {
        return List.of(
                Commands.slash("registar", "Regista um texto no arquivo")
                        .addOption(OptionType.STRING, "texto", "texto a registar", true),
                Commands.slash("estado", "Estado do arquivo: réplicas, ocupação e chunks"),
                Commands.slash("config", "Configuração do arquivo (só admin-arquivo)")
                        .addSubcommands(new SubcommandData("r", "Fator de replicação R")
                                .addOption(OptionType.STRING, "n", "novo valor de R (inteiro >= 1)", true)),
                Commands.slash("recuperar", "Recupera os registos com LSN entre de e ate")
                        .addOption(OptionType.STRING, "de", "primeiro LSN (inteiro >= 1)", true)
                        .addOption(OptionType.STRING, "ate", "último LSN (inteiro >= de)", true),
                Commands.slash("replica-token", "Gera um token de uso único para registar uma réplica (só admin-arquivo)"),
                Commands.slash("replica-remover", "Remove uma réplica do arquivo (só admin-arquivo)")
                        .addOption(OptionType.STRING, "id", "identificador da réplica", true),
                Commands.slash("rajada", "Injeta n registos sintéticos (só admin-arquivo)")
                        .addOption(OptionType.STRING, "n", "número de registos (1..100000)", true)
                        .addOption(OptionType.STRING, "intervalo-ms", "intervalo entre registos em ms (0..60000)", true));
    }

    @Override
    public void onSlashCommandInteraction(SlashCommandInteractionEvent event) {
        String nome = event.getName();
        String sub = event.getSubcommandName();
        Map<String, String> opcoes = new LinkedHashMap<>();
        for (OptionMapping o : event.getOptions()) opcoes.put(o.getName(), o.getAsString());
        String quem = event.getUser().getName();

        if (SO_ADMIN.contains(nome) && !isAdmin(event.getMember())) {
            evento("RECUSADO", "/" + nome + " de " + quem + ": sem o papel " + PAPEL_ADMIN);
            event.reply("⛔ `/" + nome + "` é só para membros com o papel **" + PAPEL_ADMIN + "**.").setEphemeral(true).queue();
            return;
        }
        try {
            String resposta = tratar(nome, sub, opcoes);
            evento("COMANDO", "/" + nome + (sub != null ? " " + sub : "") + " " + opcoes + " de " + quem);
            event.reply(resposta).queue();
        } catch (IllegalArgumentException e) {
            evento("INVALIDO", "/" + nome + (sub != null ? " " + sub : "") + " " + opcoes + " de " + quem + ": " + e.getMessage());
            event.reply("❌ " + e.getMessage()).setEphemeral(true).queue();
        }
    }

    // mensagens enviadas ao canal #arquivo (as de bots, incluindo as do próprio, e as de outros canais são ignoradas)
    @Override
    public void onMessageReceived(MessageReceivedEvent event) {
        if (!event.isFromGuild() || event.getAuthor().isBot() || event.isWebhookMessage()) return;
        if (!event.getChannel().getName().equals(CANAL)) return;
        String texto = event.getMessage().getContentRaw();
        if (texto.isBlank()) return;   // só anexos ou embeds: nada para registar
        evento("MENSAGEM", "#" + CANAL + " de " + event.getAuthor().getName() + ": " + resumo(texto, 80));
        event.getMessage().reply("📝 Mensagem detetada em #" + CANAL + ": «" + resumo(texto, 200)
                + "» — registo ainda não implementado").mentionRepliedUser(false).queue();
    }

    static boolean isAdmin(Member m) {
        return m != null && m.getRoles().stream().anyMatch(r -> r.getName().equalsIgnoreCase(PAPEL_ADMIN));
    }

    // ---------------------------------------------------------------- validação (sem Discord)

    // valida os argumentos e devolve a resposta provisória; lança IllegalArgumentException com a mensagem de erro
    static String tratar(String nome, String sub, Map<String, String> o) {
        switch (nome) {
            case "registar": {
                String texto = obrigatorio(o, "texto").trim();
                if (texto.isEmpty()) throw new IllegalArgumentException("`/registar`: o texto não pode estar vazio.");
                return "`/registar` recebido: texto=«" + resumo(texto, 200) + "» — ainda não implementado";
            }
            case "estado":
                return "`/estado` recebido — ainda não implementado";
            case "config": {
                if (!"r".equals(sub)) throw new IllegalArgumentException("`/config`: subcomando desconhecido (usa `/config r <n>`).");
                long r = inteiro(o, "n", 1, 1000);
                return "`/config r` recebido: R=" + r + " — ainda não implementado";
            }
            case "recuperar": {
                long de = inteiro(o, "de", 1, Long.MAX_VALUE);
                long ate = inteiro(o, "ate", 1, Long.MAX_VALUE);
                if (de > ate) throw new IllegalArgumentException("`/recuperar`: de=" + de + " é maior do que ate=" + ate + ".");
                return "`/recuperar` recebido: de=" + de + ", ate=" + ate + " — ainda não implementado";
            }
            case "replica-token":
                return "`/replica-token` recebido — ainda não implementado";
            case "replica-remover": {
                String id = obrigatorio(o, "id").trim();
                if (!id.matches("[A-Za-z0-9_-]{1,32}")) {
                    throw new IllegalArgumentException("`/replica-remover`: id=«" + resumo(id, 40) + "» inválido (letras, dígitos, - e _, até 32).");
                }
                return "`/replica-remover` recebido: id=" + id + " — ainda não implementado";
            }
            case "rajada": {
                long n = inteiro(o, "n", 1, 100_000);
                long intervalo = inteiro(o, "intervalo-ms", 0, 60_000);
                return "`/rajada` recebido: n=" + n + ", intervaloMs=" + intervalo + " — ainda não implementado";
            }
            default:
                throw new IllegalArgumentException("comando desconhecido: /" + nome);
        }
    }

    static String obrigatorio(Map<String, String> o, String nome) {
        String v = o.get(nome);
        if (v == null) throw new IllegalArgumentException("falta o argumento `" + nome + "`.");
        return v;
    }

    static long inteiro(Map<String, String> o, String nome, long min, long max) {
        String v = obrigatorio(o, nome).trim();
        long x;
        try {
            x = Long.parseLong(v);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("`" + nome + "`=«" + resumo(v, 40) + "» não é um número inteiro.");
        }
        if (x < min || x > max) {
            throw new IllegalArgumentException("`" + nome + "`=" + x + " fora do intervalo ["
                    + min + ", " + (max == Long.MAX_VALUE ? "∞" : String.valueOf(max)) + "].");
        }
        return x;
    }

    static String resumo(String s, int max) {
        String t = s.replace('\n', ' ');
        return t.length() <= max ? t : t.substring(0, max - 1) + "…";
    }

    // eventos na consola e em eventos-lider.txt: instante;evento;detalhe
    static synchronized void evento(String tipo, String detalhe) {
        String linha = System.currentTimeMillis() + ";" + tipo + ";" + detalhe;
        System.out.println(linha);
        try {
            Files.writeString(EVENTOS, linha + System.lineSeparator(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            System.out.println("Não foi possível escrever em " + EVENTOS + ": " + e.getMessage());
        }
    }
}
