package tpf;

import java.util.LinkedHashMap;
import java.util.Map;

// Verificação da validação dos comandos sem ligar ao Discord: LeaderBot --teste
// (não se usam bibliotecas de testes, porque o trabalho só permite JDA, Java-WebSocket e o JDK)
final class ComandosTeste {
    private static int ok, falhas;

    private ComandosTeste() { }

    static boolean correr() {
        aceita("registar", null, "texto", "temperatura 21,5 no laboratório");
        aceita("estado", null);
        aceita("config", "r", "n", "3");
        aceita("recuperar", null, "de", "10", "ate", "20");
        aceita("recuperar", null, "de", "7", "ate", "7");
        aceita("replica-token", null);
        aceita("replica-remover", null, "id", "r1");
        aceita("rajada", null, "n", "500", "intervalo-ms", "0");

        recusa("registar", null, "texto", "   ");
        recusa("config", "r", "n", "abc");          // critério de aceitação: /config r abc
        recusa("config", "r", "n", "0");
        recusa("config", "r", "n", "-2");
        recusa("recuperar", null, "de", "20", "ate", "10");   // critério de aceitação: /recuperar 20 10
        recusa("recuperar", null, "de", "x", "ate", "10");
        recusa("recuperar", null, "de", "0", "ate", "10");
        recusa("replica-remover", null, "id", "r 1; rm");
        recusa("rajada", null, "n", "0", "intervalo-ms", "10");
        recusa("rajada", null, "n", "10", "intervalo-ms", "-1");
        recusa("rajada", null, "n", "1e3", "intervalo-ms", "10");
        recusa("inexistente", null);

        // a JDA valida nomes, descrições e opções ao construir os comandos (o que o Discord aceitaria)
        try {
            var cmds = LeaderBot.comandos();
            System.out.println("OK   " + cmds.size() + " comandos construídos: "
                    + cmds.stream().map(c -> "/" + c.getName()).collect(java.util.stream.Collectors.joining(" ")));
            if (cmds.size() == 7) ok++; else falhas++;
        } catch (IllegalArgumentException e) {
            System.out.println("FALHA a construir os comandos: " + e.getMessage());
            falhas++;
        }

        System.out.println(ok + " casos OK, " + falhas + " falhas");
        return falhas == 0;
    }

    private static Map<String, String> opcoes(String... kv) {
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i + 1 < kv.length; i += 2) m.put(kv[i], kv[i + 1]);
        return m;
    }

    private static void aceita(String nome, String sub, String... kv) {
        try {
            String r = LeaderBot.tratar(nome, sub, opcoes(kv));
            System.out.println("OK   aceite  /" + nome + (sub != null ? " " + sub : "") + " " + opcoes(kv) + " → " + r);
            ok++;
        } catch (IllegalArgumentException e) {
            System.out.println("FALHA esperava aceitar /" + nome + " " + opcoes(kv) + ": " + e.getMessage());
            falhas++;
        }
    }

    private static void recusa(String nome, String sub, String... kv) {
        try {
            String r = LeaderBot.tratar(nome, sub, opcoes(kv));
            System.out.println("FALHA esperava recusar /" + nome + " " + opcoes(kv) + " mas respondeu: " + r);
            falhas++;
        } catch (IllegalArgumentException e) {
            System.out.println("OK   recusado /" + nome + (sub != null ? " " + sub : "") + " " + opcoes(kv) + " → " + e.getMessage());
            ok++;
        }
    }
}
