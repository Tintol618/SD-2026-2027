package tpf;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

// Um registo do arquivo. Em registos.log ocupa uma linha: lsn;instante;autor;canal;texto em Base64
// (o texto pode ter ';' e mudanças de linha; o autor e o canal são nomes do Discord, sem ';' nem '\n').
record Registo(long lsn, long instante, String autor, String canal, String texto) {

    Registo {
        if (lsn < 1) throw new IllegalArgumentException("lsn tem de ser >= 1");
        autor = limpar(autor);
        canal = limpar(canal);
        if (texto == null) throw new IllegalArgumentException("texto nulo");
    }

    // ';' e mudanças de linha partiriam a linha do registos.log
    private static String limpar(String s) {
        return s == null ? "" : s.replace(';', '_').replace('\n', ' ').replace('\r', ' ');
    }

    String linha() {
        return lsn + ";" + instante + ";" + autor + ";" + canal + ";"
                + Base64.getEncoder().encodeToString(texto.getBytes(StandardCharsets.UTF_8));
    }

    static Registo deLinha(String linha) {
        String[] p = linha.split(";", -1);
        if (p.length != 5) throw new IllegalArgumentException("linha com " + p.length + " campos (esperava 5)");
        try {
            return new Registo(Long.parseLong(p[0]), Long.parseLong(p[1]), p[2], p[3],
                    new String(Base64.getDecoder().decode(p[4]), StandardCharsets.UTF_8));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("linha inválida: " + e.getMessage(), e);
        }
    }
}
