package tpf;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

// Ferramentas sem Discord, para os ensaios do sprint:
//   LeaderBot --verificar            abre todos os chunks de selados/ e compara-os com registos.log
//   LeaderBot --simular <n> <ms>     injeta n registos pelo mesmo caminho de /registar e espera pela selagem
final class Verificar {
    private Verificar() { }

    static boolean verificar() throws IOException {
        if (!Files.exists(Wire.MASTER_KEY)) {
            System.out.println("Não existe " + Wire.MASTER_KEY + ": nada para verificar.");
            return false;
        }
        byte[] mk = MasterKey.ler(Wire.MASTER_KEY);
        Map<Long, Registo> log = new HashMap<>();
        for (Registo r : Arquivo.lerLog(Wire.REGISTOS, null)) log.put(r.lsn(), r);
        System.out.println("registos.log: " + log.size() + " registos");

        List<Path> bins = new ArrayList<>();
        if (Files.isDirectory(Wire.SELADOS)) {
            try (DirectoryStream<Path> ds = Files.newDirectoryStream(Wire.SELADOS, "*.bin")) {
                ds.forEach(bins::add);
            }
        }
        TreeMap<Long, long[]> intervalos = new TreeMap<>();   // primeiro LSN → {último LSN}
        int ok = 0, falhas = 0;
        for (Path p : bins) {
            String id = p.getFileName().toString().replace(".bin", "");
            String curto = id.substring(0, Math.min(12, id.length())) + "…";
            List<String> erros = new ArrayList<>();
            byte[] conteudo = Files.readAllBytes(p);

            // 1. nome = SHA-256 do conteúdo
            if (!ChunkCrypto.sha256Hex(conteudo).equals(id)) erros.add("nome ≠ SHA-256 do conteúdo");
            // 2. abrir (hash, versão da chave, GCM)
            List<Registo> rs = null;
            int claroBytes = -1;
            try {
                byte[] claro = ChunkCrypto.open(mk, Sealed.ler(Wire.SELADOS, id));
                claroBytes = claro.length;
                rs = Chunk.ler(claro);
            } catch (Exception e) {
                erros.add("open falhou: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            }
            if (rs != null) {
                // 3. padding: o chunk em claro tem tamanho múltiplo de PAD_BYTES
                if (claroBytes % Wire.PAD_BYTES != 0) erros.add("em claro " + claroBytes + " bytes, não é múltiplo de " + Wire.PAD_BYTES);
                // 4. os registos são exatamente os de registos.log (LSN, instante, autor, canal, texto)
                int iguais = 0;
                for (Registo r : rs) {
                    if (r.equals(log.get(r.lsn()))) iguais++;
                    else erros.add("LSN " + r.lsn() + " diferente de registos.log");
                }
                for (int i = 1; i < rs.size(); i++) {
                    if (rs.get(i).lsn() != rs.get(i - 1).lsn() + 1) erros.add("LSN não consecutivos dentro do chunk");
                }
                // 5. nenhum texto registado aparece em claro no .bin (o equivalente a um grep)
                for (Registo r : rs) {
                    byte[] t = r.texto().getBytes(StandardCharsets.UTF_8);
                    if (t.length >= 6 && contem(conteudo, t)) erros.add("texto do LSN " + r.lsn() + " em claro no .bin");
                }
                long a = rs.get(0).lsn(), b = rs.get(rs.size() - 1).lsn();
                intervalos.put(a, new long[]{b});
                System.out.printf("%s %s lsn=%d..%d registos=%d emClaro=%d cifrado=%d iguais ao log=%d/%d%n",
                        erros.isEmpty() ? "OK   " : "FALHA", curto, a, b, rs.size(), claroBytes, conteudo.length, iguais, rs.size());
            } else {
                System.out.println("FALHA " + curto);
            }
            for (String e : erros) System.out.println("      " + e);
            if (erros.isEmpty()) ok++; else falhas++;
        }

        // os chunks cobrem 1..ultimoSelado sem sobreposições nem buracos
        long esperado = 1;
        boolean contiguo = true;
        for (Map.Entry<Long, long[]> e : intervalos.entrySet()) {
            if (e.getKey() != esperado) {
                System.out.println("FALHA intervalo: esperava um chunk a começar em " + esperado + " e o seguinte começa em " + e.getKey());
                contiguo = false;
            }
            esperado = e.getValue()[0] + 1;
        }
        long porSelar = log.size() - (esperado - 1);
        System.out.println(ok + " chunks OK, " + falhas + " com falhas; LSN selados 1.." + (esperado - 1)
                + (contiguo ? " (contíguos)" : " (COM BURACOS)") + "; por selar: " + Math.max(0, porSelar));
        return falhas == 0 && contiguo;
    }

    static boolean contem(byte[] palheiro, byte[] agulha) {
        outer:
        for (int i = 0; i + agulha.length <= palheiro.length; i++) {
            for (int j = 0; j < agulha.length; j++) {
                if (palheiro[i + j] != agulha[j]) continue outer;
            }
            return true;
        }
        return false;
    }

    // texto dos registos sintéticos da /rajada (≈ 300 bytes, para 500 registos darem vários chunks de 64 KB)
    static String sintetico(long k, long n) {
        StringBuilder sb = new StringBuilder("registo sintético " + k + "/" + n + " ·");
        String[] palavras = {"temperatura", "humidade", "pressão", "vento", "chuva", "luz", "ruído", "co2"};
        int i = (int) (k % palavras.length);
        while (sb.length() < 300) {
            sb.append(' ').append(palavras[i % palavras.length]).append('=').append((k * 31 + i * 7) % 1000);
            i++;
        }
        return sb.toString();
    }

    static void simular(long n, long intervaloMs) throws Exception {
        Arquivo a;
        try {
            a = Arquivo.abrir(LeaderBot::evento);
        } catch (Exception e) {
            LeaderBot.evento("ERRO", "arranque recusado: " + e.getMessage());
            System.exit(4);
            return;
        }
        for (long k = 1; k <= n; k++) {
            a.registar("simulador", "local", sintetico(k, n));
            if (intervaloMs > 0) Thread.sleep(intervaloMs);
        }
        System.out.println("à espera da selagem por tempo (" + Wire.SELAGEM_MS + " ms)…");
        Thread.sleep(Wire.SELAGEM_MS + 1000);
        a.fechar();
    }
}
