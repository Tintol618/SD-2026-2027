package rep04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

// HeartbeatCheck <N> <pasta> [falhas.txt]
// Lê hb-p*.txt, lider-p*.txt e monitor-*.txt de uma pasta e escreve um relatório sobre o detetor e o lote.
// Todos os instantes comparados vêm de processos diferentes: só são comparáveis porque correm na mesma máquina.
public class HeartbeatCheck {
    static final Pattern HB = Pattern.compile("hb-p(\\d+)\\.txt");
    static final Pattern LEADER = Pattern.compile("lider-p(\\d+)\\.txt");
    static final Pattern MONITOR = Pattern.compile("monitor-(.+)\\.txt");
    static final Pattern KV = Pattern.compile("(\\w+)=([^;]*)");

    // uma linha de hb-p<k>.txt
    static class Hb {
        long inc, hb, at, lastSeq, bytes;
        int n;
    }

    // uma linha de monitor-<id>.txt: instante;evento;p<k>;detalhe
    static class Ev {
        long at;
        String type;
        int k;
        Map<String, String> kv = new HashMap<>();
        String raw;
    }

    static final StringBuilder out = new StringBuilder();

    static void p(String s) {
        out.append(s).append(System.lineSeparator());
    }

    public static void main(String[] args) throws IOException {
        int n;
        Path dir;
        try {
            n = Integer.parseInt(args[0]);
            dir = Paths.get(args[1]);
            if (n < 1) throw new IllegalArgumentException();
        } catch (RuntimeException e) {
            System.out.println("Uso: HeartbeatCheck <N> <pasta> [falhas.txt]");
            return;
        }
        Path falhasPath = null;
        if (args.length > 2) {
            falhasPath = Paths.get(args[2]);
            if (!Files.exists(falhasPath) && Files.exists(dir.resolve(args[2]))) falhasPath = dir.resolve(args[2]);
        }

        // ---------- leitura ----------
        Map<Integer, List<Hb>> hbs = new TreeMap<>();
        Map<Integer, Map<Long, SensorRecord>> recs = new TreeMap<>();
        Map<String, List<Ev>> monitors = new TreeMap<>();
        List<Path> files;
        try (Stream<Path> s = Files.list(dir)) {
            files = new ArrayList<>();
            s.forEach(files::add);
        }
        for (Path f : files) {
            String name = f.getFileName().toString();
            Matcher m;
            if ((m = HB.matcher(name)).matches()) {
                List<Hb> l = new ArrayList<>();
                for (String line : Files.readAllLines(f, StandardCharsets.UTF_8)) {
                    if (line.isBlank()) continue;
                    String[] x = line.trim().split(";");
                    if (x.length != 6) { System.out.println("linha inválida em " + name + ": " + line); continue; }
                    Hb h = new Hb();
                    h.inc = Long.parseLong(x[0]); h.hb = Long.parseLong(x[1]); h.at = Long.parseLong(x[2]);
                    h.lastSeq = Long.parseLong(x[3]); h.n = Integer.parseInt(x[4]); h.bytes = Long.parseLong(x[5]);
                    l.add(h);
                }
                hbs.put(Integer.parseInt(m.group(1)), l);
            } else if ((m = LEADER.matcher(name)).matches()) {
                Map<Long, SensorRecord> l = new TreeMap<>();
                for (String line : Files.readAllLines(f, StandardCharsets.UTF_8)) {
                    if (line.isBlank()) continue;
                    SensorRecord r = SensorRecord.fromLine(line, n);
                    l.put(r.getSeq(), r);
                }
                recs.put(Integer.parseInt(m.group(1)), l);
            } else if ((m = MONITOR.matcher(name)).matches()) {
                List<Ev> l = new ArrayList<>();
                for (String line : Files.readAllLines(f, StandardCharsets.UTF_8)) {
                    if (line.isBlank()) continue;
                    String[] x = line.trim().split(";", 4);
                    if (x.length < 3 || !x[2].startsWith("p")) continue;
                    Ev e = new Ev();
                    e.raw = line.trim();
                    e.at = Long.parseLong(x[0]);
                    e.type = x[1];
                    e.k = Integer.parseInt(x[2].substring(1));
                    if (x.length == 4) {
                        Matcher kv = KV.matcher(x[3]);
                        while (kv.find()) e.kv.put(kv.group(1), kv.group(2));
                    }
                    l.add(e);
                }
                monitors.put(m.group(1), l);
            }
        }
        List<long[]> falhas = new ArrayList<>(); // {k, instante}
        if (falhasPath != null) {
            for (String line : Files.readAllLines(falhasPath, StandardCharsets.UTF_8)) {
                String[] x = line.trim().split(";");
                if (x.length == 2 && x[0].startsWith("p")) {
                    falhas.add(new long[] { Long.parseLong(x[0].substring(1)), Long.parseLong(x[1].trim()) });
                }
            }
            falhas.sort(Comparator.comparingLong(a -> a[1]));
        }

        p("HeartbeatCheck: N = " + n + ", pasta " + dir.toAbsolutePath()
                + (falhasPath != null ? ", falhas " + falhasPath + " (" + falhas.size() + ")" : ""));

        // ---------- 1) lotes por partição e incarnação ----------
        p("");
        p("1) Lotes por partição e incarnação");
        long totalLotes = 0, totalRegsLote = 0, bytesNaoVazios = 0, regsNaoVazios = 0;
        for (int k = 0; k < n; k++) {
            List<Hb> l = hbs.getOrDefault(k, List.of());
            if (l.isEmpty()) { p("  p" + k + ": hb-p" + k + ".txt não existe ou está vazio"); continue; }
            Map<Long, List<Hb>> byInc = new LinkedHashMap<>();
            for (Hb h : l) byInc.computeIfAbsent(h.inc, x -> new ArrayList<>()).add(h);
            for (Map.Entry<Long, List<Hb>> e : byInc.entrySet()) {
                List<Hb> hs = e.getValue();
                int empty = 0, max = 0;
                long regs = 0, gapSum = 0, gapMax = 0, gaps = 0, missing = 0;
                for (int i = 0; i < hs.size(); i++) {
                    Hb h = hs.get(i);
                    if (h.n == 0) empty++;
                    else { bytesNaoVazios += h.bytes; regsNaoVazios += h.n; }
                    regs += h.n;
                    max = Math.max(max, h.n);
                    if (i > 0) {
                        long g = h.at - hs.get(i - 1).at;
                        gapSum += g; gapMax = Math.max(gapMax, g); gaps++;
                        missing += h.hb - hs.get(i - 1).hb - 1;
                    }
                }
                totalLotes += hs.size();
                totalRegsLote += regs;
                long dur = hs.get(hs.size() - 1).at - hs.get(0).at;
                p(String.format("  p%d inc=%d: %d lotes (hb %d..%d%s) em %.1f s | vazios %d (%.1f %%) | registos %d, por lote média %.2f máx %d"
                                + " (não vazios: média %.2f) | intervalo entre hb: média %s ms, máx %s ms",
                        k, e.getKey(), hs.size(), hs.get(0).hb, hs.get(hs.size() - 1).hb,
                        missing > 0 ? ", " + missing + " em falta no ficheiro" : "", dur / 1000.0,
                        empty, 100.0 * empty / hs.size(), regs, (double) regs / hs.size(), max,
                        hs.size() - empty > 0 ? (double) regs / (hs.size() - empty) : 0.0,
                        gaps > 0 ? String.format("%.0f", (double) gapSum / gaps) : "-", gaps > 0 ? String.valueOf(gapMax) : "-"));
            }
        }

        // ---------- 2) suspeitas por participante e partição ----------
        p("");
        p("2) Suspeitas por participante e partição");
        int reais = 0, falsas = 0;
        for (Map.Entry<String, List<Ev>> me : monitors.entrySet()) {
            String who = me.getKey();
            List<Ev> evs = me.getValue();
            for (int i = 0; i < evs.size(); i++) {
                Ev s = evs.get(i);
                if (!s.type.equals("SUSPEITA")) continue;
                // incarnação que o participante observava no momento da suspeita
                Long inc = null;
                for (int j = i - 1; j >= 0; j--) {
                    Ev e = evs.get(j);
                    if (e.k == s.k && e.type.equals("RECEBIDO")) { inc = Long.parseLong(e.kv.get("inc")); break; }
                }
                // o que se seguiu: RETOMA com o mesmo inc (falsa) ou com outro inc / nada (falha real)
                Ev retoma = null;
                for (int j = i + 1; j < evs.size(); j++) {
                    Ev e = evs.get(j);
                    if (e.k == s.k && e.type.equals("RETOMA")) { retoma = e; break; }
                }
                Hb lastPub = null;
                for (Hb h : hbs.getOrDefault(s.k, List.of())) if (h.at <= s.at) lastPub = h;
                boolean sameIncLater = false;
                if (inc != null) {
                    for (Hb h : hbs.getOrDefault(s.k, List.of())) if (h.inc == inc && h.at > s.at) { sameIncLater = true; break; }
                }
                String cls;
                if (inc == null) {
                    cls = "sem líder observado (nunca recebeu um LOTE desta partição)";
                } else if (retoma != null && Long.parseLong(retoma.kv.get("inc")) == inc) {
                    cls = "FALSA SUSPEITA (RETOMA com o mesmo inc " + (retoma.at - s.at) + " ms depois)";
                    falsas++;
                } else {
                    cls = "FALHA REAL" + (retoma != null ? " (RETOMA com inc " + retoma.kv.get("inc") + " " + (retoma.at - s.at) + " ms depois)" : " (sem RETOMA)");
                    reais++;
                }
                String consist = inc != null && sameIncLater && !cls.startsWith("FALSA")
                        ? " [atenção: hb-p" + s.k + ".txt tem heartbeats da mesma incarnação depois da suspeita]" : "";
                p("  " + who + " p" + s.k + " @" + s.at + ": " + cls
                        + " | silêncio desde o último heartbeat publicado: " + (lastPub != null ? (s.at - lastPub.at) + " ms (hb " + lastPub.hb + ")" : "-")
                        + " | silêncio local: " + s.kv.getOrDefault("silencio_ms", "?") + " ms" + consist);
            }
        }
        p("  total: " + reais + " falhas reais, " + falsas + " falsas suspeitas");

        // ---------- 3) tempos de deteção ----------
        if (falhasPath != null) {
            p("");
            p("3) Tempo de deteção de cada falha provocada (SUSPEITA − instante da falha)");
            List<Double> all = new ArrayList<>();
            for (int f = 0; f < falhas.size(); f++) {
                int k = (int) falhas.get(f)[0];
                long t = falhas.get(f)[1];
                long next = Long.MAX_VALUE;
                for (int g = f + 1; g < falhas.size(); g++) if (falhas.get(g)[0] == k) { next = falhas.get(g)[1]; break; }
                Hb lastPub = null;
                for (Hb h : hbs.getOrDefault(k, List.of())) if (h.at <= t) lastPub = h;
                StringBuilder sb = new StringBuilder("  p" + k + " @" + t
                        + (lastPub != null ? " (último hb publicado " + (t - lastPub.at) + " ms antes)" : "") + ":");
                for (Map.Entry<String, List<Ev>> me : monitors.entrySet()) {
                    boolean follows = false;
                    for (Ev e : me.getValue()) if (e.k == k && e.type.equals("RECEBIDO")) { follows = true; break; }
                    if (!follows) continue;
                    Ev s = null;
                    for (Ev e : me.getValue()) if (e.k == k && e.type.equals("SUSPEITA") && e.at > t && e.at < next) { s = e; break; }
                    if (s == null) sb.append(" ").append(me.getKey()).append(" sem suspeita;");
                    else {
                        double d = (s.at - t) / 1000.0;
                        all.add(d);
                        sb.append(String.format(" %s %.2f s;", me.getKey(), d));
                    }
                }
                p(sb.toString());
            }
            if (!all.isEmpty()) {
                double min = Collections.min(all), max = Collections.max(all), avg = all.stream().mapToDouble(x -> x).average().orElse(0);
                double lo = (Wire.FATOR_TIMEOUT - 1) * Wire.PERIODO_HB_MS / 1000.0;
                double hi = (Wire.FATOR_TIMEOUT * Wire.PERIODO_HB_MS + Wire.VERIFICACAO_MS) / 1000.0;
                long fora = all.stream().filter(x -> x <= lo || x > hi).count();
                p(String.format("  mínimo %.2f s | médio %.2f s | máximo %.2f s | intervalo previsto ]%.0f s, %.0f s] (Δ = %d ms, fator %d): %d fora",
                        min, avg, max, lo, hi, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT, fora));
            }
        }

        // ---------- 4) latência de lote ----------
        p("");
        p("4) Latência de lote (instante do RECEBIDO que contém o registo − instante do registo no líder)");
        for (int k = 0; k < n; k++) {
            Map<Long, SensorRecord> rs = recs.getOrDefault(k, Map.of());
            if (rs.isEmpty()) { p("  p" + k + ": sem registos"); continue; }
            Map<Long, Long> at = new HashMap<>();
            for (SensorRecord r : rs.values()) at.put(r.getSeq(), epochMs(r.getTimestamp()));
            // espera no líder: instante de publicação do lote − instante do registo
            long waitSum = 0, waitMax = 0, waitN = 0;
            for (Hb h : hbs.getOrDefault(k, List.of())) {
                for (long q = h.lastSeq - h.n + 1; q <= h.lastSeq && h.n > 0; q++) {
                    Long a = at.get(q);
                    if (a == null) continue;
                    long w = h.at - a;
                    waitSum += w; waitMax = Math.max(waitMax, w); waitN++;
                }
            }
            StringBuilder sb = new StringBuilder(String.format("  p%d: espera no líder média %s ms, máx %s ms (%d registos)",
                    k, waitN > 0 ? String.format("%.0f", (double) waitSum / waitN) : "-", waitN > 0 ? String.valueOf(waitMax) : "-", waitN));
            long sum = 0, max = 0, cnt = 0;
            for (Map.Entry<String, List<Ev>> me : monitors.entrySet()) {
                long s2 = 0, m2 = 0, c2 = 0;
                for (Ev e : me.getValue()) {
                    if (e.k != k || !e.type.equals("RECEBIDO")) continue;
                    String range = e.kv.get("seq");
                    if (range == null || range.equals("-")) continue;
                    String[] ab = range.split("\\.\\.");
                    for (long q = Long.parseLong(ab[0]); q <= Long.parseLong(ab[1]); q++) {
                        Long a = at.get(q);
                        if (a == null) continue;
                        long lat = e.at - a;
                        s2 += lat; m2 = Math.max(m2, lat); c2++;
                    }
                }
                if (c2 > 0) {
                    sb.append(String.format(" | %s média %.0f ms máx %d ms", me.getKey(), (double) s2 / c2, m2));
                    sum += s2; max = Math.max(max, m2); cnt += c2;
                }
            }
            if (cnt > 0) sb.append(String.format(" || p%d total: média %.0f ms, máx %d ms (%d receções)", k, (double) sum / cnt, max, cnt));
            p(sb.toString());
        }

        // ---------- 5) HB_PERDIDO e RECUPERACAO ----------
        p("");
        p("5) Eventos HB_PERDIDO, RECUPERACAO e REINICIO");
        for (Map.Entry<String, List<Ev>> me : monitors.entrySet()) {
            int[] lost = new int[n], rec = new int[n], rest = new int[n];
            for (Ev e : me.getValue()) {
                if (e.k < 0 || e.k >= n) continue;
                if (e.type.equals("HB_PERDIDO")) lost[e.k]++;
                if (e.type.equals("RECUPERACAO")) rec[e.k]++;
                if (e.type.equals("REINICIO")) rest[e.k]++;
            }
            StringBuilder sb = new StringBuilder("  " + me.getKey() + ":");
            int tl = 0, tr = 0;
            for (int k = 0; k < n; k++) {
                sb.append(" p").append(k).append(" HB_PERDIDO ").append(lost[k]).append(" RECUPERACAO ").append(rec[k])
                        .append(" REINICIO ").append(rest[k]).append(k < n - 1 ? " |" : "");
                tl += lost[k]; tr += rec[k];
            }
            sb.append(" || total HB_PERDIDO ").append(tl).append(", RECUPERACAO ").append(tr);
            p(sb.toString());
        }

        // ---------- 6) bytes por registo e mensagens ----------
        p("");
        p("6) Custo do transporte");
        long regBytes = 0, regCount = 0;
        for (Map.Entry<Integer, Map<Long, SensorRecord>> e : recs.entrySet()) {
            for (SensorRecord r : e.getValue().values()) {
                String reg = "REG;127.0.0.1;" + (Wire.PORTO_BASE + e.getKey()) + ";" + r.toLine();
                regBytes += reg.getBytes(StandardCharsets.UTF_8).length;
                regCount++;
            }
        }
        if (regsNaoVazios > 0) {
            p(String.format("  bytes por registo: em LOTE %.1f (incluindo a parte do cabeçalho; %d registos em lotes não vazios)",
                    (double) bytesNaoVazios / regsNaoVazios, regsNaoVazios)
                    + (regCount > 0 ? String.format(" | REG individual da COMINDIR03 %.1f", (double) regBytes / regCount) : ""));
        }
        p("  mensagens no tópico de dados: COMINDIR03 (um REG por registo) " + regCount
                + " | COMINDIR04 (LOTE) " + totalLotes + " (" + totalRegsLote + " registos transportados)");

        System.out.print(out);
        Path report = dir.resolve("heartbeat-relatorio.txt");
        Files.writeString(report, out.toString(), StandardCharsets.UTF_8);
        System.out.println("Relatório escrito em " + report.toAbsolutePath());
    }

    static long epochMs(String ts) {
        return LocalDateTime.parse(ts).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }
}
