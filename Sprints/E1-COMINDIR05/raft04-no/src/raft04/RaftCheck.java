package raft04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

// RaftCheck <N> <pasta> [falhas.txt]
// Lê no-*, estado-*, hb-*, eventos-*, truncados-*, monitor-* e produtor.txt e escreve um relatório sobre a eleição.
// Os instantes comparados vêm de processos diferentes: só são comparáveis porque correm na mesma máquina.
public class RaftCheck {
    static final Pattern NODE_FILE = Pattern.compile("(no|hb|eventos|truncados|estado)-p(\\d+)-n(\\d+)\\.txt");
    static final Pattern KV = Pattern.compile("(\\w+)=([^;]*)");
    static final Pattern NUM = Pattern.compile("-?\\d+");
    static final long JANELA_MS = 3000;   // atraso máximo aceite entre o envio de um IN e a atribuição do seq

    static class Ev {
        long at;
        String type;
        int k, node;
        String detail;
        Map<String, String> kv = new HashMap<>();
    }

    static class Hb {
        long term, hb, at, lastSeq, lastTerm, n, bytes;
        int node;
    }

    static class Sent {
        long at;
        int k;
        String sensor;
        double temp;
    }

    static final StringBuilder out = new StringBuilder();
    static int n;

    static void p(String s) { out.append(s).append(System.lineSeparator()); }

    public static void main(String[] args) throws IOException {
        Path dir;
        try {
            n = Integer.parseInt(args[0]);
            dir = Paths.get(args[1]);
            if (n < 1) throw new IllegalArgumentException();
        } catch (RuntimeException e) {
            System.out.println("Uso: RaftCheck <N> <pasta> [falhas.txt]");
            return;
        }
        Path falhasPath = null;
        if (args.length > 2) {
            falhasPath = Paths.get(args[2]);
            if (!Files.exists(falhasPath)) falhasPath = dir.resolve(args[2]);
        }

        // ---------------- leitura
        Map<Integer, Map<Integer, List<SensorRecord>>> logs = new TreeMap<>();
        Map<Integer, Map<Integer, List<Ev>>> events = new TreeMap<>();
        Map<Integer, List<Hb>> hbs = new TreeMap<>();
        Map<Integer, Map<Integer, List<String[]>>> truncated = new TreeMap<>();
        Map<String, String> states = new TreeMap<>();
        try (var files = Files.list(dir)) {
            for (Path f : (Iterable<Path>) files::iterator) {
                Matcher m = NODE_FILE.matcher(f.getFileName().toString());
                if (!m.matches()) continue;
                String kind = m.group(1);
                int k = Integer.parseInt(m.group(2)), node = Integer.parseInt(m.group(3));
                List<String> lines = Files.readAllLines(f, StandardCharsets.UTF_8);
                lines.removeIf(String::isBlank);
                switch (kind) {
                    case "no": {
                        List<SensorRecord> l = new ArrayList<>();
                        for (String line : lines) l.add(SensorRecord.fromLine(line, n));
                        logs.computeIfAbsent(k, x -> new TreeMap<>()).put(node, l);
                        break;
                    }
                    case "hb":
                        for (String line : lines) {
                            String[] x = line.trim().split(";");
                            Hb h = new Hb();
                            h.term = Long.parseLong(x[0]); h.hb = Long.parseLong(x[1]); h.at = Long.parseLong(x[2]);
                            h.lastSeq = Long.parseLong(x[3]); h.lastTerm = Long.parseLong(x[4]); h.n = Long.parseLong(x[5]);
                            h.bytes = Long.parseLong(x[6]); h.node = node;
                            hbs.computeIfAbsent(k, y -> new ArrayList<>()).add(h);
                        }
                        break;
                    case "eventos": {
                        List<Ev> l = new ArrayList<>();
                        for (String line : lines) {
                            String[] x = line.trim().split(";", 4);
                            if (x.length < 3) continue;
                            Ev e = new Ev();
                            e.at = Long.parseLong(x[0]);
                            e.type = x[1];
                            e.k = k;
                            e.node = node;
                            e.detail = x.length > 3 ? x[3] : "";
                            Matcher kv = KV.matcher(e.detail);
                            while (kv.find()) e.kv.put(kv.group(1), kv.group(2));
                            l.add(e);
                        }
                        events.computeIfAbsent(k, y -> new TreeMap<>()).put(node, l);
                        break;
                    }
                    case "truncados": {
                        List<String[]> l = new ArrayList<>();
                        for (String line : lines) l.add(line.split(";", 2));
                        truncated.computeIfAbsent(k, y -> new TreeMap<>()).put(node, l);
                        break;
                    }
                    case "estado":
                        states.put("p" + k + "-n" + node, String.join("", lines).trim());
                        break;
                    default:
                }
            }
        }
        for (List<Hb> l : hbs.values()) l.sort(Comparator.comparingLong(h -> h.at));
        List<Sent> sent = new ArrayList<>();
        Path prod = dir.resolve("produtor.txt");
        if (Files.exists(prod)) {
            for (String line : Files.readAllLines(prod, StandardCharsets.UTF_8)) {
                String[] x = line.trim().split(";");
                if (x.length != 4) continue;
                Sent s = new Sent();
                s.at = Long.parseLong(x[0]);
                s.k = Integer.parseInt(x[1].substring(1));
                s.sensor = x[2];
                s.temp = Double.parseDouble(x[3]);
                sent.add(s);
            }
        }
        List<long[]> falhas = new ArrayList<>();   // {k, nó, instante}
        if (falhasPath != null && Files.exists(falhasPath)) {
            for (String line : Files.readAllLines(falhasPath, StandardCharsets.UTF_8)) {
                String[] x = line.trim().split(";");
                if (x.length == 3 && x[0].startsWith("p") && x[1].startsWith("n")) {
                    falhas.add(new long[] { Long.parseLong(x[0].substring(1)), Long.parseLong(x[1].substring(1)), Long.parseLong(x[2]) });
                }
            }
        }

        p("RaftCheck: N = " + n + ", pasta " + dir.toAbsolutePath()
                + (falhasPath != null ? ", falhas " + falhasPath.getFileName() + " (" + falhas.size() + ")" : ""));
        int problems = 0;

        for (int k = 0; k < n; k++) {
            Map<Integer, List<Ev>> evs = events.getOrDefault(k, Map.of());
            if (evs.isEmpty()) continue;
            List<Ev> all = new ArrayList<>();
            evs.values().forEach(all::addAll);
            all.sort(Comparator.comparingLong(e -> e.at));
            p("");
            p("================ Partição p" + k + " (nós " + evs.keySet() + ")");

            // ---------------- 1) termos
            p("1) Termos");
            TreeMap<Long, List<Ev>> byTerm = new TreeMap<>();
            for (Ev e : all) {
                if (e.type.equals("CANDIDATO") || e.type.equals("LIDER") || (e.type.equals("VOTO") && e.detail.endsWith("concedido"))) {
                    byTerm.computeIfAbsent(Long.parseLong(e.kv.get("termo")), x -> new ArrayList<>()).add(e);
                }
            }
            int split = 0, noLeader = 0, multi = 0;
            for (Map.Entry<Long, List<Ev>> t : byTerm.entrySet()) {
                List<Ev> l = t.getValue();
                List<Ev> cands = l.stream().filter(e -> e.type.equals("CANDIDATO")).collect(Collectors.toList());
                List<Ev> leaders = l.stream().filter(e -> e.type.equals("LIDER")).collect(Collectors.toList());
                Map<String, Long> votes = l.stream().filter(e -> e.type.equals("VOTO"))
                        .collect(Collectors.groupingBy(e -> e.kv.get("candidato"), TreeMap::new, Collectors.counting()));
                Set<Integer> leaderNodes = leaders.stream().map(e -> e.node).collect(Collectors.toCollection(TreeSet::new));
                String desc;
                if (leaderNodes.size() > 1) {
                    multi++;
                    desc = "DOIS OU MAIS LÍDERES: " + leaderNodes;
                } else if (leaderNodes.size() == 1) {
                    Ev le = leaders.get(0);
                    Ev c0 = cands.isEmpty() ? null : cands.get(0);
                    desc = "líder n" + le.node + " (votos " + le.kv.getOrDefault("votos", "?") + ")"
                            + (c0 != null ? String.format(", eleição %d ms", le.at - c0.at) : "");
                } else if (cands.stream().map(e -> e.node).distinct().count() > 1) {
                    split++;
                    desc = "VOTO DIVIDIDO (sem líder)";
                } else {
                    noLeader++;
                    desc = "sem líder (candidato sem maioria" + (votes.isEmpty() ? ", nenhum voto concedido" : "") + ")";
                }
                p(String.format("  termo %d: candidatos %s | votos concedidos por candidato %s | %s", t.getKey(),
                        cands.stream().map(e -> "n" + e.node).collect(Collectors.toList()), votes.isEmpty() ? "{}" : votes, desc));
            }
            p("  " + byTerm.size() + " termos | votos divididos " + split + " | outros termos sem líder " + noLeader);

            // ---------------- 2) segurança
            p("2) Segurança da eleição");
            int decreasing = 0, doubleVotes = 0, errors = 0;
            for (Map.Entry<Integer, List<Ev>> ne : evs.entrySet()) {
                long max = -1;
                Map<Long, Set<String>> granted = new HashMap<>();
                for (Ev e : ne.getValue()) {
                    long t = termOf(e);
                    if (t >= 0) {
                        if (t < max) {
                            if (decreasing++ < 5) p("  TERMO DIMINUIU em n" + ne.getKey() + ": " + max + " → " + t + " (" + e.type + " @" + e.at + ")");
                        }
                        max = Math.max(max, t);
                        if (e.type.equals("ARRANQUE")) max = t;   // conta a descida uma vez e continua a partir daqui
                    }
                    if (e.type.equals("VOTO") && e.detail.endsWith("concedido")) {
                        granted.computeIfAbsent(Long.parseLong(e.kv.get("termo")), x -> new TreeSet<>()).add(e.kv.get("candidato"));
                    }
                    if (e.type.equals("ERRO") && e.detail.contains("dois lideres")) errors++;
                }
                for (Map.Entry<Long, Set<String>> g : granted.entrySet()) {
                    if (g.getValue().size() > 1) {
                        doubleVotes++;
                        p("  VOTO DUPLO: n" + ne.getKey() + " concedeu o voto a " + g.getValue() + " no termo " + g.getKey());
                    }
                }
            }
            p("  termos com mais de um LIDER: " + multi + " | nós cujo termo diminuiu: " + decreasing
                    + " | votos duplos no mesmo termo: " + doubleVotes + " | ERRO «dois líderes»: " + errors);
            if (multi + decreasing + doubleVotes + errors > 0) problems++;

            // líderes simultâneos: intervalos [LIDER, DEMISSAO ou fim] de nós diferentes que se sobrepõem
            List<long[]> spans = new ArrayList<>();   // {nó, termo, início, fim}
            long end = all.get(all.size() - 1).at;
            for (Map.Entry<Integer, List<Ev>> ne : evs.entrySet()) {
                long[] open = null;
                for (Ev e : ne.getValue()) {
                    if (e.type.equals("LIDER")) open = new long[] { ne.getKey(), Long.parseLong(e.kv.get("termo")), e.at, -1 };
                    else if (open != null && e.type.equals("DEMISSAO")) {      // demitiu-se: julgou-se líder até aqui
                        open[3] = e.at;
                        spans.add(open);
                        open = null;
                    } else if (open != null && e.type.equals("ARRANQUE")) {   // falhou: foi líder até ao último LOTE
                        open[3] = lastHbOf(hbs.getOrDefault(k, List.of()), (int) open[0], open[1], open[2]);
                        spans.add(open);
                        open = null;
                    }
                }
                if (open != null) {   // ainda líder no fim do ensaio (ou falhou sem voltar): até ao último LOTE
                    open[3] = lastHbOf(hbs.getOrDefault(k, List.of()), (int) open[0], open[1], open[2]);
                    spans.add(open);
                }
            }
            for (int a = 0; a < spans.size(); a++) {
                for (int b = a + 1; b < spans.size(); b++) {
                    long[] x = spans.get(a), y = spans.get(b);
                    if (x[0] == y[0]) continue;
                    long from = Math.max(x[2], y[2]), to = Math.min(x[3], y[3]);
                    if (from < to) {
                        p(String.format("  LÍDERES SIMULTÂNEOS: n%d (termo %d) e n%d (termo %d) durante %.1f s", x[0], x[1], y[0], y[1], (to - from) / 1000.0));
                    }
                }
            }

            // ---------------- 3) tempo até ao primeiro líder
            long boot = all.stream().filter(e -> e.type.equals("ARRANQUE")).mapToLong(e -> e.at).min().orElse(-1);
            Optional<Ev> first = all.stream().filter(e -> e.type.equals("LIDER")).findFirst();
            p("3) Primeiro líder: " + (first.isPresent() && boot >= 0
                    ? String.format("n%d no termo %s, %.2f s depois do arranque do primeiro nó (%d candidaturas antes)", first.get().node,
                    first.get().kv.get("termo"), (first.get().at - boot) / 1000.0,
                    all.stream().filter(e -> e.type.equals("CANDIDATO") && e.at <= first.get().at).count())
                    : "nenhum"));

            // registo de referência: o ficheiro mais longo; união de todos os registos dos nós da partição
            Map<Integer, List<SensorRecord>> nodeLogs = logs.getOrDefault(k, Map.of());
            List<SensorRecord> ref = nodeLogs.values().stream().max(Comparator.comparingInt(List::size)).orElse(List.of());
            List<SensorRecord> union = new ArrayList<>();
            Set<String> ids = new HashSet<>();
            for (List<SensorRecord> l : nodeLogs.values())
                for (SensorRecord r : l) if (ids.add(r.getSeq() + "#" + r.getTerm())) union.add(r);

            // ---------------- 4) falhas
            final int kp = k;
            List<Sent> sentK = sent.stream().filter(s -> s.k == kp).collect(Collectors.toList());
            final int kk = kp;
            List<long[]> falhasK = falhas.stream().filter(f -> f[0] == kk).collect(Collectors.toList());
            if (!falhasK.isEmpty()) {
                p("4) Indisponibilidade após cada falha (até ao primeiro LOTE de um termo superior)");
                for (long[] f : falhasK) {
                    long tf = f[2];
                    long termAt = hbs.getOrDefault(k, List.of()).stream().filter(h -> h.at <= tf).mapToLong(h -> h.term).max().orElse(0);
                    Optional<Hb> next = hbs.getOrDefault(k, List.of()).stream().filter(h -> h.at > tf && h.term > termAt).findFirst();
                    if (next.isPresent()) {
                        long tn = next.get().at;
                        long during = sentK.stream().filter(s -> s.at >= tf && s.at < tn).count();
                        p(String.format("  falha de n%d @%d (termo %d): novo líder n%d no termo %d, indisponibilidade %.2f s | leituras enviadas nesse intervalo: %d",
                                f[1], tf, termAt, next.get().node, next.get().term, (tn - tf) / 1000.0, during));
                    } else {
                        p("  falha de n" + f[1] + " @" + tf + " (termo " + termAt + "): nenhum LOTE de termo superior depois dela");
                    }
                }
            }

            // ---------------- 5) leituras perdidas
            List<Sent> lost = lostReadings(sentK, union);
            p("5) Leituras perdidas: " + lost.size() + " de " + sentK.size() + " enviadas para p" + k
                    + (lost.isEmpty() ? "" : " | ex.: " + lost.stream().limit(6).map(s -> s.sensor + " " + s.temp + " @" + s.at).collect(Collectors.joining(", "))));

            // ---------------- 6) truncagens
            List<Ev> truncs = all.stream().filter(e -> e.type.equals("TRUNCADO")).collect(Collectors.toList());
            p("6) Truncagens: " + truncs.size());
            for (Ev e : truncs) {
                List<String[]> dropped = truncated.getOrDefault(k, Map.of()).getOrDefault(e.node, List.of());
                int total = 0, elsewhere = 0;
                for (String[] d : dropped) {
                    if (Math.abs(Long.parseLong(d[0]) - e.at) > 1000) continue;   // linhas desta truncagem
                    total++;
                    SensorRecord r = SensorRecord.fromLine(d[1], n);
                    if (existsElsewhere(r, ref)) elsewhere++;
                }
                p(String.format("  n%d @%d: de seq %s, %s descartados (termos %s) | destes, %d existem no registo final com outro seq/termo",
                        e.node, e.at, e.kv.get("de"), e.kv.get("descartados"), e.kv.get("termos"), elsewhere));
            }

            // ---------------- 7) coincidência final
            if (!nodeLogs.isEmpty()) {
                long min = nodeLogs.values().stream().mapToLong(List::size).min().orElse(0);
                List<Long> diff = new ArrayList<>();
                for (long s = 1; s <= min; s++) {
                    Set<String> lines = new HashSet<>();
                    for (List<SensorRecord> l : nodeLogs.values()) lines.add(l.get((int) s - 1).toLine());
                    if (lines.size() > 1) diff.add(s);
                }
                p("7) Coincidência final: últimos seq " + nodeLogs.entrySet().stream().map(x -> "n" + x.getKey() + "=" + x.getValue().size()).collect(Collectors.joining(", "))
                        + " | seq divergentes até " + min + ": " + diff.size() + (diff.isEmpty() ? " (OK)" : " " + diff.subList(0, Math.min(10, diff.size()))));
                if (!diff.isEmpty()) problems++;
            }
        }

        // ---------------- monitores
        p("");
        p("Monitores do produtor e do processador");
        try (var files = Files.list(dir)) {
            for (Path f : (Iterable<Path>) files::iterator) {
                String name = f.getFileName().toString();
                if (!name.startsWith("monitor-")) continue;
                Map<String, Long> c = Files.readAllLines(f, StandardCharsets.UTF_8).stream().map(l -> l.split(";"))
                        .filter(x -> x.length > 2).collect(Collectors.groupingBy(x -> x[1] + " " + x[2], TreeMap::new, Collectors.counting()));
                c.keySet().removeIf(x -> x.startsWith("RECEBIDO"));
                p("  " + name + ": " + c);
            }
        }
        p("Estado persistente final: " + states);
        p(problems == 0 ? "RESULTADO: OK" : "RESULTADO: " + problems + " problema(s) encontrado(s)");

        System.out.print(out);
        Files.writeString(dir.resolve("raft-relatorio.txt"), out.toString(), StandardCharsets.UTF_8);
    }


    // termo referido por um evento de nó (ou -1)
    static long termOf(Ev e) {
        switch (e.type) {
            case "TERMO": {
                String[] x = e.detail.split("->");
                return Long.parseLong(x[1].trim());
            }
            case "ARRANQUE": case "CANDIDATO": case "VOTO": case "LIDER": case "NOVO_LIDER": case "DEMISSAO":
                return e.kv.containsKey("termo") ? Long.parseLong(e.kv.get("termo")) : -1;
            default:
                return -1;
        }
    }

    // último LOTE publicado pelo nó nesse termo (fim de uma liderança sem DEMISSAO registada)
    static long lastHbOf(List<Hb> hbs, int node, long term, long dflt) {
        long last = -1;
        for (Hb h : hbs) if (h.node == node && h.term == term) last = h.at;
        return last > 0 ? last : dflt;
    }

    static long epochMs(String ts) {
        return LocalDateTime.parse(ts).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

    // emparelha cada leitura enviada com um registo do mesmo sensor e temperatura, atribuído até JANELA_MS depois
    static List<Sent> lostReadings(List<Sent> sent, List<SensorRecord> records) {
        List<long[]> avail = new ArrayList<>();   // {instante, índice}
        List<SensorRecord> recs = new ArrayList<>(records);
        for (int x = 0; x < recs.size(); x++) avail.add(new long[] { epochMs(recs.get(x).getTimestamp()), x });
        avail.sort(Comparator.comparingLong(a -> a[0]));
        boolean[] used = new boolean[recs.size()];
        List<Sent> sorted = new ArrayList<>(sent);
        sorted.sort(Comparator.comparingLong(s -> s.at));
        List<Sent> lost = new ArrayList<>();
        for (Sent s : sorted) {
            boolean found = false;
            for (long[] a : avail) {
                if (a[0] < s.at - 200) continue;
                if (a[0] > s.at + JANELA_MS) break;
                SensorRecord r = recs.get((int) a[1]);
                if (!used[(int) a[1]] && r.getSensorId().equals(s.sensor) && r.getTemperature() == s.temp) {
                    used[(int) a[1]] = true;
                    found = true;
                    break;
                }
            }
            if (!found) lost.add(s);
        }
        return lost;
    }

    // a mesma leitura (sensor, temperatura, instante próximo) existe no registo de referência com outro seq ou termo?
    static boolean existsElsewhere(SensorRecord r, List<SensorRecord> ref) {
        long t = epochMs(r.getTimestamp());
        for (SensorRecord x : ref) {
            if (x.getSensorId().equals(r.getSensorId()) && x.getTemperature() == r.getTemperature()
                    && Math.abs(epochMs(x.getTimestamp()) - t) < JANELA_MS && !x.sameEntry(r)) return true;
        }
        return false;
    }
}
