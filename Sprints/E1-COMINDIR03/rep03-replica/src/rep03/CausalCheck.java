package rep03;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class CausalCheck {
    static final Pattern LEADER = Pattern.compile("lider-p(\\d+)\\.txt");
    static final int DETALHE_MAX = 30; // até este número de pares entre partições, mostra-os um a um
    static final int EXEMPLOS = 5;    // violações mostradas por tipo

    // um registo e a partição de onde veio
    static class Ev {
        final int k;
        final SensorRecord r;
        final VectorClock v;

        Ev(int k, SensorRecord r) {
            this.k = k;
            this.r = r;
            this.v = r.getVector();
        }

        String name() {
            return "p" + k + "#" + r.getSeq();
        }
    }

    public static void main(String[] args) throws IOException {
        int n;
        Path dir;
        try {
            n = Integer.parseInt(args[0]);
            dir = Paths.get(args[1]);
            if (n < 1) throw new IllegalArgumentException();
        } catch (RuntimeException e) {
            System.out.println("Uso: CausalCheck <N> <pasta>");
            return;
        }

        Map<Integer, Path> leaders = new TreeMap<>();
        try (Stream<Path> files = Files.list(dir)) {
            files.forEach(f -> {
                Matcher m = LEADER.matcher(f.getFileName().toString());
                if (m.matches()) leaders.put(Integer.parseInt(m.group(1)), f);
            });
        }

        System.out.println("CausalCheck: N = " + n + ", pasta " + dir.toAbsolutePath());
        int problems = 0;

        // 1) leitura dos ficheiros e verificação de V[k] = seq
        List<List<Ev>> parts = new ArrayList<>();
        for (int k = 0; k < n; k++) {
            List<Ev> evs = new ArrayList<>();
            parts.add(evs);
            Path p = leaders.get(k);
            if (p == null) {
                System.out.println("p" + k + ": lider-p" + k + ".txt não existe");
                continue;
            }
            int invalid = 0, wrong = 0, alarms = 0;
            String example = "";
            for (String line : Files.readAllLines(p, StandardCharsets.UTF_8)) {
                if (line.isBlank()) continue;
                SensorRecord r;
                try {
                    r = SensorRecord.fromLine(line, n);
                } catch (IllegalArgumentException e) {
                    if (invalid++ < EXEMPLOS) System.out.println("p" + k + ": linha inválida: " + line + " (" + e.getMessage() + ")");
                    continue;
                }
                if (r.getVector().get(k) != r.getSeq()) {
                    if (wrong++ == 0) example = " (ex.: seq " + r.getSeq() + " com V = " + r.getVector() + ")";
                }
                if (r.getSensorId().startsWith("ALARM-")) alarms++;
                evs.add(new Ev(k, r));
            }
            problems += invalid + (wrong > 0 ? 1 : 0);
            System.out.println("p" + k + ": " + evs.size() + " registos (" + alarms + " alarmes)"
                    + (invalid > 0 ? " | " + invalid + " linhas inválidas" : "")
                    + " | V[" + k + "] = seq: " + (wrong == 0 ? "OK" : "ERRO em " + wrong + " linhas" + example));
        }
        for (Map.Entry<Integer, Path> e : leaders.entrySet()) {
            if (e.getKey() >= n) {
                problems++;
                System.out.println("ATENÇÃO: " + e.getValue().getFileName() + " não pertence a nenhuma partição de 0.." + (n - 1));
            }
        }

        // 2) pares de registos de partições diferentes
        long before = 0, after = 0, concurrent = 0, equal = 0, concDistinctL = 0;
        long total = 0;
        for (List<Ev> evs : parts) total += evs.size();
        long pairs = 0;
        for (int i = 0; i < n; i++) for (int j = i + 1; j < n; j++) pairs += (long) parts.get(i).size() * parts.get(j).size();
        boolean detail = pairs <= DETALHE_MAX;
        if (detail && pairs > 0) {
            System.out.println();
            System.out.println("Pares entre partições:");
        }
        for (int i = 0; i < n; i++) {
            for (int j = i + 1; j < n; j++) {
                for (Ev a : parts.get(i)) {
                    for (Ev b : parts.get(j)) {
                        VectorClock.Order o = a.v.compare(b.v);
                        String rel;
                        switch (o) {
                            case BEFORE: before++; rel = a.name() + " → " + b.name(); break;
                            case AFTER: after++; rel = b.name() + " → " + a.name(); break;
                            case CONCURRENT:
                                concurrent++;
                                if (a.r.getLamport() != b.r.getLamport()) concDistinctL++;
                                rel = a.name() + " ∥ " + b.name();
                                break;
                            default: equal++; rel = a.name() + " = " + b.name() + " (vetores iguais: anomalia)";
                        }
                        if (detail) {
                            System.out.println("  " + rel + "   V " + a.v + " / " + b.v
                                    + "   L " + a.r.getLamport() + " / " + b.r.getLamport());
                        }
                    }
                }
            }
        }
        if (equal > 0) problems++;

        // 3) condição de relógio: para todo o par com a → b (mesma partição ou não), L(a) < L(b)
        long checked = 0, violations = 0;
        List<Ev> all = new ArrayList<>();
        for (List<Ev> evs : parts) all.addAll(evs);
        for (int x = 0; x < all.size(); x++) {
            for (int y = x + 1; y < all.size(); y++) {
                Ev a = all.get(x), b = all.get(y);
                VectorClock.Order o = a.v.compare(b.v);
                if (o == VectorClock.Order.AFTER) {
                    Ev t = a; a = b; b = t;
                } else if (o != VectorClock.Order.BEFORE) {
                    continue;
                }
                checked++;
                if (a.r.getLamport() >= b.r.getLamport()) {
                    if (violations++ < EXEMPLOS) {
                        System.out.println("VIOLAÇÃO da condição de relógio: " + a.name() + " → " + b.name()
                                + " mas L = " + a.r.getLamport() + " >= " + b.r.getLamport());
                    }
                }
            }
        }
        if (violations > 0) problems++;

        // 4) vista ordenada por (L, k)
        List<Ev> view = new ArrayList<>(all);
        view.sort(Comparator.comparingLong((Ev e) -> e.r.getLamport()).thenComparingInt(e -> e.k)
                .thenComparingLong(e -> e.r.getSeq()));
        List<String> out = new ArrayList<>();
        for (Ev e : view) out.add("p" + e.k + ";" + e.r.toLine());
        Path vista = dir.resolve("vista-lamport.txt");
        Files.write(vista, out, StandardCharsets.UTF_8);

        // 5) custo dos carimbos: tamanho de um REG com e sem L e V (endereço 127.0.0.1, porto 7000 + k)
        long withStamps = 0, withoutStamps = 0;
        for (Ev e : all) {
            String reg = Wire.reg("127.0.0.1", Wire.PORTO_BASE + e.k, e.r);
            String stamps = ";" + e.r.getLamport() + ";" + e.r.getVector();
            withStamps += reg.getBytes(StandardCharsets.UTF_8).length;
            withoutStamps += reg.getBytes(StandardCharsets.UTF_8).length - stamps.getBytes(StandardCharsets.UTF_8).length;
        }

        long causal = before + after;
        System.out.println();
        System.out.println("Registos: " + total + " | pares entre partições: " + pairs);
        System.out.println("  a → b (partição menor antes): " + before + " | b → a: " + after + " | causais: " + causal
                + String.format(" (%.1f %%)", 100.0 * causal / Math.max(pairs, 1)));
        System.out.println("  concorrentes: " + concurrent + String.format(" (%.1f %%)", 100.0 * concurrent / Math.max(pairs, 1))
                + (equal > 0 ? " | vetores iguais: " + equal : ""));
        System.out.println("  concorrentes com L distinto (ordenados pelo relógio de Lamport): " + concDistinctL
                + String.format(" (%.1f %% dos concorrentes)", 100.0 * concDistinctL / Math.max(concurrent, 1)));
        System.out.println("Condição de relógio: " + checked + " pares a → b verificados (todas as partições), "
                + (violations == 0 ? "0 violações (OK)" : violations + " VIOLAÇÕES"));
        if (total > 0) {
            System.out.printf("Tamanho médio de um REG: %.1f bytes sem carimbos | %.1f bytes com carimbos (+%.1f)%n",
                    (double) withoutStamps / total, (double) withStamps / total, (double) (withStamps - withoutStamps) / total);
        }
        System.out.println("Vista por (L, k) escrita em " + vista.toAbsolutePath());
        System.out.println(problems == 0 ? "RESULTADO: OK" : "RESULTADO: " + problems + " problema(s) encontrado(s)");
    }
}
