package rep02;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class PartitionCheck {
    static final Pattern LEADER = Pattern.compile("lider-p(\\d+)\\.txt");
    static final Pattern REPLICA = Pattern.compile("replica-(.+)-p(\\d+)\\.txt");

    public static void main(String[] args) throws IOException {
        int n, r;
        Path dir;
        try {
            n = Integer.parseInt(args[0]);
            dir = Paths.get(args[1]);
            r = args.length > 2 ? Integer.parseInt(args[2]) : 2; // fator de replicação esperado
        } catch (RuntimeException e) {
            System.out.println("Uso: PartitionCheck <N> <pasta> [R]   (R = 2 por omissão)");
            return;
        }
        Partitioner partitioner = new Partitioner(n);

        Map<Integer, Path> leaders = new TreeMap<>();
        Map<Integer, Map<String, Path>> replicas = new TreeMap<>();
        try (Stream<Path> files = Files.list(dir)) {
            files.forEach(f -> {
                String name = f.getFileName().toString();
                Matcher m = LEADER.matcher(name);
                if (m.matches()) {
                    leaders.put(Integer.parseInt(m.group(1)), f);
                    return;
                }
                m = REPLICA.matcher(name);
                if (m.matches()) {
                    replicas.computeIfAbsent(Integer.parseInt(m.group(2)), x -> new TreeMap<>()).put(m.group(1), f);
                }
            });
        }

        System.out.println("PartitionCheck: N = " + n + ", R esperado = " + r + ", pasta " + dir.toAbsolutePath());
        int problems = 0, converged = 0, replicaFiles = 0;
        long sum = 0;
        for (int k = 0; k < n; k++) {
            System.out.println();
            System.out.println("=== Partição p" + k);
            Path lp = leaders.get(k);
            List<String> leader = lp == null ? List.of() : lines(lp);
            if (lp == null) {
                System.out.println("  lider-p" + k + ".txt: não existe");
            }
            List<SensorRecord> recs = parse(leader, "lider-p" + k + ".txt");

            // todos os registos do líder são de sensores desta partição
            long wrong = 0;
            String example = "";
            for (SensorRecord rec : recs) {
                if (partitioner.of(rec.getSensorId()) != k) {
                    if (wrong++ == 0) example = " (ex.: " + rec.getSensorId() + " → p" + partitioner.of(rec.getSensorId()) + ")";
                }
            }
            if (wrong == 0) {
                System.out.println("  partição dos sensores: OK (todos com of(sensor) == " + k + ")");
            } else {
                problems++;
                System.out.println("  partição dos sensores: ERRO, " + wrong + " registos de outra partição" + example);
            }

            String seqLeader = checkSeq(recs);
            if (!seqLeader.startsWith("OK")) problems++;
            System.out.println("  sequência do líder: " + seqLeader);

            Map<String, Path> reps = replicas.getOrDefault(k, Map.of());
            boolean rOk = reps.size() == r;
            if (!rOk) problems++;
            System.out.println("  réplicas que guardam p" + k + ": " + reps.size() + (rOk ? " (OK, R = " + r + ")" : " (ATENÇÃO: esperado R = " + r + ")"));
            for (Map.Entry<String, Path> e : reps.entrySet()) {
                replicaFiles++;
                List<String> rep = lines(e.getValue());
                String name = e.getValue().getFileName().toString();
                String seqRep = checkSeq(parse(rep, name));
                String state;
                int common = 0;
                while (common < rep.size() && common < leader.size() && rep.get(common).equals(leader.get(common))) {
                    common++;
                }
                if (common == rep.size() && common == leader.size()) {
                    state = "IGUAL ao líder";
                    converged++;
                } else if (common == rep.size()) {
                    state = "PREFIXO do líder (faltam " + (leader.size() - rep.size()) + " registos)";
                } else {
                    state = "DIVERGENTE: difere do líder na linha " + (common + 1);
                    problems++;
                }
                if (!seqRep.startsWith("OK")) problems++;
                System.out.println("    réplica " + e.getKey() + " (" + name + "): " + rep.size() + " registos, " + state + " | sequência: " + seqRep);
            }

            Set<String> sensors = new TreeSet<>();
            for (SensorRecord rec : recs) sensors.add(rec.getSensorId());
            System.out.println("  registos: " + recs.size() + " | sensores distintos: " + sensors.size() + " " + sensors);
            sum += recs.size();
        }

        // total do sistema: todos os ficheiros de líder encontrados, incluindo partições fora de 0..N-1
        long total = 0;
        for (Map.Entry<Integer, Path> e : leaders.entrySet()) {
            int count = lines(e.getValue()).size();
            total += count;
            if (e.getKey() >= n) {
                problems++;
                System.out.println();
                System.out.println("ATENÇÃO: " + e.getValue().getFileName() + " (" + count + " registos) não pertence a nenhuma partição de 0.." + (n - 1));
            }
        }
        System.out.println();
        System.out.println("Total de registos do sistema: " + total + " | soma das partições p0..p" + (n - 1) + ": " + sum
                + (total == sum ? " (OK)" : " (DIFERENTE)"));
        System.out.println("Réplicas iguais ao líder: " + converged + " de " + replicaFiles);
        System.out.println(problems == 0 ? "RESULTADO: OK" : "RESULTADO: " + problems + " problema(s) encontrado(s)");
    }

    static List<String> lines(Path p) throws IOException {
        List<String> out = new ArrayList<>();
        for (String line : Files.readAllLines(p, StandardCharsets.UTF_8)) {
            if (!line.isBlank()) out.add(line.trim());
        }
        return out;
    }

    static List<SensorRecord> parse(List<String> lines, String name) {
        List<SensorRecord> out = new ArrayList<>();
        for (String line : lines) {
            try {
                out.add(SensorRecord.fromLine(line));
            } catch (IllegalArgumentException e) {
                System.out.println("  " + name + ": linha inválida: " + line);
            }
        }
        return out;
    }

    // a sequência tem de começar em 1 e não ter saltos nem repetições
    static String checkSeq(List<SensorRecord> recs) {
        long expected = 1;
        for (SensorRecord rec : recs) {
            if (rec.getSeq() != expected) {
                String what = rec.getSeq() < expected ? "repetição ou recuo" : "salto";
                return "ERRO, " + what + ": esperado " + expected + ", encontrado " + rec.getSeq();
            }
            expected++;
        }
        return recs.isEmpty() ? "OK (vazio)" : "OK (1.." + (expected - 1) + ")";
    }
}
