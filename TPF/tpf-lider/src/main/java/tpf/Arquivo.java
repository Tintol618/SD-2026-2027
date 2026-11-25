package tpf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

// Caminho de escrita local do líder:
//   registar(...) → LSN seguinte → linha em registos.log + fsync → chunk aberto
//   chunk aberto → selado por tamanho (CHUNK_MAX_BYTES) ou por tempo (SELAGEM_MS desde o 1.º registo)
//   selagem → serialização com padding → ChunkCrypto.seal → selados/<chunkId>.bin + .hdr
// Todos os métodos que mexem no LSN ou no chunk aberto são sincronizados no próprio Arquivo, por isso
// /registar, as mensagens de #arquivo, a /rajada e o temporizador de selagem nunca se intercalam.
final class Arquivo {
    interface Eventos {
        void evento(String tipo, String detalhe);
    }

    private final byte[] mk;
    private final Eventos ev;
    private final FileChannel log;
    private final ScheduledExecutorService agenda;

    private long ultimoLsn;           // último LSN em registos.log
    private long ultimoSelado;        // último LSN que já está num chunk selado
    private int selados;              // chunks em selados/
    private Chunk aberto = new Chunk();
    private ScheduledFuture<?> temporizador;
    private boolean avariado;         // uma escrita em registos.log falhou: recusa até reiniciar

    private Arquivo(byte[] mk, Eventos ev, FileChannel log) {
        this.mk = mk;
        this.ev = ev;
        this.log = log;
        this.agenda = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "selagem");
            t.setDaemon(true);
            return t;
        });
    }

    // arranque: master.key, registos.log (continua a numeração), selados/ (último LSN selado) e
    // recolocação no chunk aberto dos registos que ficaram por selar
    static Arquivo abrir(Eventos ev) throws IOException, GeneralSecurityException {
        Wire.validar();
        trancar();
        boolean[] criada = {false};
        byte[] mk = MasterKey.carregarOuCriar(Wire.MASTER_KEY, criada);
        if (criada[0]) ev.evento("CHAVE", "master.key gerada (32 bytes de SecureRandom, leitura só para o dono)");

        List<Registo> doLog = lerLog(Wire.REGISTOS, ev);
        FileChannel ch = FileChannel.open(Wire.REGISTOS, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.APPEND);
        Arquivo a = new Arquivo(mk, ev, ch);
        a.ultimoLsn = doLog.isEmpty() ? 0 : doLog.get(doLog.size() - 1).lsn();

        // o último LSN selado vem dos próprios chunks: abri-los confirma também que a master.key é a certa
        Files.createDirectories(Wire.SELADOS);
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(Wire.SELADOS, "*.tmp")) {
            for (Path p : ds) Files.deleteIfExists(p);      // escrita interrompida a meio
        }
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(Wire.SELADOS, "*.bin")) {
            for (Path p : ds) {
                String id = p.getFileName().toString().replace(".bin", "");
                if (!Files.exists(Wire.SELADOS.resolve(id + ".hdr"))) {
                    ev.evento("AVISO", "selados/" + id + ".bin sem .hdr (selagem interrompida): ignorado, os registos voltam a ser selados");
                    continue;
                }
                List<Registo> rs;
                try {
                    rs = Chunk.ler(ChunkCrypto.open(mk, Sealed.ler(Wire.SELADOS, id)));
                } catch (GeneralSecurityException | IllegalArgumentException e) {
                    ch.close();
                    throw new GeneralSecurityException("não foi possível abrir selados/" + id
                            + ".bin (" + e.getMessage() + "). Chunk alterado ou master.key errada: arranque recusado."
                            + " Usa --verificar para ver todos os chunks.", e);
                }
                a.selados++;
                a.ultimoSelado = Math.max(a.ultimoSelado, rs.get(rs.size() - 1).lsn());
            }
        }

        int pendentes = 0;
        synchronized (a) {
            for (Registo r : doLog) {
                if (r.lsn() > a.ultimoSelado) {
                    a.paraChunk(r);
                    pendentes++;
                }
            }
        }
        ev.evento("ARQUIVO", "ultimoLsn=" + a.ultimoLsn + ";chunksSelados=" + a.selados + ";ultimoLsnSelado="
                + a.ultimoSelado + ";porSelar=" + pendentes + ";chunkMax=" + Wire.CHUNK_MAX_BYTES + ";pad="
                + Wire.PAD_BYTES + ";selagemMs=" + Wire.SELAGEM_MS);
        return a;
    }

    // Dois líderes na mesma pasta escreveriam o mesmo LSN duas vezes no registos.log (o Discord entrega
    // cada comando a todas as sessões do bot). O trinco do SO é libertado sozinho quando o processo termina,
    // mesmo que morra sem paragem ordenada, por isso nunca fica preso.
    private static FileChannel trincoCanal;
    private static java.nio.channels.FileLock trinco;

    private static synchronized void trancar() throws IOException {
        if (trinco != null) return;
        trincoCanal = FileChannel.open(Wire.TRINCO, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        try {
            trinco = trincoCanal.tryLock();
        } catch (java.nio.channels.OverlappingFileLockException e) {
            trinco = null;
        }
        if (trinco == null) {
            trincoCanal.close();
            trincoCanal = null;
            throw new IOException("já há um líder a correr nesta pasta (" + Wire.TRINCO.toAbsolutePath()
                    + " está trancado). Fecha-o antes de arrancar outro.");
        }
    }

    // lê registos.log e confirma que os LSN são 1, 2, 3, … Uma última linha incompleta (falha a meio de
    // uma escrita, antes do fsync e portanto nunca confirmada ao utilizador) é cortada do ficheiro.
    static List<Registo> lerLog(Path f, Eventos ev) throws IOException {
        List<Registo> rs = new ArrayList<>();
        if (!Files.exists(f)) return rs;
        byte[] tudo = Files.readAllBytes(f);
        int fim = tudo.length;
        if (fim > 0 && tudo[fim - 1] != '\n') {
            int corte = fim - 1;
            while (corte >= 0 && tudo[corte] != '\n') corte--;
            corte++;
            try (FileChannel ch = FileChannel.open(f, StandardOpenOption.WRITE)) {
                ch.truncate(corte);
                ch.force(true);
            }
            if (ev != null) ev.evento("AVISO", f + ": última linha incompleta (" + (fim - corte) + " bytes) descartada");
            fim = corte;
        }
        String[] linhas = new String(tudo, 0, fim, StandardCharsets.UTF_8).split("\n");
        int n = 0;
        for (String l : linhas) {
            n++;
            if (l.isEmpty()) continue;
            Registo r;
            try {
                r = Registo.deLinha(l);
            } catch (IllegalArgumentException e) {
                throw new IOException(f + ", linha " + n + ": " + e.getMessage());
            }
            long esperado = rs.isEmpty() ? 1 : rs.get(rs.size() - 1).lsn() + 1;
            if (r.lsn() != esperado) {
                throw new IOException(f + ", linha " + n + ": LSN " + r.lsn() + " (esperava " + esperado + ")");
            }
            rs.add(r);
        }
        return rs;
    }

    // o único caminho de escrita: /registar, mensagens em #arquivo e /rajada passam todos por aqui
    synchronized Registo registar(String autor, String canal, String texto) throws IOException {
        if (avariado) throw new IOException("registos.log em erro: reinicia o líder");
        Registo r = new Registo(ultimoLsn + 1, System.currentTimeMillis(), autor, canal, texto);
        ByteBuffer b = ByteBuffer.wrap((r.linha() + "\n").getBytes(StandardCharsets.UTF_8));
        try {
            while (b.hasRemaining()) log.write(b);
            log.force(true);                     // fsync: só depois disto o LSN é confirmado
        } catch (IOException e) {
            avariado = true;                     // não se sabe se a linha ficou no disco: não reutilizar o LSN
            ev.evento("ERRO", "escrita em registos.log falhou no LSN " + r.lsn() + ": " + e.getMessage());
            throw e;
        }
        ultimoLsn = r.lsn();
        ev.evento("REGISTO", "lsn=" + r.lsn() + ";autor=" + r.autor() + ";canal=" + r.canal()
                + ";bytes=" + Chunk.tamanho(r));
        paraChunk(r);
        return r;
    }

    // chamado com o monitor do Arquivo
    private void paraChunk(Registo r) {
        if (!aberto.cabe(r, Wire.CHUNK_MAX_BYTES)) selar("tamanho");   // o registo não cabe: sela o atual
        aberto.acrescentar(r);
        if (aberto.numero() == 1) {
            Chunk este = aberto;
            temporizador = agenda.schedule(() -> selarPorTempo(este), Wire.SELAGEM_MS, TimeUnit.MILLISECONDS);
        }
        if (aberto.bytes() >= Wire.CHUNK_MAX_BYTES) selar("tamanho");  // ficou cheio (ou um só registo enorme)
    }

    private synchronized void selarPorTempo(Chunk c) {
        if (aberto == c && !c.vazio()) selar("tempo");
    }

    // chamado com o monitor do Arquivo
    private void selar(String motivo) {
        if (aberto.vazio()) return;
        if (temporizador != null) temporizador.cancel(false);
        temporizador = null;
        Chunk c = aberto;
        aberto = new Chunk();
        byte[] claro = c.selar(Wire.PAD_BYTES);
        try {
            Sealed s = ChunkCrypto.seal(mk, claro);
            s.gravar(Wire.SELADOS);
            java.util.Arrays.fill(claro, (byte) 0);
            selados++;
            ultimoSelado = c.ultimoLsn();
            ev.evento("SELADO", "chunkId=" + s.chunkId + ";lsn=" + c.primeiroLsn() + ".." + c.ultimoLsn()
                    + ";registos=" + c.numero() + ";tamanho=" + claro.length + ";cifrado=" + s.cifrado.length
                    + ";motivo=" + motivo);
        } catch (IOException | GeneralSecurityException e) {
            // os registos continuam em registos.log; voltam para um chunk aberto e tenta-se de novo mais tarde
            ev.evento("ERRO", "selagem de lsn=" + c.primeiroLsn() + ".." + c.ultimoLsn() + " falhou: " + e.getMessage());
            Chunk novo = new Chunk();
            for (Registo r : c.registos()) novo.acrescentar(r);
            aberto = novo;
            temporizador = agenda.schedule(() -> selarPorTempo(novo), Wire.SELAGEM_MS, TimeUnit.MILLISECONDS);
        }
    }

    synchronized long ultimoLsn() { return ultimoLsn; }
    synchronized long ultimoSelado() { return ultimoSelado; }
    synchronized int chunksSelados() { return selados; }
    synchronized int registosPorSelar() { return aberto.numero(); }

    // paragem ordenada: sela o que está aberto e fecha registos.log
    synchronized void fechar() {
        if (!aberto.vazio()) selar("paragem");
        agenda.shutdownNow();
        try {
            log.close();
        } catch (IOException e) {
            ev.evento("ERRO", "fecho de registos.log: " + e.getMessage());
        }
    }
}
