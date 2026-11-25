package tpf;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

// Chunk: grupo de registos com LSN consecutivos. Enquanto está aberto recebe registos; depois de selado
// (serializado) é imutável e o registo seguinte abre um chunk novo.
//
// Formato binário do chunk em claro (inteiros em big-endian, texto em UTF-8):
//   "TPFC" (4 bytes) | formato = 1 (1 byte) | n.º de registos (int32)
//   n × [ lsn (int64) | instante (int64) | autor: comprimento (uint16) + bytes
//                                       | canal: comprimento (uint16) + bytes
//                                       | texto: comprimento (int32)  + bytes ]
//   padding com 0x00 até ao múltiplo seguinte de PAD_BYTES (nenhum, se já for múltiplo)
final class Chunk {
    static final byte[] MAGIC = {'T', 'P', 'F', 'C'};
    static final int FORMATO = 1;
    static final int CABECALHO = 4 + 1 + 4;

    private final List<Registo> registos = new ArrayList<>();
    private int bytes = CABECALHO;      // tamanho serializado atual, sem padding
    private boolean selado;

    // bytes que o registo ocupa no chunk serializado
    static int tamanho(Registo r) {
        return 8 + 8 + 2 + utf8(r.autor()).length + 2 + utf8(r.canal()).length + 4 + utf8(r.texto()).length;
    }

    // true se o registo cabe sem o chunk passar de CHUNK_MAX_BYTES. Um chunk vazio aceita sempre o primeiro
    // registo, mesmo que sozinho exceda o limite (fica um chunk de um só registo).
    boolean cabe(Registo r, int max) {
        return registos.isEmpty() || bytes + tamanho(r) <= max;
    }

    void acrescentar(Registo r) {
        if (selado) throw new IllegalStateException("chunk selado: é imutável");
        if (!registos.isEmpty() && r.lsn() != ultimoLsn() + 1) {
            throw new IllegalArgumentException("LSN " + r.lsn() + " não é o seguinte de " + ultimoLsn());
        }
        registos.add(r);
        bytes += tamanho(r);
    }

    boolean vazio() { return registos.isEmpty(); }
    boolean selado() { return selado; }
    int bytes() { return bytes; }
    int numero() { return registos.size(); }
    long primeiroLsn() { return registos.get(0).lsn(); }
    long ultimoLsn() { return registos.get(registos.size() - 1).lsn(); }
    List<Registo> registos() { return Collections.unmodifiableList(registos); }

    // sela o chunk e devolve-o serializado com padding até um múltiplo de pad
    byte[] selar(int pad) {
        if (registos.isEmpty()) throw new IllegalStateException("chunk vazio");
        selado = true;
        return serializar(registos, pad);
    }

    static byte[] serializar(List<Registo> rs, int pad) {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(buf)) {
            out.write(MAGIC);
            out.writeByte(FORMATO);
            out.writeInt(rs.size());
            for (Registo r : rs) {
                out.writeLong(r.lsn());
                out.writeLong(r.instante());
                curto(out, utf8(r.autor()));
                curto(out, utf8(r.canal()));
                byte[] t = utf8(r.texto());
                out.writeInt(t.length);
                out.write(t);
            }
            int resto = out.size() % pad;
            if (resto != 0) out.write(new byte[pad - resto]);
        } catch (IOException e) {
            throw new UncheckedIOException(e);     // não acontece com um ByteArrayOutputStream
        }
        return buf.toByteArray();
    }

    // leitura inversa: reconstrói os registos e ignora o padding
    static List<Registo> ler(byte[] claro) {
        ByteBuffer b = ByteBuffer.wrap(claro);
        try {
            byte[] m = new byte[4];
            b.get(m);
            if (!java.util.Arrays.equals(m, MAGIC)) throw new IllegalArgumentException("não é um chunk (magic errado)");
            int formato = b.get() & 0xff;
            if (formato != FORMATO) throw new IllegalArgumentException("formato de chunk desconhecido: " + formato);
            int n = b.getInt();
            if (n < 1) throw new IllegalArgumentException("número de registos inválido: " + n);
            List<Registo> rs = new ArrayList<>(Math.min(n, 4096));
            for (int i = 0; i < n; i++) {
                long lsn = b.getLong();
                long instante = b.getLong();
                String autor = texto(b, b.getShort() & 0xffff);
                String canal = texto(b, b.getShort() & 0xffff);
                String texto = texto(b, b.getInt());
                rs.add(new Registo(lsn, instante, autor, canal, texto));
            }
            return rs;      // o que sobra no buffer é padding
        } catch (BufferUnderflowException e) {
            throw new IllegalArgumentException("chunk truncado", e);
        }
    }

    private static void curto(DataOutputStream out, byte[] b) throws IOException {
        if (b.length > 0xffff) throw new IllegalArgumentException("campo com mais de 65535 bytes");
        out.writeShort(b.length);
        out.write(b);
    }

    private static String texto(ByteBuffer b, int n) {
        if (n < 0 || n > b.remaining()) throw new IllegalArgumentException("comprimento inválido: " + n);
        byte[] x = new byte[n];
        b.get(x);
        return new String(x, StandardCharsets.UTF_8);
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
