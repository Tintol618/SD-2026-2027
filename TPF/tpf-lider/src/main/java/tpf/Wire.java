package tpf;

import java.nio.file.Path;
import java.nio.file.Paths;

// Parâmetros do arquivo. Os três primeiros podem ser alterados no arranque com -Dtpf.<nome>=<valor>.
final class Wire {
    private Wire() { }

    // tamanho máximo do chunk serializado (antes do padding): um registo que o faria exceder sela-o primeiro
    static final int CHUNK_MAX_BYTES = Integer.getInteger("tpf.chunkMaxBytes", 65536);
    // o chunk em claro é completado com zeros até um múltiplo deste valor
    static final int PAD_BYTES = Integer.getInteger("tpf.padBytes", 4096);
    // um chunk aberto é selado este tempo depois do seu primeiro registo, mesmo que não esteja cheio
    static final long SELAGEM_MS = Long.getLong("tpf.selagemMs", 5000);

    // cifra
    static final int MASTER_KEY_BYTES = 32;
    static final int KEY_VERSION = 1;
    static final String HKDF_INFO = "tpf-chunk-v1";
    static final String AAD_PREFIXO = "tpf-v1";
    static final int SALT_BYTES = 32;
    static final int NONCE_BYTES = 12;
    static final int TAG_BITS = 128;

    // ficheiros do líder (na pasta de onde é arrancado)
    static final Path MASTER_KEY = Paths.get(System.getProperty("tpf.masterKey", "master.key"));
    static final Path REGISTOS = Paths.get("registos.log");
    static final Path SELADOS = Paths.get("selados");

    static void validar() {
        if (CHUNK_MAX_BYTES < 64) throw new IllegalArgumentException("tpf.chunkMaxBytes tem de ser >= 64");
        if (PAD_BYTES < 1) throw new IllegalArgumentException("tpf.padBytes tem de ser >= 1");
        if (SELAGEM_MS < 1) throw new IllegalArgumentException("tpf.selagemMs tem de ser >= 1");
    }
}
