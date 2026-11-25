package tpf;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Base64;

// Chunk selado: o texto cifrado (com a tag GCM no fim) e os parâmetros públicos necessários para o abrir.
// Em disco são dois ficheiros com o mesmo nome, o chunkId (SHA-256 do texto cifrado, em hexadecimal):
//   selados/<chunkId>.bin   só os bytes cifrados (texto cifrado + tag de 16 bytes)
//   selados/<chunkId>.hdr   uma linha  keyVersion;salt;nonce  (keyVersion em decimal, salt e nonce em Base64)
final class Sealed {
    final String chunkId;
    final int keyVersion;
    final byte[] salt;
    final byte[] nonce;
    final byte[] cifrado;

    Sealed(String chunkId, int keyVersion, byte[] salt, byte[] nonce, byte[] cifrado) {
        this.chunkId = chunkId;
        this.keyVersion = keyVersion;
        this.salt = salt;
        this.nonce = nonce;
        this.cifrado = cifrado;
    }

    String hdr() {
        Base64.Encoder b64 = Base64.getEncoder();
        return keyVersion + ";" + b64.encodeToString(salt) + ";" + b64.encodeToString(nonce);
    }

    // cópia com outro cabeçalho e o mesmo texto cifrado (usada nos testes)
    Sealed comCabecalho(int keyVersion, byte[] salt, byte[] nonce) {
        return new Sealed(chunkId, keyVersion, salt, nonce, cifrado);
    }

    // grava .bin e depois .hdr, cada um num temporário que é sincronizado e movido de forma atómica
    void gravar(Path pasta) throws IOException {
        Files.createDirectories(pasta);
        escreverAtomico(pasta.resolve(chunkId + ".bin"), cifrado);
        escreverAtomico(pasta.resolve(chunkId + ".hdr"), (hdr() + "\n").getBytes(StandardCharsets.UTF_8));
    }

    static Sealed ler(Path pasta, String chunkId) throws IOException {
        byte[] cifrado = Files.readAllBytes(pasta.resolve(chunkId + ".bin"));
        String linha = Files.readString(pasta.resolve(chunkId + ".hdr"), StandardCharsets.UTF_8).trim();
        String[] p = linha.split(";", -1);
        if (p.length != 3) throw new IOException(chunkId + ".hdr: esperava keyVersion;salt;nonce");
        try {
            Base64.Decoder b64 = Base64.getDecoder();
            return new Sealed(chunkId, Integer.parseInt(p[0]), b64.decode(p[1]), b64.decode(p[2]), cifrado);
        } catch (IllegalArgumentException e) {
            throw new IOException(chunkId + ".hdr inválido: " + e.getMessage(), e);
        }
    }

    static void escreverAtomico(Path destino, byte[] conteudo) throws IOException {
        Path tmp = destino.resolveSibling(destino.getFileName() + ".tmp");
        try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            java.nio.ByteBuffer b = java.nio.ByteBuffer.wrap(conteudo);
            while (b.hasRemaining()) ch.write(b);
            ch.force(true);
        }
        Files.move(tmp, destino, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }
}
