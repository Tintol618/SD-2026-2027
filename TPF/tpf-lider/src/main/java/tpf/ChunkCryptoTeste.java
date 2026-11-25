package tpf;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.HexFormat;
import java.util.List;

// Testes do chunk e da cifra (LeaderBot --teste). Sem bibliotecas de testes: o trabalho só permite a JDA,
// a Java-WebSocket e o JDK.
final class ChunkCryptoTeste {
    private static int ok, falhas;

    private ChunkCryptoTeste() { }

    static boolean correr() throws Exception {
        ok = 0;
        falhas = 0;
        System.out.println("--- Registo");
        Registo r = new Registo(7, 1764000000000L, "ana;x", "arquivo", "texto com ; e\nmudança de linha · ç");
        verifica("linha → registo → linha", Registo.deLinha(r.linha()).equals(r));
        verifica("autor sem ';'", !r.autor().contains(";"));

        System.out.println("--- Chunk");
        List<Registo> rs = List.of(
                new Registo(7, 1, "ana", "arquivo", "primeiro"),
                new Registo(8, 2, "rui", "arquivo", "segundo, com acentuação: é à õ"),
                new Registo(9, 3, "ana", "geral", ""),
                new Registo(10, 4, "bot", "local", "x".repeat(5000)));
        Chunk c = new Chunk();
        for (Registo x : rs) c.acrescentar(x);
        byte[] claro = c.selar(Wire.PAD_BYTES);
        verifica("tamanho em claro múltiplo de PAD_BYTES (" + claro.length + ")", claro.length % Wire.PAD_BYTES == 0);
        verifica("leitura inversa devolve os mesmos registos e ignora o padding", Chunk.ler(claro).equals(rs));
        verifica("chunk selado é imutável", lanca(() -> c.acrescentar(new Registo(11, 5, "a", "b", "c"))));
        Chunk d = new Chunk();
        d.acrescentar(new Registo(1, 1, "a", "b", "c"));
        verifica("LSN não consecutivo é recusado", lanca(() -> d.acrescentar(new Registo(3, 1, "a", "b", "c"))));
        Chunk e = new Chunk();
        Registo grande = new Registo(1, 1, "a", "b", "y".repeat(Wire.CHUNK_MAX_BYTES));
        verifica("um chunk vazio aceita um registo maior do que o limite", e.cabe(grande, Wire.CHUNK_MAX_BYTES));
        e.acrescentar(new Registo(1, 1, "a", "b", "c"));
        verifica("um chunk com registos recusa o que o faria exceder o limite",
                !e.cabe(new Registo(2, 1, "a", "b", "y".repeat(Wire.CHUNK_MAX_BYTES)), Wire.CHUNK_MAX_BYTES));
        int um = Chunk.serializar(List.of(rs.get(0)), Wire.PAD_BYTES).length;
        verifica("1 registo de " + Chunk.tamanho(rs.get(0)) + " bytes → chunk em claro de " + um + " bytes (um bloco de padding)",
                um == Wire.PAD_BYTES);

        System.out.println("--- HKDF-SHA256 (RFC 5869, caso de teste A.1, primeiros 32 bytes)");
        HexFormat hex = HexFormat.of();
        byte[] okm = ChunkCrypto.hkdf(hex.parseHex("0b".repeat(22)), hex.parseHex("000102030405060708090a0b0c"),
                hex.parseHex("f0f1f2f3f4f5f6f7f8f9"));
        verifica("OKM = 3cb25f25…c5bf", hex.formatHex(okm).equals("3cb25f25faacd57a90434f64d0362f2a2d2d0a90cf1a5a4c5db02d56ecc4c5bf"));

        System.out.println("--- ChunkCrypto");
        byte[] mk = ChunkCrypto.aleatorio(32);
        Sealed s = ChunkCrypto.seal(mk, claro);
        verifica("chunkId = SHA-256 do texto cifrado", s.chunkId.equals(ChunkCrypto.sha256Hex(s.cifrado)));
        verifica("texto cifrado = em claro + tag de 16 bytes", s.cifrado.length == claro.length + 16);
        verifica("cifrar e decifrar devolve o chunk original", java.util.Arrays.equals(ChunkCrypto.open(mk, s), claro));
        verifica("o texto registado não aparece no texto cifrado",
                !Verificar.contem(s.cifrado, "acentuação".getBytes(StandardCharsets.UTF_8)));
        Sealed s2 = ChunkCrypto.seal(mk, claro);
        verifica("o mesmo chunk cifrado duas vezes dá ids diferentes (salt e nonce novos)", !s2.chunkId.equals(s.chunkId));

        // byte alterado no texto cifrado: falha logo na verificação do hash
        byte[] alterado = s.cifrado.clone();
        alterado[100] ^= 0x01;
        recusa("byte alterado no texto cifrado (hash ≠ id)", mk, new Sealed(s.chunkId, s.keyVersion, s.salt, s.nonce, alterado), "hash");
        // o mesmo byte alterado com o id recalculado: passa o hash e falha a autenticação GCM
        recusa("byte alterado com o id recalculado (tag GCM)", mk,
                new Sealed(ChunkCrypto.sha256Hex(alterado), s.keyVersion, s.salt, s.nonce, alterado), "AEADBadTag");
        byte[] tagAlterada = s.cifrado.clone();
        tagAlterada[tagAlterada.length - 1] ^= 0x01;
        recusa("byte alterado na tag (id recalculado)", mk,
                new Sealed(ChunkCrypto.sha256Hex(tagAlterada), s.keyVersion, s.salt, s.nonce, tagAlterada), "AEADBadTag");
        recusa("salt diferente", mk, s.comCabecalho(s.keyVersion, ChunkCrypto.aleatorio(32), s.nonce), "AEADBadTag");
        recusa("nonce diferente", mk, s.comCabecalho(s.keyVersion, s.salt, ChunkCrypto.aleatorio(12)), "AEADBadTag");
        recusa("keyVersion diferente (2)", mk, s.comCabecalho(2, s.salt, s.nonce), "versão");
        recusa("master.key diferente", ChunkCrypto.aleatorio(32), s, "AEADBadTag");
        recusa("master.key com 31 bytes", ChunkCrypto.aleatorio(31), s, "32 bytes");
        // keyVersion entra na AAD: se a versão 2 fosse aceite, a tag continuava a falhar
        try {
            javax.crypto.Cipher ci = javax.crypto.Cipher.getInstance("AES/GCM/NoPadding");
            ci.init(javax.crypto.Cipher.DECRYPT_MODE, new javax.crypto.spec.SecretKeySpec(ChunkCrypto.dek(mk, s.salt), "AES"),
                    new javax.crypto.spec.GCMParameterSpec(128, s.nonce));
            ci.updateAAD(ChunkCrypto.aad(2, s.salt, s.nonce));
            ci.doFinal(s.cifrado);
            verifica("keyVersion faz parte da AAD", false);
        } catch (javax.crypto.AEADBadTagException x) {
            verifica("keyVersion faz parte da AAD (AAD com versão 2 → AEADBadTagException)", true);
        }

        System.out.println(ok + " testes OK, " + falhas + " falhas");
        return falhas == 0;
    }

    private static void recusa(String nome, byte[] mk, Sealed s, String esperado) {
        try {
            ChunkCrypto.open(mk, s);
            verifica(nome + ": open devia falhar", false);
        } catch (GeneralSecurityException e) {
            String m = e.getClass().getSimpleName() + ": " + e.getMessage();
            verifica(nome + " → " + m, m.contains(esperado));
        }
    }

    private static boolean lanca(Runnable r) {
        try {
            r.run();
            return false;
        } catch (RuntimeException e) {
            return true;
        }
    }

    private static void verifica(String nome, boolean cond) {
        System.out.println((cond ? "OK    " : "FALHA ") + nome);
        if (cond) ok++; else falhas++;
    }
}
