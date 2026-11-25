package tpf;

import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HexFormat;

// Cifra dos chunks: uma DEK por chunk, derivada da master key (MK) com HKDF-SHA256, e AES-256-GCM.
//   salt  = 32 bytes aleatórios por chunk     nonce = 12 bytes aleatórios por chunk
//   DEK   = HKDF-SHA256(MK, salt, info = "tpf-chunk-v1"), 32 bytes
//   aad   = "tpf-v1" | keyVersion (int32 big-endian) | salt | nonce
//   chunkId = SHA-256 do texto cifrado (incluindo a tag), em hexadecimal
final class ChunkCrypto {
    private static final SecureRandom RANDOM = new SecureRandom();

    private ChunkCrypto() { }

    static Sealed seal(byte[] mk, byte[] claro) throws GeneralSecurityException {
        verificarMk(mk);
        byte[] salt = aleatorio(Wire.SALT_BYTES);
        byte[] nonce = aleatorio(Wire.NONCE_BYTES);
        int versao = Wire.KEY_VERSION;
        byte[] dek = dek(mk, salt);
        try {
            Cipher c = Cipher.getInstance("AES/GCM/NoPadding");
            c.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(dek, "AES"), new GCMParameterSpec(Wire.TAG_BITS, nonce));
            c.updateAAD(aad(versao, salt, nonce));
            byte[] cifrado = c.doFinal(claro);            // texto cifrado seguido da tag de 16 bytes
            return new Sealed(sha256Hex(cifrado), versao, salt, nonce, cifrado);
        } finally {
            Arrays.fill(dek, (byte) 0);
        }
    }

    // verifica, por esta ordem, o hash contra o id, a versão da chave e a autenticação GCM;
    // qualquer falha lança exceção e nada do conteúdo é devolvido
    static byte[] open(byte[] mk, Sealed s) throws GeneralSecurityException {
        verificarMk(mk);
        String hash = sha256Hex(s.cifrado);
        if (!MessageDigest.isEqual(hash.getBytes(StandardCharsets.US_ASCII), s.chunkId.getBytes(StandardCharsets.US_ASCII))) {
            throw new GeneralSecurityException("hash do conteúdo (" + hash.substring(0, 12) + "…) diferente do chunkId ("
                    + abreviar(s.chunkId) + ")");
        }
        if (s.keyVersion != Wire.KEY_VERSION) {
            throw new GeneralSecurityException("versão de chave desconhecida: " + s.keyVersion);
        }
        if (s.salt.length != Wire.SALT_BYTES || s.nonce.length != Wire.NONCE_BYTES) {
            throw new GeneralSecurityException("salt ou nonce com tamanho errado");
        }
        byte[] dek = dek(mk, s.salt);
        try {
            Cipher c = Cipher.getInstance("AES/GCM/NoPadding");
            c.init(Cipher.DECRYPT_MODE, new SecretKeySpec(dek, "AES"), new GCMParameterSpec(Wire.TAG_BITS, s.nonce));
            c.updateAAD(aad(s.keyVersion, s.salt, s.nonce));
            return c.doFinal(s.cifrado);                  // AEADBadTagException se a autenticação falhar
        } finally {
            Arrays.fill(dek, (byte) 0);
        }
    }

    static byte[] dek(byte[] mk, byte[] salt) throws GeneralSecurityException {
        return hkdf(mk, salt, Wire.HKDF_INFO.getBytes(StandardCharsets.US_ASCII));
    }

    // HKDF-SHA256 (RFC 5869) com um único bloco de expansão: 32 bytes
    //   extração:  PRK = HMAC-SHA256(chave = salt, mensagem = IKM)
    //   expansão:  OKM = HMAC-SHA256(chave = PRK,  mensagem = info | 0x01)
    static byte[] hkdf(byte[] ikm, byte[] salt, byte[] info) throws GeneralSecurityException {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(salt, "HmacSHA256"));
        byte[] prk = mac.doFinal(ikm);
        mac.init(new SecretKeySpec(prk, "HmacSHA256"));
        mac.update(info);
        mac.update((byte) 0x01);
        byte[] okm = mac.doFinal();
        Arrays.fill(prk, (byte) 0);
        return okm;
    }

    static byte[] aad(int keyVersion, byte[] salt, byte[] nonce) {
        byte[] prefixo = Wire.AAD_PREFIXO.getBytes(StandardCharsets.US_ASCII);
        return ByteBuffer.allocate(prefixo.length + 4 + salt.length + nonce.length)
                .put(prefixo).putInt(keyVersion).put(salt).put(nonce).array();
    }

    static String sha256Hex(byte[] dados) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(dados));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("SHA-256 indisponível", e);
        }
    }

    static byte[] aleatorio(int n) {
        byte[] b = new byte[n];
        RANDOM.nextBytes(b);
        return b;
    }

    private static void verificarMk(byte[] mk) throws GeneralSecurityException {
        if (mk == null || mk.length != Wire.MASTER_KEY_BYTES) {
            throw new GeneralSecurityException("a master key tem de ter " + Wire.MASTER_KEY_BYTES + " bytes");
        }
    }

    private static String abreviar(String s) {
        return s.length() <= 12 ? s : s.substring(0, 12) + "…";
    }
}
