package tpf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.AclEntry;
import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.AclEntryType;
import java.nio.file.attribute.AclFileAttributeView;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserPrincipal;
import java.security.SecureRandom;
import java.util.EnumSet;
import java.util.List;

// master.key: 32 bytes aleatórios, só no líder e nunca no repositório. É a raiz de todas as DEK.
final class MasterKey {
    private MasterKey() { }

    // devolve a chave; se o ficheiro não existir, gera-a (primeiro arranque). criada[0] diz se foi gerada agora.
    static byte[] carregarOuCriar(Path f, boolean[] criada) throws IOException {
        if (Files.exists(f)) {
            criada[0] = false;
            return ler(f);
        }
        byte[] mk = new byte[Wire.MASTER_KEY_BYTES];
        new SecureRandom().nextBytes(mk);
        try (FileChannel ch = FileChannel.open(f, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            ch.write(ByteBuffer.wrap(mk));
            ch.force(true);
        }
        soLeituraDoDono(f);
        criada[0] = true;
        return mk;
    }

    // lê uma master.key existente; recusa-a se não tiver exatamente 32 bytes
    static byte[] ler(Path f) throws IOException {
        long tamanho = Files.size(f);
        if (tamanho != Wire.MASTER_KEY_BYTES) {
            throw new IOException(f + " tem " + tamanho + " bytes (esperava " + Wire.MASTER_KEY_BYTES
                    + "): arranque recusado. Repõe a chave correta; apagá-la gera outra e os chunks já selados deixam de abrir.");
        }
        return Files.readAllBytes(f);
    }

    // POSIX: r-------- (0400). Windows (NTFS): a ACL passa a ter uma só entrada, a do dono, com leitura
    // (e permissão para alterar a ACL e apagar o ficheiro, para o próprio dono o poder gerir).
    static void soLeituraDoDono(Path f) throws IOException {
        PosixFileAttributeView posix = Files.getFileAttributeView(f, PosixFileAttributeView.class);
        if (posix != null) {
            Files.setPosixFilePermissions(f, PosixFilePermissions.fromString("r--------"));
            return;
        }
        AclFileAttributeView acl = Files.getFileAttributeView(f, AclFileAttributeView.class);
        if (acl != null) {
            UserPrincipal dono = Files.getOwner(f);
            AclEntry so = AclEntry.newBuilder()
                    .setType(AclEntryType.ALLOW)
                    .setPrincipal(dono)
                    .setPermissions(EnumSet.of(AclEntryPermission.READ_DATA, AclEntryPermission.READ_ATTRIBUTES,
                            AclEntryPermission.READ_NAMED_ATTRS, AclEntryPermission.READ_ACL,
                            AclEntryPermission.WRITE_ACL, AclEntryPermission.DELETE, AclEntryPermission.SYNCHRONIZE))
                    .build();
            acl.setAcl(List.of(so));
            return;
        }
        // outro sistema de ficheiros: o melhor que o java.io permite
        java.io.File x = f.toFile();
        x.setReadable(false, false);
        x.setWritable(false, false);
        x.setReadable(true, true);
    }
}
