package gbn;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Funções utilitárias compartilhadas entre Emissor e Receptor:
 * cálculo de hash MD5 (requisito R9 - verificação de integridade) e
 * formatação amigável de tamanhos em bytes.
 */
public class Utils {

    /** Calcula o hash MD5 de um arquivo em disco, lendo-o em blocos (sem carregar tudo em memória). */
    public static String calcularMD5(File arquivo) throws IOException, NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        try (InputStream is = new BufferedInputStream(new FileInputStream(arquivo))) {
            byte[] buffer = new byte[8192];
            int lidos;
            while ((lidos = is.read(buffer)) != -1) {
                md.update(buffer, 0, lidos);
            }
        }
        return bytesParaHex(md.digest());
    }

    /** Calcula o hash MD5 de um array de bytes já em memória. */
    public static String calcularMD5(byte[] dados) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        return bytesParaHex(md.digest(dados));
    }

    private static String bytesParaHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    /** Formata um número de bytes em unidade legível (KB, MB, GB...). */
    public static String formatarBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(1024));
        exp = Math.min(exp, 6);
        char prefixo = "KMGTPE".charAt(exp - 1);
        return String.format("%.2f %sB", bytes / Math.pow(1024, exp), prefixo);
    }
}
