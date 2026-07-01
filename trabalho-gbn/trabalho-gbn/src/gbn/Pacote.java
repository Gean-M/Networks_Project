package gbn;

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Representa um datagrama do protocolo Go-Back-N implementado sobre UDP.
 *
 * Formato do cabeçalho, conforme especificado no enunciado (Seção 3.4),
 * montado/desmontado com ByteBuffer para garantir portabilidade:
 *
 *   [1 byte  tipo         ] 0=DATA, 1=ACK, 2=HANDSHAKE, 3=FIN
 *   [4 bytes num_seq      ] número de sequência do pacote (int, big-endian)
 *   [4 bytes num_ack      ] número de confirmação (somente em ACKs)
 *   [2 bytes tamanho_dados] quantidade de bytes válidos no payload (short)
 *   [N bytes dados        ] payload (até MAX_PAYLOAD bytes)
 *
 * Pacotes de controle (HANDSHAKE) reaproveitam o campo "dados" para
 * transportar uma string de parâmetros separados por '|'.
 */
public class Pacote {

    // ----- tipos de pacote -----
    public static final int TIPO_DATA = 0;
    public static final int TIPO_ACK = 1;
    public static final int TIPO_HANDSHAKE = 2;
    public static final int TIPO_FIN = 3;

    // ----- constantes de tamanho -----
    public static final int MAX_PAYLOAD = 1024;
    public static final int HEADER_SIZE = 1 + 4 + 4 + 2; // 11 bytes
    public static final int MAX_PACKET_SIZE = HEADER_SIZE + MAX_PAYLOAD;

    // ----- sentinelas usadas no campo num_ack para pacotes de controle -----
    public static final int ACK_HANDSHAKE = -1;
    public static final int ACK_FIN = -2;

    public final int tipo;
    public final int numSeq;
    public final int numAck;
    public final byte[] dados; // tamanho exato dos dados válidos (sem padding)

    public Pacote(int tipo, int numSeq, int numAck, byte[] dados) {
        this.tipo = tipo;
        this.numSeq = numSeq;
        this.numAck = numAck;
        this.dados = (dados == null) ? new byte[0] : dados;
    }

    // ---------------------------------------------------------------
    // Fábricas de conveniência
    // ---------------------------------------------------------------

    /** Cria um pacote de dados a partir de um trecho [offset, offset+tamanho) do array de origem. */
    public static Pacote criarData(int seq, byte[] origem, int offset, int tamanho) {
        byte[] payload = new byte[tamanho];
        System.arraycopy(origem, offset, payload, 0, tamanho);
        return new Pacote(TIPO_DATA, seq, 0, payload);
    }

    /** Cria um ACK cumulativo (ou uma sentinela ACK_HANDSHAKE / ACK_FIN). */
    public static Pacote criarAck(int numAck) {
        return new Pacote(TIPO_ACK, 0, numAck, new byte[0]);
    }

    /** Cria o pacote de controle de handshake, com os parâmetros da sessão em texto. */
    public static Pacote criarHandshake(String payload) {
        return new Pacote(TIPO_HANDSHAKE, 0, 0, payload.getBytes(StandardCharsets.UTF_8));
    }

    /** Cria o pacote de encerramento (FIN). */
    public static Pacote criarFin(int totalPacotesEnviados) {
        return new Pacote(TIPO_FIN, totalPacotesEnviados, 0, new byte[0]);
    }

    public String getDadosComoString() {
        return new String(dados, StandardCharsets.UTF_8);
    }

    // ---------------------------------------------------------------
    // Serialização / desserialização
    // ---------------------------------------------------------------

    /** Serializa o pacote em um array de bytes pronto para envio via UDP. */
    public byte[] serializar() {
        ByteBuffer buf = ByteBuffer.allocate(HEADER_SIZE + dados.length);
        buf.put((byte) tipo);
        buf.putInt(numSeq);
        buf.putInt(numAck);
        buf.putShort((short) dados.length);
        buf.put(dados);
        return buf.array();
    }

    /** Constrói um DatagramPacket pronto para ser enviado a (destino, porta). */
    public DatagramPacket paraDatagramPacket(InetAddress destino, int porta) {
        byte[] bytes = serializar();
        return new DatagramPacket(bytes, bytes.length, destino, porta);
    }

    /** Desserializa um pacote recebido a partir do buffer bruto (e seu comprimento útil). */
    public static Pacote deserializar(byte[] buffer, int length) {
        ByteBuffer buf = ByteBuffer.wrap(buffer, 0, length);
        int tipo = buf.get() & 0xFF;
        int numSeq = buf.getInt();
        int numAck = buf.getInt();
        int tamanhoDados = buf.getShort() & 0xFFFF;
        byte[] dados = new byte[tamanhoDados];
        buf.get(dados, 0, tamanhoDados);
        return new Pacote(tipo, numSeq, numAck, dados);
    }

    @Override
    public String toString() {
        String tipoStr;
        switch (tipo) {
            case TIPO_DATA: tipoStr = "DATA"; break;
            case TIPO_ACK: tipoStr = "ACK"; break;
            case TIPO_HANDSHAKE: tipoStr = "HANDSHAKE"; break;
            case TIPO_FIN: tipoStr = "FIN"; break;
            default: tipoStr = "DESCONHECIDO(" + tipo + ")";
        }
        return String.format("Pacote[tipo=%s, seq=%d, ack=%d, bytes=%d]",
                tipoStr, numSeq, numAck, dados.length);
    }
}
