package gbn;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.Random;

/**
 * Módulo Receptor do protocolo Go-Back-N (Seção 3.2 do enunciado).
 *
 * A FSM do receptor possui um único estado: aceita apenas pacotes com
 * seqnum == expectedseqnum, envia ACK cumulativo, e descarta (sem ACK
 * novo) qualquer pacote fora de ordem, reenviando o último ACK válido.
 *
 * Além disso, simula perda de pacotes de acordo com a probabilidade
 * recebida no handshake (Seção 4), e ao final calcula estatísticas e
 * verifica a integridade do arquivo recebido via hash MD5 (R9).
 *
 * O Receptor roda em loop, podendo atender múltiplas sessões de
 * transferência sequenciais sem precisar ser reiniciado.
 */
public class Receptor {

    private static final int PORTA_PADRAO = 5000;
    private static final int PAYLOAD_SIZE = Pacote.MAX_PAYLOAD;
    private static final int IDLE_TIMEOUT_MS = 30000; // abandona sessão sem atividade por 30s

    public static void main(String[] args) {
        // Garante acentuação correta no console independentemente do locale do SO.
        System.setOut(new PrintStream(System.out, true, StandardCharsets.UTF_8));
        System.setErr(new PrintStream(System.err, true, StandardCharsets.UTF_8));

        int porta = PORTA_PADRAO;
        if (args.length >= 1) {
            try {
                porta = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("Porta inválida, usando padrão " + PORTA_PADRAO);
            }
        }

        try (DatagramSocket socket = new DatagramSocket(porta)) {
            System.out.println("=================================================");
            System.out.println(" Receptor Go-Back-N — aguardando na porta " + porta);
            System.out.println("=================================================");

            byte[] buffer = new byte[Pacote.MAX_PACKET_SIZE];
            Random random = new Random();

            while (true) {
                DatagramPacket recvPacket = new DatagramPacket(buffer, buffer.length);
                socket.receive(recvPacket);
                Pacote pacote = Pacote.deserializar(recvPacket.getData(), recvPacket.getLength());

                if (pacote.tipo != Pacote.TIPO_HANDSHAKE) {
                    // Pacote fora do contexto de uma sessão ativa (ex.: retransmissão tardia
                    // de uma sessão anterior) — ignorado.
                    continue;
                }

                try {
                    processarSessao(socket, pacote, recvPacket.getAddress(), recvPacket.getPort(), random);
                } catch (Exception e) {
                    System.err.println("Erro durante a sessão: " + e.getMessage());
                }
            }

        } catch (SocketException e) {
            System.err.println("Erro ao abrir socket na porta " + porta + ": " + e.getMessage());
        } catch (IOException e) {
            System.err.println("Erro de E/S: " + e.getMessage());
        }
    }

    /** Processa uma sessão completa de transferência, do handshake ao FIN. */
    private static void processarSessao(DatagramSocket socket, Pacote handshake,
                                         InetAddress enderecoEmissor, int portaEmissor,
                                         Random random) throws IOException {

        String[] partes = handshake.getDadosComoString().split("\\|", -1);
        if (partes.length < 4) {
            System.err.println("Handshake mal formatado, sessão ignorada.");
            return;
        }
        String caminhoDestino = partes[0];
        long tamanhoArquivo = Long.parseLong(partes[1]);
        double probPerda = Double.parseDouble(partes[2]);
        String hashOrigem = partes[3];

        System.out.println();
        System.out.println("--- Nova sessão de transferência ---");
        System.out.println("Origem                : " + enderecoEmissor.getHostAddress() + ":" + portaEmissor);
        System.out.println("Arquivo de destino     : " + caminhoDestino);
        System.out.println("Tamanho do arquivo     : " + Utils.formatarBytes(tamanhoArquivo));
        System.out.printf ("Probabilidade de perda : %.1f%%%n", probPerda * 100);

        File arquivoDestino = new File(caminhoDestino);
        File dirPai = arquivoDestino.getParentFile();
        if (dirPai != null && !dirPai.exists()) {
            dirPai.mkdirs();
        }

        // Confirma o handshake (pacotes de controle não sofrem perda simulada).
        enviarAck(socket, Pacote.ACK_HANDSHAKE, enderecoEmissor, portaEmissor);

        int expectedSeqNum = 0;
        int ultimoAckEnviado = -1;
        long recebidosOk = 0;
        long perdidosSimulados = 0;
        long foraDeOrdem = 0;
        long inicio = System.currentTimeMillis();

        int totalPacotesEsperados = (int) Math.ceil(tamanhoArquivo / (double) PAYLOAD_SIZE);
        if (totalPacotesEsperados == 0) totalPacotesEsperados = 0; // arquivo vazio: nada a receber

        // Timeout de inatividade: se nenhum pacote chegar por muito tempo, o emissor
        // provavelmente travou/caiu. Abandona a sessão em vez de bloquear o Receptor
        // para sempre, permitindo que ele volte a aguardar um novo handshake.
        socket.setSoTimeout(IDLE_TIMEOUT_MS);

        try (RandomAccessFile raf = new RandomAccessFile(arquivoDestino, "rw")) {
            raf.setLength(tamanhoArquivo);

            byte[] buffer = new byte[Pacote.MAX_PACKET_SIZE];

            while (expectedSeqNum < totalPacotesEsperados) {
                DatagramPacket recv = new DatagramPacket(buffer, buffer.length);
                try {
                    socket.receive(recv);
                } catch (SocketTimeoutException e) {
                    System.out.println();
                    System.out.println("[SESSÃO ABANDONADA] nenhum pacote recebido em " +
                            (IDLE_TIMEOUT_MS / 1000) + "s; o emissor provavelmente foi encerrado. " +
                            "Voltando a aguardar um novo handshake.");
                    socket.setSoTimeout(0);
                    return;
                }
                Pacote p = Pacote.deserializar(recv.getData(), recv.getLength());

                if (p.tipo == Pacote.TIPO_HANDSHAKE) {
                    // Reenvio do handshake (ACK anterior pode ter se perdido) ou um NOVO
                    // emissor iniciando outra sessão. Não há como distinguir com segurança
                    // nem retomar a sessão atual: abandona-a e deixa o laço externo tratar
                    // o handshake como o início de uma nova sessão.
                    System.out.println();
                    System.out.println("[SESSÃO ABANDONADA] handshake recebido durante sessão em andamento.");
                    enviarAck(socket, Pacote.ACK_HANDSHAKE, recv.getAddress(), recv.getPort());
                    socket.setSoTimeout(0);
                    processarSessao(socket, p, recv.getAddress(), recv.getPort(), random);
                    return;
                }
                if (p.tipo != Pacote.TIPO_DATA) {
                    continue;
                }

                if (p.numSeq == expectedSeqNum) {
                    // Pacote em ordem: sorteia a perda simulada (Seção 4 do enunciado).
                    double r = random.nextDouble();
                    if (r < probPerda) {
                        perdidosSimulados++;
                        System.out.printf("%n[PERDA SIMULADA] pacote %d descartado (r=%.3f < p=%.3f)%n",
                                p.numSeq, r, probPerda);
                        // Não envia ACK -> o emissor retransmitirá após o timeout.
                    } else {
                        int offset = p.numSeq * PAYLOAD_SIZE;
                        raf.seek(offset);
                        raf.write(p.dados);
                        recebidosOk++;
                        ultimoAckEnviado = p.numSeq;
                        enviarAck(socket, p.numSeq, recv.getAddress(), recv.getPort());
                        expectedSeqNum++;

                        if (recebidosOk % 20 == 0 || expectedSeqNum == totalPacotesEsperados) {
                            System.out.printf("\rRecebidos: %d/%d pacotes (%.1f%%)   ",
                                    expectedSeqNum, totalPacotesEsperados,
                                    100.0 * expectedSeqNum / totalPacotesEsperados);
                        }
                    }
                } else {
                    // Fora de ordem: descarta e reenvia o último ACK válido (se já houver um).
                    foraDeOrdem++;
                    if (ultimoAckEnviado >= 0) {
                        enviarAck(socket, ultimoAckEnviado, recv.getAddress(), recv.getPort());
                    }
                }
            }
        }

        System.out.println();
        System.out.println("Arquivo recebido por completo. Aguardando pacote FIN...");

        // Aguarda o FIN; pode haver retransmissões tardias de DATA já confirmados, que são ignoradas.
        byte[] buffer = new byte[Pacote.MAX_PACKET_SIZE];
        boolean finRecebido = false;
        socket.setSoTimeout(5000);
        while (!finRecebido) {
            try {
                DatagramPacket recv = new DatagramPacket(buffer, buffer.length);
                socket.receive(recv);
                Pacote p = Pacote.deserializar(recv.getData(), recv.getLength());
                if (p.tipo == Pacote.TIPO_FIN) {
                    finRecebido = true;
                    enviarAck(socket, Pacote.ACK_FIN, recv.getAddress(), recv.getPort());
                } else if (p.tipo == Pacote.TIPO_DATA && ultimoAckEnviado >= 0) {
                    enviarAck(socket, ultimoAckEnviado, recv.getAddress(), recv.getPort());
                }
            } catch (SocketTimeoutException e) {
                System.out.println("Timeout aguardando FIN; encerrando sessão mesmo assim.");
                break;
            }
        }
        socket.setSoTimeout(0);

        long duracaoMs = System.currentTimeMillis() - inicio;

        // Verificação de integridade via MD5 (requisito desejável R9).
        String resultadoIntegridade;
        try {
            String hashRecebido = Utils.calcularMD5(arquivoDestino);
            resultadoIntegridade = hashRecebido.equalsIgnoreCase(hashOrigem)
                    ? "OK (hashes idênticos)"
                    : "FALHOU! hashes diferentes (origem=" + hashOrigem + ", recebido=" + hashRecebido + ")";
        } catch (Exception e) {
            resultadoIntegridade = "erro ao calcular hash: " + e.getMessage();
        }

        long totalEventosSorteio = recebidosOk + perdidosSimulados;
        double taxaPerdaEfetiva = totalEventosSorteio == 0 ? 0.0
                : (double) perdidosSimulados / totalEventosSorteio;

        System.out.println();
        System.out.println("=========== ESTATÍSTICAS DA SESSÃO ===========");
        System.out.println("Pacotes recebidos corretamente    : " + recebidosOk);
        System.out.println("Pacotes perdidos (simulados)      : " + perdidosSimulados);
        System.out.println("Pacotes fora de ordem descartados : " + foraDeOrdem);
        System.out.printf ("Taxa de perda efetiva              : %.2f%% (configurada: %.2f%%)%n",
                taxaPerdaEfetiva * 100, probPerda * 100);
        System.out.println("Tempo total da sessão              : " + duracaoMs + " ms");
        System.out.println("Verificação de integridade (MD5)   : " + resultadoIntegridade);
        System.out.println("Arquivo salvo em                   : " + arquivoDestino.getAbsolutePath());
        System.out.println("================================================");
    }

    private static void enviarAck(DatagramSocket socket, int numAck, InetAddress destino, int porta)
            throws IOException {
        Pacote ack = Pacote.criarAck(numAck);
        socket.send(ack.paraDatagramPacket(destino, porta));
    }
}
