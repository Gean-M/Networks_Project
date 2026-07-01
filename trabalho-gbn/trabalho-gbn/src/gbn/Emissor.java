package gbn;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Módulo Emissor do protocolo Go-Back-N (Seção 3.3 do enunciado).
 *
 * Implementa a FSM do emissor com os dois estados descritos no livro
 * (Kurose & Ross, Fig. 3.20): "aguardando chamada de cima" (janela com
 * espaço disponível) e "janela cheia / temporizador ativo" (transmite
 * dentro da janela e trata timeout/ACKs). Usa duas threads concorrentes:
 * a thread principal envia segmentos, e uma thread dedicada escuta ACKs.
 * Um único temporizador (java.util.Timer) cuida do pacote mais antigo
 * não confirmado (base), reiniciado a cada ACK que avança a janela.
 *
 * Uso:
 *   java Emissor <arquivo_origem> <IP_destino>:<path_destino> <tamanho_janela> <prob_perda> [porta_destino]
 */
public class Emissor {

    private static final int PAYLOAD_SIZE = Pacote.MAX_PAYLOAD;
    private static final int TIMEOUT_MS = 1000;            // temporizador da FSM do GBN
    private static final int HANDSHAKE_TIMEOUT_MS = 2000;
    private static final int HANDSHAKE_MAX_TENTATIVAS = 5;
    private static final int FIN_TIMEOUT_MS = 1500;
    private static final int FIN_MAX_TENTATIVAS = 5;
    private static final int PORTA_PADRAO_RECEPTOR = 5000;

    // ---- estado protegido por "lock": base, nextSeqNum, buffer da janela e o temporizador ----
    private final Object lock = new Object();
    private int base = 0;
    private int nextSeqNum = 0;
    private int totalPacotes;
    private Pacote[] janelaBuffer;
    private Timer timer;

    // ---- confirmação do FIN, tratada separadamente do fluxo principal de ACKs ----
    private final Object finLock = new Object();
    private volatile boolean finAckRecebido = false;

    private DatagramSocket socket;
    private InetAddress enderecoDestino;
    private int portaDestino;
    private int windowSize;

    // ---- estatísticas ----
    private final AtomicLong pacotesEnviados = new AtomicLong(0); // inclui retransmissões
    private final AtomicLong retransmissoes = new AtomicLong(0);
    private final AtomicLong acksRecebidos = new AtomicLong(0);

    public static void main(String[] args) {
        // Garante acentuação correta no console independentemente do local e do SO.
        System.setOut(new PrintStream(System.out, true, StandardCharsets.UTF_8));
        System.setErr(new PrintStream(System.err, true, StandardCharsets.UTF_8));

        if (args.length < 4) {
            System.err.println("Uso: java gbn.Emissor <arquivo_origem> <IP_destino>:<path_destino> <tamanho_janela> <prob_perda> [porta_destino]");
            System.err.println("Exemplo: java gbn.Emissor foto.jpg 192.168.0.10:/tmp/foto_recebida.jpg 8 0.10");
            System.exit(1);
        }
        try {
            new Emissor().executar(args);
        } catch (Exception e) {
            System.err.println("Erro fatal: " + e.getMessage());
            System.exit(1);
        }
    }

    private void executar(String[] args) throws Exception {
        String caminhoOrigem = args[0];
        String destinoArg = args[1];
        windowSize = Integer.parseInt(args[2]);
        double probPerda = Double.parseDouble(args[3]);
        int portaReceptor = args.length >= 5 ? Integer.parseInt(args[4]) : PORTA_PADRAO_RECEPTOR;

        if (windowSize <= 0) {
            throw new IllegalArgumentException("O tamanho da janela deve ser maior que zero.");
        }
        if (probPerda < 0.0 || probPerda > 1.0) {
            throw new IllegalArgumentException("A probabilidade de perda deve estar entre 0.0 e 1.0.");
        }

        int idx = destinoArg.indexOf(':');
        if (idx < 0) {
            throw new IllegalArgumentException("Formato esperado <IP_destino>:<path_destino>, recebido: " + destinoArg);
        }
        String ipDestino = destinoArg.substring(0, idx);
        String caminhoDestino = destinoArg.substring(idx + 1);

        File arquivoOrigem = new File(caminhoOrigem);
        if (!arquivoOrigem.exists() || !arquivoOrigem.isFile()) {
            throw new FileNotFoundException("Arquivo de origem não encontrado: " + caminhoOrigem);
        }

        byte[] conteudo = Files.readAllBytes(arquivoOrigem.toPath());
        long tamanhoArquivo = conteudo.length;
        totalPacotes = (int) Math.ceil(tamanhoArquivo / (double) PAYLOAD_SIZE);
        janelaBuffer = new Pacote[windowSize];

        String hashOrigem = Utils.calcularMD5(conteudo);

        enderecoDestino = InetAddress.getByName(ipDestino);
        portaDestino = portaReceptor;
        socket = new DatagramSocket();

        System.out.println("=================================================");
        System.out.println(" Emissor Go-Back-N");
        System.out.println("=================================================");
        System.out.println("Arquivo de origem  : " + arquivoOrigem.getAbsolutePath());
        System.out.println("Tamanho             : " + Utils.formatarBytes(tamanhoArquivo));
        System.out.println("Destino             : " + ipDestino + ":" + portaReceptor + " -> " + caminhoDestino);
        System.out.println("Janela (N)          : " + windowSize);
        System.out.printf ("Prob. de perda      : %.1f%%%n", probPerda * 100);
        System.out.println("Total de pacotes    : " + totalPacotes);
        System.out.println("Hash MD5 (origem)   : " + hashOrigem);
        System.out.println();

        if (totalPacotes == 0) {
            System.out.println("Arquivo vazio: nada a transmitir além do handshake/FIN.");
        }

        if (!realizarHandshake(caminhoDestino, tamanhoArquivo, probPerda, hashOrigem)) {
            System.err.println("Não foi possível confirmar o handshake com o receptor. Abortando.");
            socket.close();
            return;
        }

        long inicio = System.currentTimeMillis();

        Thread ackListener = new Thread(this::escutarAcks, "ACK-Listener");
        ackListener.setDaemon(true);
        ackListener.start();

        enviarTodosOsPacotes(conteudo);

        // Aguarda confirmação de todos os pacotes (base alcança totalPacotes).
        synchronized (lock) {
            while (base < totalPacotes) {
                lock.wait();
            }
            pararTemporizador();
        }

        long duracaoMs = System.currentTimeMillis() - inicio;

        enviarFinComConfirmacao();
        socket.close();

        double segundos = duracaoMs / 1000.0;
        double throughputKBs = segundos > 0 ? (tamanhoArquivo / 1024.0) / segundos : tamanhoArquivo / 1024.0;

        System.out.println();
        System.out.println("=========== ESTATÍSTICAS DA TRANSMISSÃO ===========");
        System.out.println("Pacotes de dados (distintos)  : " + totalPacotes);
        System.out.println("Pacotes enviados (c/ retrans.): " + pacotesEnviados.get());
        System.out.println("Retransmissões                : " + retransmissoes.get());
        System.out.println("ACKs recebidos                : " + acksRecebidos.get());
        System.out.println("Tempo total                   : " + duracaoMs + " ms");
        System.out.printf ("Throughput estimado            : %.2f KB/s%n", throughputKBs);
        System.out.println("=====================================================");
    }

    // ---------------------------------------------------------------
    // Handshake
    // ---------------------------------------------------------------

    private boolean realizarHandshake(String caminhoDestino, long tamanhoArquivo,
                                       double probPerda, String hashOrigem) throws IOException {
        String payload = String.join("|",
                caminhoDestino,
                Long.toString(tamanhoArquivo),
                Double.toString(probPerda),
                hashOrigem);
        Pacote handshake = Pacote.criarHandshake(payload);
        DatagramPacket dp = handshake.paraDatagramPacket(enderecoDestino, portaDestino);

        byte[] buffer = new byte[Pacote.MAX_PACKET_SIZE];
        socket.setSoTimeout(HANDSHAKE_TIMEOUT_MS);

        for (int tentativa = 1; tentativa <= HANDSHAKE_MAX_TENTATIVAS; tentativa++) {
            System.out.println("Enviando handshake (tentativa " + tentativa + "/" + HANDSHAKE_MAX_TENTATIVAS + ")...");
            socket.send(dp);
            try {
                DatagramPacket recv = new DatagramPacket(buffer, buffer.length);
                socket.receive(recv);
                Pacote resposta = Pacote.deserializar(recv.getData(), recv.getLength());
                if (resposta.tipo == Pacote.TIPO_ACK && resposta.numAck == Pacote.ACK_HANDSHAKE) {
                    System.out.println("Handshake confirmado pelo receptor.");
                    socket.setSoTimeout(0);
                    return true;
                }
            } catch (SocketTimeoutException e) {
                // tenta novamente
            }
        }
        socket.setSoTimeout(0);
        return false;
    }

    // ---------------------------------------------------------------
    // Envio dos segmentos de dados (thread principal)
    // ---------------------------------------------------------------

    private void enviarTodosOsPacotes(byte[] conteudo) throws IOException {
        for (int seq = 0; seq < totalPacotes; seq++) {
            synchronized (lock) {
                while (nextSeqNum >= base + windowSize) {
                    try {
                        lock.wait();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
                int offset = seq * PAYLOAD_SIZE;
                int tamanho = Math.min(PAYLOAD_SIZE, conteudo.length - offset);
                Pacote pacote = Pacote.criarData(seq, conteudo, offset, tamanho);
                janelaBuffer[seq % windowSize] = pacote;
                enviarPacote(pacote);
                pacotesEnviados.incrementAndGet();
                if (base == nextSeqNum) {
                    iniciarTemporizador();
                }
                nextSeqNum++;
            }
            if ((seq + 1) % 20 == 0 || seq == totalPacotes - 1) {
                System.out.printf("\rEnviados: %d/%d | ACKs: %d | Retransmissões: %d   ",
                        seq + 1, totalPacotes, acksRecebidos.get(), retransmissoes.get());
            }
        }
    }

    private void enviarPacote(Pacote pacote) throws IOException {
        socket.send(pacote.paraDatagramPacket(enderecoDestino, portaDestino));
    }

    // ---------------------------------------------------------------
    // Recepção de ACKs (thread dedicada)
    // ---------------------------------------------------------------

    private void escutarAcks() {
        byte[] buffer = new byte[Pacote.MAX_PACKET_SIZE];
        while (true) {
            try {
                DatagramPacket recv = new DatagramPacket(buffer, buffer.length);
                socket.receive(recv);
                Pacote pacote = Pacote.deserializar(recv.getData(), recv.getLength());
                if (pacote.tipo != Pacote.TIPO_ACK) {
                    continue;
                }

                if (pacote.numAck == Pacote.ACK_FIN) {
                    synchronized (finLock) {
                        finAckRecebido = true;
                        finLock.notifyAll();
                    }
                    continue;
                }
                if (pacote.numAck < 0) {
                    continue; // outras sentinelas (ex.: handshake reenviado) não tratadas aqui
                }

                acksRecebidos.incrementAndGet();
                synchronized (lock) {
                    // ACK cumulativo: confirma todos os pacotes até numAck.
                    if (pacote.numAck + 1 > base) {
                        base = pacote.numAck + 1;
                        if (base == nextSeqNum) {
                            pararTemporizador();
                        } else {
                            iniciarTemporizador(); // reinicia para o novo "base"
                        }
                        lock.notifyAll();
                    }
                    // ACKs duplicados/atrasados (numAck+1 <= base) são ignorados, como no GBN clássico.
                }
            } catch (IOException e) {
                break; // socket fechado -> encerra a thread
            }
        }
    }

    // ---------------------------------------------------------------
    // Temporizador único (para o pacote "base")
    // ---------------------------------------------------------------

    /** Deve ser chamado sempre dentro de synchronized(lock). */
    private void iniciarTemporizador() {
        pararTemporizador();
        timer = new Timer(true);
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                onTimeout();
            }
        }, TIMEOUT_MS);
    }

    /** Deve ser chamado sempre dentro de synchronized(lock). */
    private void pararTemporizador() {
        if (timer != null) {
            timer.cancel();
            timer = null;
        }
    }

    private void onTimeout() {
        synchronized (lock) {
            if (base >= nextSeqNum) {
                return; // nada pendente (condição de corrida entre ACK e disparo do timer)
            }
            System.out.printf("%n[TIMEOUT] Retransmitindo pacotes %d até %d%n", base, nextSeqNum - 1);
            for (int seq = base; seq < nextSeqNum; seq++) {
                try {
                    enviarPacote(janelaBuffer[seq % windowSize]);
                    pacotesEnviados.incrementAndGet();
                    retransmissoes.incrementAndGet();
                } catch (IOException e) {
                    System.err.println("Erro ao retransmitir pacote " + seq + ": " + e.getMessage());
                }
            }
            iniciarTemporizador(); // reinicia o temporizador para a nova rodada
        }
    }

    // ---------------------------------------------------------------
    // Encerramento (FIN)
    // ---------------------------------------------------------------

    private void enviarFinComConfirmacao() throws IOException {
        Pacote fin = Pacote.criarFin(totalPacotes);
        int tentativas = 0;

        while (!finAckRecebido && tentativas < FIN_MAX_TENTATIVAS) {
            tentativas++;
            System.out.println("Enviando FIN (tentativa " + tentativas + "/" + FIN_MAX_TENTATIVAS + ")...");
            enviarPacote(fin);
            synchronized (finLock) {
                if (!finAckRecebido) {
                    try {
                        finLock.wait(FIN_TIMEOUT_MS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }
        if (finAckRecebido) {
            System.out.println("FIN confirmado pelo receptor.");
        } else {
            System.out.println("Aviso: FIN não confirmado após " + FIN_MAX_TENTATIVAS + " tentativas.");
        }
    }
}
