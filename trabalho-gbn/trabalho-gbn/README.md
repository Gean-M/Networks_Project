# Trabalho Final — Redes de Computadores
## Implementação do Protocolo Go-Back-N em Java via UDP

Implementação dos módulos **Emissor** e **Receptor** do protocolo de
transferência confiável **Go-Back-N (GBN)**, rodando sobre sockets UDP
(`DatagramSocket` / `DatagramPacket`), conforme especificado no enunciado do
trabalho (Kurose & Ross, Cap. 3).

---

## 1. Estrutura do projeto

```
trabalho-gbn/
├── src/gbn/
│   ├── Pacote.java     # formato do datagrama GBN (serialização via ByteBuffer)
│   ├── Utils.java      # hash MD5 (R9) e formatação de tamanhos
│   ├── Receptor.java   # FSM do receptor GBN + simulação de perda + estatísticas
│   └── Emissor.java    # FSM do emissor GBN (janela deslizante, timer, threads)
├── testes/
│   └── executar_testes.sh   # bateria de testes variando N e prob_perda (R8)
└── README.md
```

## 2. Compilação

Requer apenas o JDK padrão (testado com Java 21, mas compatível com
qualquer JDK ≥ 8). Nenhuma dependência externa.

```bash
cd trabalho-gbn
javac -d out src/gbn/*.java
```

Isso gera os `.class` em `out/gbn/`.

## 3. Execução

### 3.1 Receptor

Deve ser iniciado **antes** do Emissor.

```bash
java -cp out gbn.Receptor [porta]
```

- `porta` é opcional (padrão: **5000**).
- O Receptor roda em loop, aceitando múltiplas transferências sequenciais
  sem precisar ser reiniciado entre testes.

Exemplo:
```bash
java -cp out gbn.Receptor 5000
```

### 3.2 Emissor

```bash
java -cp out gbn.Emissor <arquivo_origem> <IP_destino>:<path_destino> <tamanho_janela> <prob_perda> [porta_destino]
```

- `arquivo_origem`: caminho do arquivo local a transferir.
- `IP_destino:path_destino`: IP do Receptor e caminho **absoluto** onde o
  arquivo deve ser salvo do lado dele.
- `tamanho_janela`: N (tamanho da janela deslizante GBN).
- `prob_perda`: real entre `0.0` e `1.0` (ex.: `0.10` = 10%).
- `porta_destino`: opcional (padrão: **5000**), deve coincidir com a porta
  em que o Receptor está escutando.

Exemplo (igual ao do enunciado):
```bash
java -cp out gbn.Emissor /home/alice/foto.jpg 192.168.0.10:/tmp/foto_recebida.jpg 8 0.10
```

Teste local (mesma máquina, ambos os terminais):
```bash
# Terminal 1
java -cp out gbn.Receptor 5000

# Terminal 2
java -cp out gbn.Emissor meuarquivo.bin 127.0.0.1:/tmp/recebido.bin 8 0.10
```

> **Dica do enunciado:** comece testando com `prob_perda = 0.0` para validar
> a transferência básica antes de ativar a simulação de perdas.

## 4. Formato do datagrama

Implementado em `Pacote.java`, exatamente conforme a tabela do enunciado
(Seção 3.4), serializado com `ByteBuffer`:

| Campo            | Tamanho       | Descrição                              |
|-------------------|---------------|------------------------------------------|
| `tipo`            | 1 byte        | 0=DATA, 1=ACK, 2=HANDSHAKE, 3=FIN        |
| `num_seq`         | 4 bytes (int) | Número de sequência                      |
| `num_ack`         | 4 bytes (int) | Número de confirmação (em ACKs)          |
| `tamanho_dados`   | 2 bytes (short)| Bytes válidos no payload                |
| `dados`           | até 1024 bytes| Payload                                  |

Pacotes de controle (`HANDSHAKE`) reaproveitam o campo `dados` para
transportar uma string `path_destino|tamanho_arquivo|prob_perda|hash_md5`.

ACKs de controle usam sentinelas no campo `num_ack`:
- `-1` → confirmação do handshake (`ACK_HANDSHAKE`)
- `-2` → confirmação do FIN (`ACK_FIN`)
- `≥0` → ACK cumulativo normal de dados

## 5. Decisões de projeto

- **Threads do Emissor**: a thread principal envia segmentos respeitando a
  janela deslizante (bloqueando com `wait()`/`notifyAll()` quando a janela
  está cheia); uma thread dedicada (`ACK-Listener`) escuta ACKs e avança a
  variável `base`. O acesso a `base`, `nextSeqNum` e ao buffer da janela é
  protegido por um único monitor (`lock`), evitando condições de corrida.
- **Temporizador único**: conforme a FSM do livro (Fig. 3.20), há **um único**
  `java.util.Timer` associado ao pacote mais antigo não confirmado (`base`).
  Ele é cancelado quando `base == nextSeqNum` (nada pendente) e
  reiniciado a cada ACK que avança a janela ou a cada timeout (que
  retransmite todos os pacotes de `base` até `nextSeqNum-1`).
- **Buffer circular**: os pacotes em trânsito ficam num array de tamanho N
  indexado por `seq % N`, evitando reler o arquivo do disco a cada
  retransmissão (conforme sugerido no enunciado).
- **Handshake e FIN confiáveis**: como são pacotes de controle, não sofrem a
  perda simulada (que só se aplica a pacotes DATA recebidos em ordem,
  conforme a Seção 4 do enunciado). Mesmo assim, são enviados com
  confirmação e retentativas (até 5 tentativas) para tolerar perdas reais
  de rede, não apenas as simuladas.
- **Sessão "presa"/abandonada**: se o Emissor morrer no meio de uma
  transferência, o Receptor teria, em tese, ficado bloqueado para sempre
  esperando o próximo pacote. Para evitar isso, há um timeout de
  inatividade de 30s que abandona a sessão e volta a aguardar um novo
  handshake — útil tanto para uso real quanto para a robustez durante a
  apresentação (o avaliador pode interromper e reiniciar o Emissor sem
  precisar reiniciar o Receptor).
- **Escrita do arquivo**: o Receptor usa `RandomAccessFile.seek(seq * 1024)`
  para gravar cada segmento na posição correta, em vez de um stream
  sequencial — simples e robusto mesmo que a implementação seja estendida
  no futuro (ex.: Selective Repeat, fora do escopo aqui).
- **Hash MD5 (R9)**: o Emissor calcula o MD5 do arquivo original e o envia
  no handshake; o Receptor recalcula o MD5 do arquivo salvo ao final e
  compara, reportando o resultado nas estatísticas.
- **Codificação do console**: `System.out`/`System.err` são explicitamente
  configurados para UTF-8 no início de cada `main`, evitando caracteres
  acentuados corrompidos em terminais com locale diferente (especialmente
  relevante no Windows).

## 6. Limitações conhecidas / simplificações

- O número de sequência usa um `int` de 32 bits sem wraparound (em vez de
  um espaço de *k* bits módulo `2^k`, como descrito de forma abstrata na
  Seção 2.1 do enunciado). Isso é equivalente na prática (o enunciado já
  fixa `num_seq` como `int` de 4 bytes na Seção 3.4) e suporta arquivos de
  até ~2 bilhões de segmentos (muito acima de qualquer caso de uso real
  aqui).
- O parser do handshake usa `|` como delimitador; caminhos de destino que
  contenham `|` no nome não são suportados (limitação aceitável para os
  testes do trabalho).
- Não há suporte a múltiplos Emissores simultâneos para o mesmo Receptor
  (o Receptor atende uma sessão por vez, sequencialmente — não era exigido
  o contrário).

## 7. O que ainda falta (próximas iterações)

O código-fonte completo do Emissor e do Receptor está implementado,
compilado e testado (veja a Seção 8). Ainda faltam, para a entrega
completa pedida no enunciado:

- [ ] **Relatório técnico em PDF** (mínimo 3 páginas) com decisões de
      projeto, dificuldades, testes realizados e gráficos/tabelas
      comparando diferentes N e probabilidades de perda. O script
      `testes/executar_testes.sh` já automatiza a coleta desses dados em
      CSV — falta rodá-lo no ambiente final e montar os gráficos/tabelas.
- [ ] **Repositório Git** (GitHub/GitLab) com este código e o link enviado
      pelo sistema da instituição.
- [ ] Testar em **duas máquinas reais** na rede (não apenas localhost),
      como será exigido na apresentação ao vivo.
- [ ] Definir e ensaiar a demonstração ao vivo (arquivo ≥ 1 MB, perda de
      10%, conforme a Seção 6 do enunciado).

## 8. Testes já realizados

Durante o desenvolvimento, o projeto foi validado localmente (emissor e
receptor em `127.0.0.1`, em portas diferentes):

- Transferência de arquivo de 1.46 MB com `prob_perda = 0.0`: 1500/1500
  pacotes, 0 retransmissões, hash MD5 idêntico.
- Transferência do mesmo arquivo com `prob_perda = 0.10` e `N = 8`:
  1500/1500 pacotes recebidos, ~9-10% de taxa de perda efetiva (compatível
  com a Lei dos Grandes Números), retransmissões ocorrendo corretamente
  via timeout, **hash MD5 idêntico ao original** (verificado também via
  `md5sum`/`cmp` byte a byte, fora do próprio programa).
- Arquivo menor que um segmento (48 bytes): transferido corretamente em
  um único pacote.
- Validação de argumentos de linha de comando inválidos (arquivo
  inexistente, formato de destino sem `:`, janela ≤ 0, probabilidade fora
  de `[0,1]`): todos tratados com mensagens de erro claras, sem stack
  traces.
- Script `testes/executar_testes.sh`: validado com matriz reduzida de N e
  prob_perda, gerando CSV com tempo, retransmissões e throughput por
  combinação.

## 9. Referências

KUROSE, James F.; ROSS, Keith W. *Redes de Computadores: Uma Abordagem
Top-Down*. 8. ed. São Paulo: Pearson, 2021. Capítulo 3, Seções 3.4 e 3.4.3.
