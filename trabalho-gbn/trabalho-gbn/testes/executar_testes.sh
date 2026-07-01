#!/usr/bin/env bash
#
# executar_testes.sh
#
# Roda uma bateria de testes do Emissor/Receptor GBN variando o tamanho da
# janela (N) e a probabilidade de perda (p), registrando os resultados em
# um CSV. Serve de ponto de partida para a análise pedida no requisito
# desejável R8 (impacto de N no tempo de transferência) e para os
# gráficos/tabelas do relatório técnico.
#
# Uso:
#   ./executar_testes.sh [arquivo_teste] [porta]
#
# Requisitos: bash, javac/java no PATH, projeto já com as fontes em src/gbn.
#
# Observação: este script roda tudo em localhost (Receptor e Emissor na
# mesma máquina). Para medir efeitos de rede reais, rode o Receptor em
# outra máquina/VM e ajuste o IP de destino manualmente.

set -uo pipefail

DIR_RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="$DIR_RAIZ/out"
TESTES_DIR="$DIR_RAIZ/testes"
RESULTADOS_DIR="$TESTES_DIR/resultados"
ARQUIVO_TESTE="${1:-$RESULTADOS_DIR/arquivo_teste.bin}"
PORTA="${2:-5500}"
CSV="$RESULTADOS_DIR/resultados.csv"

# Combinações a testar — ajuste livremente conforme a análise desejada.
JANELAS=(1 2 4 8 16 32)
PROBS=(0.0 0.05 0.10 0.20)

mkdir -p "$RESULTADOS_DIR"

echo "=== Compilando projeto ==="
javac -d "$OUT_DIR" "$DIR_RAIZ"/src/gbn/*.java || { echo "Falha na compilação."; exit 1; }

if [ ! -f "$ARQUIVO_TESTE" ]; then
    echo "=== Gerando arquivo de teste de 1 MB em $ARQUIVO_TESTE ==="
    dd if=/dev/urandom of="$ARQUIVO_TESTE" bs=1024 count=1024 status=none
fi

echo "=== Iniciando Receptor na porta $PORTA ==="
java -cp "$OUT_DIR" gbn.Receptor "$PORTA" > "$RESULTADOS_DIR/receptor.log" 2>&1 &
PID_RECEPTOR=$!
sleep 1

# Garante que o Receptor seja encerrado ao final do script, mesmo em erro/Ctrl+C.
trap 'kill "$PID_RECEPTOR" 2>/dev/null' EXIT

echo "janela,prob_perda,tempo_ms,pacotes_enviados,retransmissoes,acks_recebidos,throughput_kbs" > "$CSV"

for N in "${JANELAS[@]}"; do
    for P in "${PROBS[@]}"; do
        DESTINO="$RESULTADOS_DIR/recebido_N${N}_p${P}.bin"
        LOG="$RESULTADOS_DIR/emissor_N${N}_p${P}.log"
        echo ">>> Testando N=$N, prob_perda=$P ..."

        java -cp "$OUT_DIR" gbn.Emissor "$ARQUIVO_TESTE" "127.0.0.1:$DESTINO" "$N" "$P" "$PORTA" > "$LOG" 2>&1

        TEMPO=$(grep -oP 'Tempo total\s*:\s*\K[0-9]+' "$LOG" | tail -1)
        ENVIADOS=$(grep -oP 'Pacotes enviados \(c/ retrans\.\)\s*:\s*\K[0-9]+' "$LOG" | tail -1)
        RETRANS=$(grep -oP 'Retransmissões\s*:\s*\K[0-9]+' "$LOG" | tail -1)
        ACKS=$(grep -oP 'ACKs recebidos\s*:\s*\K[0-9]+' "$LOG" | tail -1)
        THROUGHPUT=$(grep -oP 'Throughput estimado\s*:\s*\K[0-9.]+' "$LOG" | tail -1)

        echo "$N,$P,${TEMPO:-NA},${ENVIADOS:-NA},${RETRANS:-NA},${ACKS:-NA},${THROUGHPUT:-NA}" >> "$CSV"

        # Confere integridade rapidamente (não interrompe o lote em caso de falha,
        # apenas registra um aviso).
        if command -v md5sum >/dev/null && [ -f "$DESTINO" ]; then
            ORIG_HASH=$(md5sum "$ARQUIVO_TESTE" | awk '{print $1}')
            DEST_HASH=$(md5sum "$DESTINO" | awk '{print $1}')
            if [ "$ORIG_HASH" != "$DEST_HASH" ]; then
                echo "    [AVISO] hash divergente para N=$N, p=$P!"
            fi
        fi
    done
done

echo "=== Concluído. Resultados em: $CSV ==="
