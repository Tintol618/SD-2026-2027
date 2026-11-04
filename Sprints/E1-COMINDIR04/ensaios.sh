#!/usr/bin/env bash
# ============================================================
#  E1-COMINDIR04 - ensaios automáticos (Git Bash, Windows)
#  Uso: ./ensaios.sh <ensaio> [...]
#     a  b  b-parado  c  d  e1000  e10000  f1 f2 f3 f4 f5   (ou: todos)
#  Cada ensaio corre numa pasta própria em execucao/<ensaio>,
#  arranca réplicas -> líderes -> processador -> produtor e no
#  fim corre HeartbeatCheck, CausalCheck e PartitionCheck.
# ============================================================
set -u
cd "$(dirname "$0")"
BASE="$(pwd -W 2>/dev/null || pwd)"
N=3
command -v javac > /dev/null || export PATH="/d/androidstudio/jbr/bin:$PATH" # JDK do Android Studio, se não houver outro
J="java -Xmx128m -XX:+UseSerialGC -Dstdout.encoding=UTF-8"

compilar() {
    for p in produtor lider replica processador; do
        javac --release 11 -encoding UTF-8 -d "rep04-$p/out" rep04-$p/src/rep04/*.java || exit 1
    done
}

agora() { date +%s%3N; }
espera() { sleep "$1"; }

declare -A PID
winpid() { cat "/proc/$1/winpid" 2>/dev/null || echo "$1"; }

# arranca um processo java em segundo plano, na pasta do ensaio
lanca() { # nome props projeto classe args...
    local nome=$1 props=$2 proj=$3 cls=$4; shift 4
    (cd "$RUN" && exec $J $props -cp "$BASE/rep04-$proj/out" "rep04.$cls" "$@" >> "$nome.log" 2>&1) &
    PID[$nome]=$!
}

lider() { lanca "lider-$1" "$PROPS_L" lider PartitionLeader "$1" $N; }

mata() { # termina o líder sem lhe dar hipótese de reagir e anota a falha
    echo "p$2;$(agora)" >> "$RUN/falhas.txt"
    taskkill //F //T //PID "$(winpid "${PID[$1]}")" > /dev/null 2>&1 || kill -9 "${PID[$1]}"
    echo "  [$(date +%T)] $1 terminado (kill -9)"
}

suspende() { powershell -NoProfile -ExecutionPolicy Bypass -File "$BASE/suspender.ps1" "$1" "rep04.PartitionLeader $2 "; }

produz() { echo "$*" >> "$RUN/produtor.in"; echo "  [$(date +%T)] produtor: $*"; }

arranque() { # nome do ensaio, props dos líderes, props dos participantes
    RUN="$BASE/execucao/$1"; PROPS_L=$2; PROPS_P=$3
    rm -rf "$RUN"; mkdir -p "$RUN"; : > "$RUN/produtor.in"
    echo "=== ensaio $1 ($(date +%T)) líderes: [$PROPS_L] participantes: [$PROPS_P]"
    lanca replica1 "$PROPS_P" replica PartitionReplica 1 0,1
    lanca replica2 "$PROPS_P" replica PartitionReplica 2 1,2
    lanca replica3 "$PROPS_P" replica PartitionReplica 3 2,0
    espera 3
    lider 0; lider 1; lider 2
    espera 3
    lanca processador "$PROPS_P" processador AlarmProcessor $N
    espera 2
    (cd "$RUN" && tail -s 0.1 -f produtor.in | exec $J $PROPS_P -cp "$BASE/rep04-produtor/out" rep04.Producer $N > produtor.log 2>&1) &
    PID[produtor]=$!
    espera 2
}

fim() {
    for k in "${!PID[@]}"; do
        taskkill //F //T //PID "$(winpid "${PID[$k]}")" > /dev/null 2>&1
        kill -9 "${PID[$k]}" 2>/dev/null
    done
    kill $(ps -ef | awk '/tail -s 0.1 -f produtor.in/ && !/awk/{print $2}') 2>/dev/null
    # garante que nenhum processo do ensaio fica vivo a publicar no ensaio seguinte
    powershell -NoProfile -Command "Get-CimInstance Win32_Process -Filter \"Name='java.exe'\" | Where-Object { \$_.CommandLine -match 'rep04\.(PartitionLeader|PartitionReplica|AlarmProcessor|Producer)' } | ForEach-Object { Stop-Process -Id \$_.ProcessId -Force }" 2>/dev/null
    unset PID; declare -gA PID
    espera 1
    local F=""; [ -s "$RUN/falhas.txt" ] && F="$RUN/falhas.txt"
    local CP="$BASE/rep04-replica/out"
    { echo "################ HeartbeatCheck"; $J $PROPS_P -cp "$CP" rep04.HeartbeatCheck $N "$RUN" $F
      echo; echo "################ CausalCheck"; $J -cp "$CP" rep04.CausalCheck $N "$RUN"
      echo; echo "################ PartitionCheck"; $J -cp "$CP" rep04.PartitionCheck $N "$RUN"; } > "$RUN/verificacao.txt" 2>&1
    echo "  verificação em $RUN/verificacao.txt"
}

# ---------- ensaios ----------
ensaio_a() { # Figura 1: S01 e S04 estão em p0
    arranque a "" ""
    produz "S01 21.0"; espera 2; produz "S04 22.5"
    espera 9                      # pelo menos um LOTE vazio depois do lote com 1 e 2
    hbs() { cat "$RUN/hb-p$1.txt" 2>/dev/null | wc -l; }
    local h=$(hbs 0); until [ "$(hbs 0)" -gt "$h" ]; do sleep 0.05; done  # acabou de sair um lote: o próximo só daqui a 5 s
    produz "S01 21.4"
    until grep -aq "S01;21.4" "$RUN/produtor.log"; do sleep 0.05; done; espera 0.3  # já chegou ao daemon
    mata lider-0 0
    espera 18                     # suspeita esperada entre 10 s e 16 s depois
    lider 0
    espera 8
    fim
}

ensaio_b() { arranque b "" ""; produz "rajada 300 10"; espera 15; fim; }
ensaio_b_parado() { arranque b-parado "" ""; espera 60; fim; }

ensaio_c() { # 3 falhas de p1 durante rajadas, reinício 30 s depois de cada
    arranque c "${1:-}" "${1:-}"
    for i in 1 2 3; do
        produz "rajada 300 10"; espera 1.5
        mata lider-1 1
        espera 30
        lider 1
        espera 8
    done
    fim
}

ensaio_d() { # pausa de 20 s do líder de p2 com leituras para p2 durante a pausa (S03, S06 e S09 estão em p2)
    arranque d "" ""
    espera 3
    suspende suspend 2; echo "  [$(date +%T)] lider-2 suspenso"; echo "p2-pausa;$(agora)" >> "$RUN/pausas.txt"
    espera 4; produz "S03 20.1"; espera 4; produz "S06 21.2"; espera 4; produz "S09 22.3"; espera 8
    suspende resume 2; echo "  [$(date +%T)] lider-2 retomado"; echo "p2-retoma;$(agora)" >> "$RUN/pausas.txt"
    espera 15
    fim
}

ensaio_e() { # período Δ diferente em todos os processos: (b) seguido de uma execução de (c)
    local P="-Drep04.periodo=$1"
    arranque "e$1" "$P" "$P"
    produz "rajada 300 10"; espera 10
    espera 60                     # produtor parado
    produz "rajada 300 10"; espera 1.5
    mata lider-1 1
    espera 30
    lider 1
    espera $(( $1 / 1000 * 2 + 4 ))
    fim
}

ensaio_f() { # falha deliberada X (1..4 no líder; 5 = participantes com fator 1)
    if [ "$1" = 5 ]; then arranque f5 "" "-Drep04.fator=1"; else arranque "f$1" "-Drep04.erro=$1" ""; fi
    espera 8
    if [ "$1" = 4 ]; then           # a rajada apanha o lote hb=10, que a falha 4 descarta
        until [ "$(cat "$RUN/hb-p0.txt" 2>/dev/null | wc -l)" -ge 9 ]; do sleep 0.1; done; espera 1
    fi
    produz "rajada 300 10"; espera 10
    produz "S01 21.0"; espera 22
    [ "$1" = 5 ] && espera 30
    fim
}

compilar
[ $# -eq 0 ] && set -- todos
[ "$1" = todos ] && set -- a b b-parado c d e1000 e10000 f1 f2 f3 f4 f5
for e in "$@"; do
    case $e in
        a) ensaio_a ;; b) ensaio_b ;; b-parado) ensaio_b_parado ;; c) ensaio_c ;; d) ensaio_d ;;
        e1000) ensaio_e 1000 ;; e5000) ensaio_e 5000 ;; e10000) ensaio_e 10000 ;;
        f[1-5]) ensaio_f "${e#f}" ;;
        *) echo "ensaio desconhecido: $e" ;;
    esac
done
