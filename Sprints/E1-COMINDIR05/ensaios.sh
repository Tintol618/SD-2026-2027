#!/usr/bin/env bash
# ============================================================
#  E1-COMINDIR05 - ensaios automáticos (Git Bash, Windows)
#  Uso: ./ensaios.sh <ensaio> [...]
#     a  b  c  d  d-seguidor  e  f1 f2 f3 f4 f5 f6   (ou: todos)
#  Cada ensaio corre numa pasta própria em execucao/<ensaio>,
#  arranca os 9 nós (3 por partição), o processador e o produtor,
#  e no fim corre RaftCheck e CausalCheck (verificacao.txt).
# ============================================================
set -u
cd "$(dirname "$0")"
BASE="$(pwd -W 2>/dev/null || pwd)"
N=3
command -v javac > /dev/null || export PATH="/d/androidstudio/jbr/bin:$PATH" # JDK do Android Studio, se não houver outro
J="java -Xmx96m -XX:+UseSerialGC -Dstdout.encoding=UTF-8"

compilar() {
    for p in produtor no processador; do
        javac --release 11 -encoding UTF-8 -d "raft04-$p/out" raft04-$p/src/raft04/*.java || exit 1
    done
}

agora() { date +%s%3N; }
espera() { sleep "$1"; }
log() { echo "  [$(date +%T)] $*"; }

declare -A PID
winpid() { cat "/proc/$1/winpid" 2>/dev/null || echo "$1"; }

lanca() { # nome props projeto classe args...
    local nome=$1 props=$2 proj=$3 cls=$4; shift 4
    (cd "$RUN" && exec $J $props -cp "$BASE/raft04-$proj/out" "raft04.$cls" "$@" >> "$nome.log" 2>&1) &
    PID[$nome]=$!
}

no() { lanca "no-p$1-n$2" "${3:-$PROPS_N}" no PartitionNode "$1" "$2" $N; }

# termina o nó sem lhe dar hipótese de reagir e anota a falha em falhas.txt
mata_no() {
    local w; w=$(winpid "${PID[no-p$1-n$2]}")
    echo "p$1;n$2;$(agora)" >> "$RUN/falhas.txt"
    taskkill //F //T //PID "$w" > /dev/null 2>&1 || kill -9 "${PID[no-p$1-n$2]}"
    log "nó n$2 de p$1 terminado (kill -9)"
}

isola()  { touch "$RUN/isolar-p$1-n$2"; echo "isolar;p$1;n$2;$(agora)" >> "$RUN/particoes.txt"; log "nó n$2 de p$1 ISOLADO"; }
repara() { rm -f "$RUN/isolar-p$1-n$2"; echo "reparar;p$1;n$2;$(agora)" >> "$RUN/particoes.txt"; log "nó n$2 de p$1 reposto"; }

produz() { echo "$*" >> "$RUN/produtor.in"; log "produtor: $*"; }

# nó que publicou o LOTE mais recente na partição k (o líder atual, para o observador)
lider_de() {
    local k=$1 best=0 bt=0 f t i
    for i in 1 2 3; do
        f="$RUN/hb-p$k-n$i.txt"; [ -s "$f" ] || continue
        t=$(tail -1 "$f" | cut -d';' -f3)
        [ "$t" -gt "$bt" ] && { bt=$t; best=$i; }
    done
    echo $best
}

# espera por um evento LIDER na partição k posterior a <desde>, no máximo <lim> s
espera_lider() {
    local k=$1 desde=$2 lim=$3 t0; t0=$(date +%s)
    while :; do
        cat "$RUN"/eventos-p$k-n*.txt 2>/dev/null | awk -F';' -v d="$desde" '$2=="LIDER" && $1>d {f=1} END{exit !f}' && return 0
        [ $(( $(date +%s) - t0 )) -ge "$lim" ] && return 1
        sleep 0.2
    done
}

# espera que o líder de k publique um novo LOTE
espera_lote() {
    local k=$1 l f n0; l=$(lider_de "$k"); f="$RUN/hb-p$k-n$l.txt"; n0=$(wc -l < "$f")
    until [ "$(wc -l < "$f")" -gt "$n0" ]; do sleep 0.05; done
}

pasta() { RUN="$BASE/execucao/$1"; rm -rf "$RUN"; mkdir -p "$RUN"; : > "$RUN/produtor.in"; echo "=== ensaio $1 ($(date +%T)) [$PROPS_N]"; }

participantes() {
    lanca processador "" processador AlarmProcessor $N
    (cd "$RUN" && tail -s 0.1 -f produtor.in | exec $J -cp "$BASE/raft04-produtor/out" raft04.Producer $N > produtor.log 2>&1) &
    PID[produtor]=$!
}

arranque() { # nome, propriedades dos nós
    PROPS_N=${2:-}; pasta "$1"
    local k i
    for k in 0 1 2; do for i in 1 2 3; do no $k $i; done; done
    participantes
    local t0; t0=$(agora)
    for k in 0 1 2; do espera_lider $k "$t0" 40 || log "p$k sem líder ao fim de 40 s"; done
    log "líderes: p0=n$(lider_de 0 2>/dev/null) p1=n$(lider_de 1) p2=n$(lider_de 2)"
    espera 2
}

matar_tudo() {
    local x
    for x in "${!PID[@]}"; do taskkill //F //T //PID "$(winpid "${PID[$x]}")" > /dev/null 2>&1; kill -9 "${PID[$x]}" 2>/dev/null; done
    kill $(ps -ef | awk '/tail -s 0.1 -f produtor.in/ && !/awk/{print $2}') 2>/dev/null
    powershell -NoProfile -Command "Get-CimInstance Win32_Process -Filter \"Name='java.exe'\" | Where-Object { \$_.CommandLine -match 'raft04\.(PartitionNode|AlarmProcessor|Producer)' } | ForEach-Object { Stop-Process -Id \$_.ProcessId -Force }" 2>/dev/null
    unset PID; declare -gA PID
    rm -f "$RUN"/isolar-*
    espera 1
}

fim() {
    matar_tudo
    local F=""; [ -s "$RUN/falhas.txt" ] && F="falhas.txt"
    { echo "################ RaftCheck"; $J -cp "$BASE/raft04-no/out" raft04.RaftCheck $N "$RUN" $F
      echo; echo "################ CausalCheck"; $J -cp "$BASE/raft04-no/out" raft04.CausalCheck $N "$RUN"; } > "$RUN/verificacao.txt" 2>&1
    log "verificação em $RUN/verificacao.txt"
}

# ---------------- ensaios

ensaio_a() { # Figura 1 em p1 (S02 e S05 estão em p1)
    arranque a
    local l; l=$(lider_de 1); log "líder de p1: n$l"
    produz "S02 20.1"; espera 2; produz "S05 23.7"
    espera_lote 1; espera 0.3
    produz "S02 20.4"
    until grep -aq "S02 20.4" "$RUN/produtor.log"; do sleep 0.05; done; espera 0.3
    local t0; t0=$(agora); mata_no 1 "$l"
    espera 2; produz "S05 23.9"                        # durante a eleição
    espera_lider 1 "$t0" 30 && log "novo líder de p1: n$(lider_de 1)"
    espera 2; produz "S02 20.6"; espera 3
    no 1 "$l"; log "antigo líder n$l de volta"
    espera 8
    fim
}

ensaio_b() { # 10 arranques por ordens diferentes e 5 com prazo fixo e arranque simultâneo
    local r k i ordem t0
    [ "${1:-}" = fixo ] || for r in $(seq -w 1 10); do
        PROPS_N=""; pasta "b/aleatorio-$r"
        ordem=$(for k in 0 1 2; do for i in 1 2 3; do echo "$k $i"; done; done | shuf)
        while read -r k i; do no "$k" "$i"; espera 0.$((RANDOM % 8)); done <<< "$ordem"
        t0=$(agora); for k in 0 1 2; do espera_lider $k 0 40 || log "p$k sem líder"; done
        espera 1; fim
    done
    for r in $(seq 1 5); do
        # os nove esperam pelo mesmo instante (daqui a 4 s) antes de iniciarem o prazo: arranque simultâneo
        PROPS_N="-Draft04.eleicaoMin=6000 -Draft04.eleicaoMax=6000 -Draft04.inicio=$(( $(agora) + 4000 ))"; pasta "b/fixo-$r"
        for k in 0 1 2; do for i in 1 2 3; do no "$k" "$i"; done; done
        for k in 0 1 2; do espera_lider $k 0 60 || log "p$k sem líder ao fim de 60 s"; done
        espera 1; fim
    done
    PROPS_N=""
}

ensaio_c() { # 3 falhas do líder de p1 durante rajadas, reinício 30 s depois
    arranque c "${1:-}"
    local x l
    for x in 1 2 3; do
        produz "rajada 300 10"; espera 1.5
        l=$(lider_de 1); mata_no 1 "$l"
        espera 30
        no 1 "$l"; log "n$l de volta"
        espera 8
    done
    fim
}

ensaio_d() { # isolamento do líder de p1 durante 30 s, a meio de uma rajada
    arranque "${1:-d}" "${2:-}"
    local l; l=$(lider_de 1)
    produz "rajada 300 10"; espera 1
    isola 1 "$l"
    espera 18                           # entretanto, o lado maioritário elege um líder num termo novo
    produz "rajada 150 10"              # as mesmas leituras chegam aos dois líderes
    espera 12
    repara 1 "$l"
    espera 12
    fim
}

ensaio_d_seguidor() { # isolamento de um seguidor de p1 durante 60 s, sem rajada
    arranque "${1:-d-seguidor}" "${2:-}"
    local l s; l=$(lider_de 1); s=$(( l % 3 + 1 ))
    log "líder n$l, seguidor isolado n$s"
    isola 1 "$s"
    produz "S02 21.0"; espera 20; produz "S05 21.5"; espera 20; produz "S08 22.0"; espera 20
    repara 1 "$s"
    espera 20
    produz "S02 22.5"; espera 4
    fim
}

ensaio_e() { # dois dos três nós de p1 terminados; 30 s de leituras; volta um
    arranque e
    local l s x; l=$(lider_de 1); s=$(( l % 3 + 1 ))
    mata_no 1 "$l"; mata_no 1 "$s"
    for x in $(seq 1 10); do produz "S0$(( (x % 3) * 3 + 2 )) $(( 20 + x )).0"; espera 3; done
    local t0; t0=$(agora)
    no 1 "$s"; log "n$s de volta"
    espera_lider 1 "$t0" 40 && log "líder de p1 ao fim de $(( ($(agora) - t0) / 1000 )) s: n$(lider_de 1)"
    produz "S02 29.9"; espera 4
    fim
}

ensaio_f() {
    case $1 in
        1|6) # maioria errada: prazo fixo e arranque simultâneo, para haver candidatos ao mesmo tempo
            PROPS_N="-Draft04.erro=$1 -Draft04.eleicaoMin=6000 -Draft04.eleicaoMax=6000 -Draft04.inicio=$(( $(agora) + 4000 ))"; pasta "f$1${2:-}"
            local k i; for k in 0 1 2; do for i in 1 2 3; do no "$k" "$i"; done; done
            participantes
            espera 30; produz "rajada 60 50"; espera 10
            fim ;;
        2) # estado só em memória: um seguidor reiniciado logo depois de conceder um voto
            arranque f2 "-Draft04.erro=2"
            local l t0 v="" i; l=$(lider_de 1); t0=$(agora)
            mata_no 1 "$l"
            while [ -z "$v" ]; do       # espera pelo primeiro voto concedido por um dos dois sobreviventes
                for i in 1 2 3; do
                    [ "$i" = "$l" ] && continue
                    awk -F';' -v d="$t0" '$1>d && $2=="VOTO" && /concedido/ {f=1} END{exit !f}' "$RUN/eventos-p1-n$i.txt" 2>/dev/null && { v=$i; break; }
                done
                sleep 0.02
            done
            mata_no 1 "$v"; no 1 "$v"; log "seguidor n$v reiniciado logo após votar"
            espera 25; no 1 "$l"; espera 10
            fim ;;
        3) # sem restrição de voto: um seguidor atrasado (isolado) regressa e candidata-se
            ensaio_d_seguidor f3 "-Draft04.erro=3" ;;
        4) # seguidor que aplica lotes de termo inferior: ensaio 9(d) repetido
            ensaio_d f4 "-Draft04.erro=4" ;;
        5) # novo líder com L e V a zero
            arranque f5 "-Draft04.erro=5"
            produz "rajada 150 10"; espera 4
            local l t0; l=$(lider_de 1); t0=$(agora); mata_no 1 "$l"
            espera_lider 1 "$t0" 30
            produz "rajada 150 10"; espera 6
            fim ;;
    esac
    PROPS_N=""
}

compilar
PROPS_N=""
[ $# -eq 0 ] && set -- todos
[ "$1" = todos ] && set -- a b c d d-seguidor e f1 f2 f3 f4 f5 f6
for e in "$@"; do
    case $e in
        a) ensaio_a ;; b) ensaio_b ;; b-fixo) ensaio_b fixo ;; c) ensaio_c ;; d) ensaio_d ;; d-seguidor) ensaio_d_seguidor ;; e) ensaio_e ;;
        f[1-6]) ensaio_f "${e#f}" ;;
        f6-[0-9]) ensaio_f 6 "-${e#f6-}" ;;
        *) echo "ensaio desconhecido: $e" ;;
    esac
done
