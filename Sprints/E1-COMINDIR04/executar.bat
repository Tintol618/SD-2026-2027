@echo off
rem ============================================================
rem  E1-COMINDIR04 - Detecao de falhas por heartbeat e envio em lote
rem  Compila os 4 projetos, garante o daemon IPFS, arranca
rem  replicas -> lideres -> processador -> produtor e no fim
rem  corre o HeartbeatCheck, o CausalCheck e o PartitionCheck.
rem  (os ensaios 8 e 9 completos estao automatizados em ensaios.sh)
rem ============================================================
setlocal
chcp 65001 >nul
title Executar COMINDIR04
cd /d "%~dp0"

set "BASE=%~dp0"
set "RUN=%BASE%execucao\manual"
set "API=http://127.0.0.1:5001/api/v0"
set "J=java -Xmx128m -XX:+UseSerialGC -Dstdout.encoding=UTF-8"
set N=3

rem ---------- ferramentas ----------
where javac >nul 2>nul || (echo ERRO: javac nao encontrado no PATH. & goto fim)
where ipfs  >nul 2>nul || (echo ERRO: ipfs ^(Kubo^) nao encontrado no PATH. & goto fim)
where curl  >nul 2>nul || (echo ERRO: curl nao encontrado. & goto fim)

rem ---------- modo ----------
echo.
echo  E1-COMINDIR04
echo  -------------
echo   1 - Interativo  (o produtor abre numa janela; escreves as leituras)
echo   2 - Rajada      (rajada 300 10 automatica, como no ponto 8(b))
echo   3 - Figura 1    (S01 21.0, S04 22.5, LOTE vazio, S01 21.4 e falha do lider p0, como no ponto 8(a))
echo.
choice /c 123 /n /m "Escolhe o modo [1/2/3]: "
set MODO=%errorlevel%

rem ---------- compilar ----------
echo.
echo [1/4] A compilar...
for %%P in (produtor lider replica processador) do (
    javac --release 11 -encoding UTF-8 -d "rep04-%%P\out" rep04-%%P\src\rep04\*.java || (echo ERRO a compilar rep04-%%P & goto fim)
    echo       rep04-%%P ok
)

rem ---------- daemon IPFS ----------
echo [2/4] Daemon IPFS em %API%
curl -s -o nul -X POST "%API%/id" && goto ipfsok
echo       Nao esta a correr: a arrancar numa janela nova...
ipfs config --json Pubsub.Enabled true >nul
start "IPFS daemon" ipfs daemon
set T=0
:esperaipfs
ping -n 2 127.0.0.1 >nul
curl -s -o nul -X POST "%API%/id" && goto ipfsok
set /a T+=1
if %T% lss 30 goto esperaipfs
echo ERRO: o daemon nao respondeu em 30 s.
goto fim
:ipfsok
echo       daemon ok

rem ---------- pasta de execucao ----------
if not exist "%RUN%" goto novapasta
if %MODO%==3 goto limpar
choice /c SN /n /m "A pasta execucao\manual ja existe. Apagar os resultados anteriores? [S/N]: "
if errorlevel 2 goto pastaok
:limpar
rmdir /s /q "%RUN%"
:novapasta
mkdir "%RUN%"
:pastaok
echo       ficheiros em %RUN%

rem ---------- arrancar (pausas com ping: funcionam mesmo com a entrada redirecionada) ----------
echo [3/4] Replicas, lideres e processador (uma janela cada)
start "COMINDIR04 R1 (p0,p1)" /D "%RUN%" cmd /k %J% -cp "%BASE%rep04-replica\out" rep04.PartitionReplica 1 0,1
start "COMINDIR04 R2 (p1,p2)" /D "%RUN%" cmd /k %J% -cp "%BASE%rep04-replica\out" rep04.PartitionReplica 2 1,2
start "COMINDIR04 R3 (p2,p0)" /D "%RUN%" cmd /k %J% -cp "%BASE%rep04-replica\out" rep04.PartitionReplica 3 2,0
ping -n 5 127.0.0.1 >nul
start "COMINDIR04 Lider p0" /D "%RUN%" cmd /k %J% -cp "%BASE%rep04-lider\out" rep04.PartitionLeader 0 %N%
start "COMINDIR04 Lider p1" /D "%RUN%" cmd /k %J% -cp "%BASE%rep04-lider\out" rep04.PartitionLeader 1 %N%
start "COMINDIR04 Lider p2" /D "%RUN%" cmd /k %J% -cp "%BASE%rep04-lider\out" rep04.PartitionLeader 2 %N%
ping -n 5 127.0.0.1 >nul
start "COMINDIR04 Processador" /D "%RUN%" cmd /k %J% -cp "%BASE%rep04-processador\out" rep04.AlarmProcessor %N%
ping -n 5 127.0.0.1 >nul

set "PROD=%J% -cp "%BASE%rep04-produtor\out" rep04.Producer %N%"
echo [4/4] Produtor
if %MODO%==2 goto rajada
if %MODO%==3 goto figura

start "COMINDIR04 Produtor" /D "%RUN%" cmd /k %PROD%
echo.
echo  Na janela do Produtor escreve, por exemplo:
echo     S02 29.3          (leitura; ^>= 28 gera alarme)
echo     rajada 300 10     (300 leituras de S01..S10, 10 ms entre elas)
echo     sair
echo  Para provocar uma falha, fecha a janela de um lider e acrescenta
echo  "p^<k^>;^<instante em ms^>" a execucao\manual\falhas.txt.
echo.
echo  Quando acabares, volta aqui e carrega numa tecla para as verificacoes.
pause >nul
goto verificar

:rajada
pushd "%RUN%"
(echo rajada 300 10& echo sair) | %PROD%
popd
goto verificar

:figura
rem S01 e S04 estao em p0; depois de um LOTE vazio, S01 21.4 fica pendente e o lider p0 e terminado logo a seguir
pushd "%RUN%"
(echo S01 21.0& ping -n 3 127.0.0.1 >nul& echo S04 22.5& ping -n 12 127.0.0.1 >nul& echo S01 21.4& echo sair) | %PROD%
popd
for /f %%T in ('powershell -NoProfile -Command "[DateTimeOffset]::Now.ToUnixTimeMilliseconds()"') do echo p0;%%T>> "%RUN%\falhas.txt"
taskkill /FI "WINDOWTITLE eq COMINDIR04 Lider p0*" /T /F >nul 2>nul
echo Lider p0 terminado. A aguardar 18 s pela SUSPEITA nas replicas...
ping -n 19 127.0.0.1 >nul
start "COMINDIR04 Lider p0" /D "%RUN%" cmd /k %J% -cp "%BASE%rep04-lider\out" rep04.PartitionLeader 0 %N%
echo Lider p0 reiniciado: confirma REINICIO, RETOMA e RECUPERACAO nas replicas 1 e 3.
goto verificar

rem ---------- verificacoes ----------
:verificar
echo.
echo A aguardar 11 s (dois periodos de heartbeat) para os ultimos lotes chegarem...
ping -n 12 127.0.0.1 >nul
echo.
echo ================= HeartbeatCheck ==============
set "FALHAS="
if exist "%RUN%\falhas.txt" set "FALHAS=%RUN%\falhas.txt"
%J% -cp "%BASE%rep04-replica\out" rep04.HeartbeatCheck %N% "%RUN%" %FALHAS%
echo.
echo ================= CausalCheck =================
%J% -cp "%BASE%rep04-replica\out" rep04.CausalCheck %N% "%RUN%"
echo.
echo ================= PartitionCheck ==============
%J% -cp "%BASE%rep04-replica\out" rep04.PartitionCheck %N% "%RUN%"
echo.
choice /c SN /n /m "Fechar as janelas das replicas, lideres, processador e produtor? [S/N]: "
if errorlevel 2 goto fim
taskkill /FI "WINDOWTITLE eq COMINDIR04*" /T /F >nul 2>nul
echo Janelas fechadas (o daemon IPFS continua a correr).

:fim
echo.
pause
endlocal
