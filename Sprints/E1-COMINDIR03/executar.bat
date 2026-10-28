@echo off
rem ============================================================
rem  E1-COMINDIR03 - Relogios logicos e vetoriais sobre particoes
rem  Compila os 4 projetos, garante o daemon IPFS, arranca
rem  replicas -> lideres -> processador -> produtor e no fim
rem  corre o CausalCheck e o PartitionCheck.
rem ============================================================
setlocal
chcp 65001 >nul
title Executar COMINDIR03
cd /d "%~dp0"

set "BASE=%~dp0"
set "RUN=%BASE%execucao"
set "API=http://127.0.0.1:5001/api/v0"
set "J=java -Xmx128m -XX:+UseSerialGC -Dstdout.encoding=UTF-8"
set N=3

rem ---------- ferramentas ----------
where javac >nul 2>nul || (echo ERRO: javac nao encontrado no PATH. & goto fim)
where ipfs  >nul 2>nul || (echo ERRO: ipfs ^(Kubo^) nao encontrado no PATH. & goto fim)
where curl  >nul 2>nul || (echo ERRO: curl nao encontrado. & goto fim)

rem ---------- modo ----------
echo.
echo  E1-COMINDIR03
echo  -------------
echo   1 - Interativo  (o produtor abre numa janela; escreves as leituras)
echo   2 - Rajada      (rajada 300 10 automatica, como no ponto 7(c))
echo   3 - Figura 1    (S02 29.3, S01 21.0, S04 22.5, como no ponto 7(a))
echo.
choice /c 123 /n /m "Escolhe o modo [1/2/3]: "
set MODO=%errorlevel%

rem ---------- compilar ----------
echo.
echo [1/4] A compilar...
for %%P in (produtor lider replica processador) do (
    javac --release 11 -encoding UTF-8 -d "rep03-%%P\out" rep03-%%P\src\rep03\*.java || (echo ERRO a compilar rep03-%%P & goto fim)
    echo       rep03-%%P ok
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
choice /c SN /n /m "A pasta execucao ja existe. Apagar os resultados anteriores? [S/N]: "
if errorlevel 2 goto pastaok
:limpar
rmdir /s /q "%RUN%"
:novapasta
mkdir "%RUN%"
:pastaok
echo       ficheiros em %RUN%

rem ---------- arrancar (pausas com ping: funcionam mesmo com a entrada redirecionada) ----------
echo [3/4] Replicas, lideres e processador (uma janela cada)
start "COMINDIR03 R1 (p0,p1)" /D "%RUN%" cmd /k %J% -cp "%BASE%rep03-replica\out" rep03.PartitionReplica 1 0,1
start "COMINDIR03 R2 (p1,p2)" /D "%RUN%" cmd /k %J% -cp "%BASE%rep03-replica\out" rep03.PartitionReplica 2 1,2
start "COMINDIR03 R3 (p2,p0)" /D "%RUN%" cmd /k %J% -cp "%BASE%rep03-replica\out" rep03.PartitionReplica 3 2,0
ping -n 5 127.0.0.1 >nul
start "COMINDIR03 Lider p0" /D "%RUN%" cmd /k %J% -cp "%BASE%rep03-lider\out" rep03.PartitionLeader 0 %N%
start "COMINDIR03 Lider p1" /D "%RUN%" cmd /k %J% -cp "%BASE%rep03-lider\out" rep03.PartitionLeader 1 %N%
start "COMINDIR03 Lider p2" /D "%RUN%" cmd /k %J% -cp "%BASE%rep03-lider\out" rep03.PartitionLeader 2 %N%
ping -n 5 127.0.0.1 >nul
start "COMINDIR03 Processador" /D "%RUN%" cmd /k %J% -cp "%BASE%rep03-processador\out" rep03.AlarmProcessor %N%
ping -n 5 127.0.0.1 >nul

set "PROD=%J% -cp "%BASE%rep03-produtor\out" rep03.Producer %N%"
echo [4/4] Produtor
if %MODO%==2 goto rajada
if %MODO%==3 goto figura

start "COMINDIR03 Produtor" /D "%RUN%" cmd /k %PROD%
echo.
echo  Na janela do Produtor escreve, por exemplo:
echo     S02 29.3          (leitura; ^>= 28 gera alarme)
echo     rajada 300 10     (300 leituras de S01..S10)
echo     sair
echo.
echo  Quando acabares, volta aqui e carrega numa tecla para as verificacoes.
pause >nul
goto verificar

:rajada
(echo rajada 300 10& echo sair) | %PROD%
goto verificar

:figura
rem pausas entre leituras para o alarme de S02 sair antes de o processador ver S01
(echo S02 29.3& ping -n 4 127.0.0.1 >nul& echo S01 21.0& ping -n 3 127.0.0.1 >nul& echo S04 22.5& ping -n 2 127.0.0.1 >nul& echo sair) | %PROD%
goto verificar

rem ---------- verificacoes ----------
:verificar
echo.
echo A aguardar 5 s para os ultimos registos e alarmes chegarem...
ping -n 6 127.0.0.1 >nul
echo.
echo ================= CausalCheck =================
%J% -cp "%BASE%rep03-replica\out" rep03.CausalCheck %N% "%RUN%"
echo.
echo ================= PartitionCheck ==============
%J% -cp "%BASE%rep03-replica\out" rep03.PartitionCheck %N% "%RUN%"
echo.
echo Vista total por (L, k): %RUN%\vista-lamport.txt
echo.
choice /c SN /n /m "Fechar as janelas das replicas, lideres, processador e produtor? [S/N]: "
if errorlevel 2 goto fim
taskkill /FI "WINDOWTITLE eq COMINDIR03*" /T /F >nul 2>nul
echo Janelas fechadas (o daemon IPFS continua a correr).

:fim
echo.
pause
endlocal
