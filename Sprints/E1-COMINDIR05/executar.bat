@echo off
rem ============================================================
rem  E1-COMINDIR05 - Eleicao do lider de particao (estilo RAFT)
rem  Compila os 3 projetos, garante o daemon IPFS, arranca os
rem  9 nos (3 por particao), o processador e o produtor, e no fim
rem  corre o RaftCheck e o CausalCheck.
rem  (os ensaios 9 e 10 completos estao automatizados em ensaios.sh)
rem ============================================================
setlocal
chcp 65001 >nul
title Executar COMINDIR05
cd /d "%~dp0"

set "BASE=%~dp0"
set "RUN=%BASE%execucao\manual"
set "API=http://127.0.0.1:5001/api/v0"
set "J=java -Xmx96m -XX:+UseSerialGC -Dstdout.encoding=UTF-8"
set N=3

rem ---------- ferramentas (sem JDK no PATH: tenta o do Android Studio e o JAVA_HOME) ----------
where javac >nul 2>nul || if exist "D:\androidstudio\jbr\bin\javac.exe" set "PATH=D:\androidstudio\jbr\bin;%PATH%"
where javac >nul 2>nul || if defined JAVA_HOME if exist "%JAVA_HOME%\bin\javac.exe" set "PATH=%JAVA_HOME%\bin;%PATH%"
where javac >nul 2>nul || (echo ERRO: javac nao encontrado. Instala um JDK ^(17 ou superior^) ou define JAVA_HOME. & goto fim)
where ipfs  >nul 2>nul || (echo ERRO: ipfs ^(Kubo^) nao encontrado no PATH. & goto fim)
where curl  >nul 2>nul || (echo ERRO: curl nao encontrado. & goto fim)

echo.
echo  E1-COMINDIR05
echo  -------------
echo   1 - Interativo  (o produtor abre numa janela; escreves as leituras)
echo   2 - Rajada      (rajada 300 10 automatica)
echo.
choice /c 12 /n /m "Escolhe o modo [1/2]: "
set MODO=%errorlevel%

echo.
echo [1/4] A compilar...
for %%P in (produtor no processador) do (
    javac --release 11 -encoding UTF-8 -d "raft04-%%P\out" raft04-%%P\src\raft04\*.java || (echo ERRO a compilar raft04-%%P & goto fim)
    echo       raft04-%%P ok
)

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

rem os ficheiros de estado e de registos de uma execucao anterior mudam o comportamento: pasta nova
if exist "%RUN%" rmdir /s /q "%RUN%"
mkdir "%RUN%"
echo       ficheiros em %RUN%

echo [3/4] Nove nos (uma janela cada), processador
for %%K in (0 1 2) do for %%I in (1 2 3) do start "COMINDIR05 p%%K-n%%I" /D "%RUN%" cmd /k %J% -cp "%BASE%raft04-no\out" raft04.PartitionNode %%K %%I %N%
start "COMINDIR05 Processador" /D "%RUN%" cmd /k %J% -cp "%BASE%raft04-processador\out" raft04.AlarmProcessor %N%
echo       primeira eleicao: entre 6 e 12 s depois do arranque...
ping -n 15 127.0.0.1 >nul

set "PROD=%J% -cp "%BASE%raft04-produtor\out" raft04.Producer %N%"
echo [4/4] Produtor
if %MODO%==2 goto rajada
start "COMINDIR05 Produtor" /D "%RUN%" cmd /k %PROD%
echo.
echo  Na janela do Produtor escreve, por exemplo:
echo     S02 20.1          (S02, S05 e S08 vao para p1)
echo     rajada 300 10     (300 leituras de S01..S10, 10 ms entre elas)
echo  Para provocar uma falha: fecha a janela do lider de p1 e anota "p1;n^<i^>;^<instante ms^>" em execucao\manual\falhas.txt
echo  Para isolar o no i de p1 (particao de rede): cria o ficheiro execucao\manual\isolar-p1-n^<i^> e depois apaga-o
echo.
echo  Quando acabares, volta aqui e carrega numa tecla para as verificacoes.
pause >nul
goto verificar

:rajada
pushd "%RUN%"
(echo rajada 300 10& echo sair) | %PROD%
popd

:verificar
echo.
echo A aguardar 5 s pelos ultimos lotes...
ping -n 6 127.0.0.1 >nul
set "FALHAS="
if exist "%RUN%\falhas.txt" set "FALHAS=falhas.txt"
echo ================= RaftCheck =================
%J% -cp "%BASE%raft04-no\out" raft04.RaftCheck %N% "%RUN%" %FALHAS%
echo.
echo ================= CausalCheck ===============
%J% -cp "%BASE%raft04-no\out" raft04.CausalCheck %N% "%RUN%"
echo.
choice /c SN /n /m "Fechar as janelas dos nos, processador e produtor? [S/N]: "
if errorlevel 2 goto fim
taskkill /FI "WINDOWTITLE eq COMINDIR05*" /T /F >nul 2>nul
echo Janelas fechadas (o daemon IPFS continua a correr).

:fim
echo.
pause
endlocal
