@echo off
rem ============================================================
rem  tpf-lider - compila (Maven) e arranca o LeaderBot
rem  Uso: executar.bat [guildId]      (opcional se o bot estiver num so servidor)
rem       executar.bat --teste        (valida os comandos sem Discord)
rem  O token vem de DISCORD_TOKEN ou de token.txt nesta pasta.
rem ============================================================
setlocal
chcp 65001 >nul
cd /d "%~dp0"

rem JDK: o do PATH; senao o do Android Studio ou o JAVA_HOME
where javac >nul 2>nul || if exist "D:\androidstudio\jbr\bin\javac.exe" set "JAVA_HOME=D:\androidstudio\jbr"
if defined JAVA_HOME set "PATH=%JAVA_HOME%\bin;%PATH%"
where java >nul 2>nul || (echo ERRO: Java 17+ nao encontrado. & goto fim)

rem Maven: o do PATH; senao o de %USERPROFILE%\tools
where mvn >nul 2>nul || if exist "%USERPROFILE%\tools\apache-maven-3.9.9\bin\mvn.cmd" set "PATH=%USERPROFILE%\tools\apache-maven-3.9.9\bin;%PATH%"
where mvn >nul 2>nul || (echo ERRO: Maven nao encontrado ^(ver README.md^). & goto fim)

if not exist target\tpf-lider.jar (
    echo A compilar...
    call mvn -q -B package || (echo ERRO a compilar. & goto fim)
)

java -Dstdout.encoding=UTF-8 -jar target\tpf-lider.jar %*

:fim
endlocal
