# Equivalente a kill -STOP / kill -CONT no Windows: suspende ou retoma todas as threads de um processo java
# Uso: powershell -File suspender.ps1 suspend|resume <padrão da linha de comando>
#      (ex.: suspender.ps1 suspend "rep04.PartitionLeader 2 ")
param([string]$acao, [string]$padrao)
Add-Type -Namespace Win -Name Nt -MemberDefinition @'
[DllImport("ntdll.dll")] public static extern int NtSuspendProcess(System.IntPtr h);
[DllImport("ntdll.dll")] public static extern int NtResumeProcess(System.IntPtr h);
'@
$alvos = Get-CimInstance Win32_Process -Filter "Name='java.exe'" | Where-Object { $_.CommandLine -like "*$padrao*" }
if (-not $alvos) { Write-Output "nenhum processo java com '$padrao'"; exit 1 }
foreach ($a in $alvos) {
    $p = Get-Process -Id $a.ProcessId
    if ($acao -eq 'suspend') { $r = [Win.Nt]::NtSuspendProcess($p.Handle) } else { $r = [Win.Nt]::NtResumeProcess($p.Handle) }
    Write-Output "$acao pid $($a.ProcessId): $r"
}
