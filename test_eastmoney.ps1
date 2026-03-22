# PowerShell测试东方财富API
$url = "https://82.push2.eastmoney.com/api/qt/clist/get"
$params = "?p=1&pz=10&po=1&np=1&fltt=2&invt=2&fid=f3&fs=m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23&fields=f12,f14,f2,f3"

$headers = @{
    'User-Agent' = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    'Accept' = 'application/json'
    'Referer' = 'https://quote.eastmoney.com/'
}

try {
    $resp = Invoke-WebRequest -Uri ($url + $params) -Headers $headers -UseBasicParsing -TimeoutSec 20
    Write-Host "Status: $($resp.StatusCode)"
    Write-Host "Content: $($resp.Content.Substring(0, [Math]::Min(200, $resp.Content.Length)))"
} catch {
    Write-Host "Error: $($_.Exception.Message)"
}