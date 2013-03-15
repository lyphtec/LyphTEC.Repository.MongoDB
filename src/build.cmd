@echo Off
set config=%1
if "%config%" == "" (
   set config=Debug
)

%WINDIR%\Microsoft.NET\Framework\v4.0.30319\msbuild Build\Build.proj /p:Configuration="%config%" /m /v:M /fl /flp:LogFile=msbuild.log;Verbosity=Normal /nr:false

rem del _artifacts\"%config%"\*.dll

if "%2" == "Push" (
    xcopy _artifacts\"%config%"\*.nupkg D:\Development\LocalNuGet\LocalNuGet\Packages\ /F /Y
)