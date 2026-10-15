@echo off
REM Right-click this file and choose "Run as administrator" (one time only).
netsh advfirewall firewall add rule name="Workflow Tracker (TCP 8080)" dir=in action=allow protocol=TCP localport=8080 profile=domain,private
echo.
echo Done. Other computers in the office can now open the workflow system on port 8080.
pause
